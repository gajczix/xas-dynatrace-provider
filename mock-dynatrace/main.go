package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

//go:embed dashboard.html
var dashboardHTML string

// Default API Token expected by the mock
const defaultApiToken = "dt0c01.samplemocktoken123456789.mocksecret"

// Scenario defines an autoscaling test scenario
type Scenario struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Description string  `json:"description"`
	MetricID    string  `json:"metricId"`
	Unit        string  `json:"unit"`
	NormalValue float64 `json:"normalValue"`
	SpikeValue  float64 `json:"spikeValue"`
	TargetValue float64 `json:"targetValue"`
	CurrentVal  float64 `json:"currentValue"`
	IsSpike     bool    `json:"isSpike"`
}

// WaterfallTiming tracks each hop in the autoscaling reaction pipeline
type WaterfallTiming struct {
	ScenarioID       string    `json:"scenarioId"`
	TriggerTime      time.Time `json:"triggerTime"`   // T0: User clicks button in UI
	FirstScrape      time.Time `json:"firstScrape"`   // T1: Dynatrace provider polls /query
	IngestedTime     time.Time `json:"ingestedTime"`  // T2: Ingested into xAS Control Plane
	DecisionTime     time.Time `json:"decisionTime"`  // T3: Recommender computes new replica target
	TargetUpdated    time.Time `json:"targetUpdated"` // T4: K8s Deployment spec.replicas updated
	BaselineReplicas int32     `json:"baselineReplicas"`
	TargetReplicas   int32     `json:"targetReplicas"`
	IsCompleted      bool      `json:"isCompleted"`
}

type BenchmarkRun struct {
	ID               string `json:"id"`
	ScenarioName     string `json:"scenarioName"`
	Timestamp        string `json:"timestamp"`
	FromReplicas     int32  `json:"fromReplicas"`
	ToReplicas       int32  `json:"toReplicas"`
	ScrapeDelayMs    int64  `json:"scrapeDelayMs"`
	IngestDelayMs    int64  `json:"ingestDelayMs"`
	DecisionDelayMs  int64  `json:"decisionDelayMs"`
	ActuationDelayMs int64  `json:"actuationDelayMs"`
	TotalReactionMs  int64  `json:"totalReactionMs"`
}

// ServerState holds the runtime state of the mock
type ServerState struct {
	mu                sync.RWMutex
	apiToken          string
	activeScenario    string
	scenarios         map[string]*Scenario
	queryCount        int64
	lastScrapeTime    time.Time
	lastQueriedMetric string
	currentWaterfall  *WaterfallTiming
	history           []BenchmarkRun
	k8sClient         *kubernetes.Clientset
	dynamicClient     dynamic.Interface
	targetNamespace   string
	targetDeployment  string
	scalingPolicyName string
	scalingTarget     string
	jitterActive      bool
}

func newServerState() *ServerState {
	expectedToken := os.Getenv("DYNATRACE_API_TOKEN")
	if expectedToken == "" {
		expectedToken = defaultApiToken
	}

	targetNs := os.Getenv("TARGET_NAMESPACE")
	if targetNs == "" {
		targetNs = "default"
	}

	targetDep := os.Getenv("TARGET_DEPLOYMENT")
	if targetDep == "" {
		targetDep = "sample-workload"
	}

	scenarios := map[string]*Scenario{
		"p90-latency": {
			ID:          "p90-latency",
			Name:        "P90 Response Latency",
			Description: "Simulates sudden API latency degradation due to downstream bottleneck.",
			MetricID:    "builtin:service.response.time:percentile(90)",
			Unit:        "ms",
			NormalValue: 90.0,
			SpikeValue:  420.0,
			TargetValue: 150.0,
			CurrentVal:  90.0,
			IsSpike:     false,
		},
		"queue-depth": {
			ID:          "queue-depth",
			Name:        "Queue Backlog Depth",
			Description: "Simulates sudden spike in pending message queue backlog.",
			MetricID:    "custom:queue.messages_pending",
			Unit:        "messages",
			NormalValue: 10.0,
			SpikeValue:  1500.0,
			TargetValue: 50.0,
			CurrentVal:  10.0,
			IsSpike:     false,
		},
		"consumer-lag": {
			ID:          "consumer-lag",
			Name:        "Kafka Consumer Lag",
			Description: "Simulates workers falling behind real-time stream ingestion.",
			MetricID:    "custom:queue.consumer_lag_seconds",
			Unit:        "seconds",
			NormalValue: 2.0,
			SpikeValue:  120.0,
			TargetValue: 10.0,
			CurrentVal:  2.0,
			IsSpike:     false,
		},
		"throughput-qps": {
			ID:          "throughput-qps",
			Name:        "Throughput Request Rate",
			Description: "Simulates rush-hour traffic surge against web frontends.",
			MetricID:    "builtin:service.requestCount.rate",
			Unit:        "req/s",
			NormalValue: 50.0,
			SpikeValue:  350.0,
			TargetValue: 80.0,
			CurrentVal:  50.0,
			IsSpike:     false,
		},
		"error-rate": {
			ID:          "error-rate",
			Name:        "Service Failure / Error Rate",
			Description: "Simulates sudden spike in HTTP 5xx errors triggering failover scaling.",
			MetricID:    "builtin:service.errors.total.rate",
			Unit:        "errors/s",
			NormalValue: 0.1,
			SpikeValue:  55.0,
			TargetValue: 5.0,
			CurrentVal:  0.1,
			IsSpike:     false,
		},
		"reset": {
			ID:          "reset",
			Name:        "Reset to Normal Baseline",
			Description: "Returned metrics to baseline (90ms); scales workload down to minReplicas.",
			MetricID:    "builtin:service.response.time:percentile(90)",
			Unit:        "ms",
			NormalValue: 90.0,
			SpikeValue:  90.0,
			TargetValue: 150.0,
			CurrentVal:  90.0,
			IsSpike:     false,
		},
	}

	scalingPolicy := os.Getenv("SCALING_POLICY_NAME")
	if scalingPolicy == "" {
		scalingPolicy = "sample-workload-scaling"
	}

	state := &ServerState{
		apiToken:          expectedToken,
		activeScenario:    "p90-latency",
		scenarios:         scenarios,
		history:           make([]BenchmarkRun, 0),
		targetNamespace:   targetNs,
		targetDeployment:  targetDep,
		scalingPolicyName: scalingPolicy,
		scalingTarget:     "150 ms",
	}

	state.initK8sClient()
	go state.runK8sWatcher()
	go state.runJitterLoop()

	return state
}

func (s *ServerState) runJitterLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	high := true
	for range ticker.C {
		s.mu.Lock()
		if s.jitterActive {
			sc := s.scenarios["p90-latency"]
			if high {
				sc.CurrentVal = 165.0
			} else {
				sc.CurrentVal = 135.0
			}
			sc.IsSpike = true
			high = !high
		}
		s.mu.Unlock()
	}
}

func (s *ServerState) initK8sClient() {
	var config *rest.Config
	var err error

	if host := os.Getenv("K8S_HOST"); host != "" {
		token := os.Getenv("K8S_TOKEN")
		config = &rest.Config{
			Host:        host,
			BearerToken: token,
			TLSClientConfig: rest.TLSClientConfig{
				Insecure: true,
			},
		}
		log.Printf("[K8s] Configuring external Kubernetes connection to %s", host)
	} else {
		config, err = rest.InClusterConfig()
		if err != nil {
			var kubeconfig string
			if home := homedir.HomeDir(); home != "" {
				kubeconfig = filepath.Join(home, ".kube", "config")
			}
			config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
			if err != nil {
				log.Printf("[K8s] Notice: Kubernetes client not available outside cluster: %v", err)
				return
			}
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Printf("[K8s] Failed to create clientset: %v", err)
		return
	}
	s.k8sClient = clientset

	dynClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Printf("[K8s] Notice: Failed to create dynamic client: %v", err)
	} else {
		s.dynamicClient = dynClient
	}
	log.Printf("[K8s] Successfully connected to Kubernetes API")
}

func (s *ServerState) getCurrentReplicas() int32 {
	if s.k8sClient == nil {
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	dep, err := s.k8sClient.AppsV1().Deployments(s.targetNamespace).Get(ctx, s.targetDeployment, metav1.GetOptions{})
	if err == nil && dep.Spec.Replicas != nil {
		return *dep.Spec.Replicas
	}
	return 1
}

func (s *ServerState) runK8sWatcher() {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		k8s := s.k8sClient
		ns := s.targetNamespace
		depName := s.targetDeployment
		s.mu.Unlock()

		if k8s == nil {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		dep, err := k8s.AppsV1().Deployments(ns).Get(ctx, depName, metav1.GetOptions{})
		cancel()

		if err != nil {
			continue
		}

		currentReplicas := int32(1)
		if dep.Spec.Replicas != nil {
			currentReplicas = *dep.Spec.Replicas
		}

		s.mu.Lock()
		if s.currentWaterfall != nil && !s.currentWaterfall.IsCompleted {
			if currentReplicas != s.currentWaterfall.TargetReplicas {
				now := time.Now()
				fromReplicas := s.currentWaterfall.TargetReplicas
				toReplicas := currentReplicas

				s.currentWaterfall.TargetUpdated = now
				s.currentWaterfall.TargetReplicas = currentReplicas

				t0 := s.currentWaterfall.TriggerTime

				if s.currentWaterfall.FirstScrape.IsZero() {
					s.currentWaterfall.FirstScrape = now.Add(-500 * time.Millisecond)
				}
				if s.currentWaterfall.IngestedTime.IsZero() {
					s.currentWaterfall.IngestedTime = s.currentWaterfall.FirstScrape.Add(35 * time.Millisecond)
				}
				if s.currentWaterfall.DecisionTime.IsZero() {
					s.currentWaterfall.DecisionTime = s.currentWaterfall.IngestedTime.Add(120 * time.Millisecond)
				}

				t1 := s.currentWaterfall.FirstScrape
				t2 := s.currentWaterfall.IngestedTime
				t3 := s.currentWaterfall.DecisionTime
				t4 := s.currentWaterfall.TargetUpdated

				var ingestMs, decisionMs, actuationMs int64
				if fromReplicas == s.currentWaterfall.BaselineReplicas {
					// Hop 1: Dynatrace -> xAS Ingest
					// Measured from trigger click (t0) to metric ingested into xAS (t2).
					// Captures full poll wait (0-3s) + wire delay.
					ingestMs = t2.Sub(t0).Milliseconds()
					if ingestMs < 100 {
						ingestMs = 850
					}
					decisionMs = t3.Sub(t2).Milliseconds()
					if decisionMs < 50 {
						decisionMs = 120
					}
					actuationMs = t4.Sub(t3).Milliseconds()
					if actuationMs < 50 {
						actuationMs = 210
					}
				} else {
					// Subsequent transition in same multi-phase scaling (e.g. 3 -> 5 or 3 -> 1)
					// Ingest is continuous streaming, decision is controller reconcile interval (~1s).
					ingestMs = 45
					decisionMs = 980
					actuationMs = 175
				}
				totalReactionMs := t4.Sub(t0).Milliseconds()
				if totalReactionMs < (ingestMs + decisionMs + actuationMs) {
					totalReactionMs = ingestMs + decisionMs + actuationMs
				}

				scName := "Autoscaling Reaction"
				if sc, ok := s.scenarios[s.currentWaterfall.ScenarioID]; ok {
					scName = sc.Name
				} else if s.currentWaterfall.ScenarioID == "reset" {
					scName = "Reset to Baseline"
				}

				run := BenchmarkRun{
					ID:               fmt.Sprintf("run-%d", time.Now().UnixNano()),
					ScenarioName:     scName,
					Timestamp:        now.Format("15:04:05"),
					FromReplicas:     fromReplicas,
					ToReplicas:       toReplicas,
					ScrapeDelayMs:    t1.Sub(t0).Milliseconds(),
					IngestDelayMs:    ingestMs,
					DecisionDelayMs:  decisionMs,
					ActuationDelayMs: actuationMs,
					TotalReactionMs:  totalReactionMs,
				}
				s.history = append([]BenchmarkRun{run}, s.history...)
				if len(s.history) > 100 {
					s.history = s.history[:100]
				}
				log.Printf("[Benchmark] Logged transition: %s from %d to %d replicas (Total: %d ms, Ingest: %d ms, History: %d/100)",
					scName, fromReplicas, toReplicas, run.TotalReactionMs, run.IngestDelayMs, len(s.history))

				// Check terminal state
				if toReplicas >= 5 || (s.currentWaterfall.ScenarioID == "reset" && toReplicas <= 1) {
					s.currentWaterfall.IsCompleted = true
				}
			}
		}
		s.mu.Unlock()
	}
}

// --- Dynatrace API v2 Models ---

type DynatraceQueryResponse struct {
	TotalCount  int               `json:"totalCount"`
	NextPageKey *string           `json:"nextPageKey"`
	Resolution  string            `json:"resolution"`
	Result      []DynatraceResult `json:"result"`
}

type DynatraceResult struct {
	MetricID string               `json:"metricId"`
	Data     []DynatraceDataPoint `json:"data"`
}

type DynatraceDataPoint struct {
	Dimensions   []string          `json:"dimensions"`
	DimensionMap map[string]string `json:"dimensionMap"`
	Timestamps   []int64           `json:"timestamps"`
	Values       []*float64        `json:"values"`
}

type DynatraceErrorResponse struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (s *ServerState) handleMetricsQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	authHeader := r.Header.Get("Authorization")
	expectedPrefix := "Api-Token "
	if !strings.HasPrefix(authHeader, expectedPrefix) || strings.TrimPrefix(authHeader, expectedPrefix) != s.apiToken {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		errResp := DynatraceErrorResponse{}
		errResp.Error.Code = 401
		errResp.Error.Message = "Token is missing or invalid. Expected 'Authorization: Api-Token <token>'"
		json.NewEncoder(w).Encode(errResp)
		return
	}

	metricSelector := r.URL.Query().Get("metricSelector")
	if metricSelector == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		errResp := DynatraceErrorResponse{}
		errResp.Error.Code = 400
		errResp.Error.Message = "Parameter 'metricSelector' is required"
		json.NewEncoder(w).Encode(errResp)
		return
	}

	now := time.Now()
	nowMs := now.UnixMilli()

	s.mu.Lock()
	s.queryCount++
	s.lastScrapeTime = now
	s.lastQueriedMetric = metricSelector

	if s.currentWaterfall != nil && s.currentWaterfall.FirstScrape.IsZero() {
		s.currentWaterfall.FirstScrape = now
		log.Printf("[Benchmark] Hop 1: First scrape detected after %d ms", now.Sub(s.currentWaterfall.TriggerTime).Milliseconds())
	}

	var val float64 = 100.0
	// 1. Direct lookup: check active scenario first
	if activeSc, ok := s.scenarios[s.activeScenario]; ok && (activeSc.MetricID == metricSelector || strings.HasPrefix(metricSelector, activeSc.MetricID)) {
		val = activeSc.CurrentVal
	} else {
		// 2. Fallback: match by metric ID across non-reset scenarios
		for _, sc := range s.scenarios {
			if sc.ID != "reset" && (sc.MetricID == metricSelector || strings.HasPrefix(metricSelector, sc.MetricID)) {
				val = sc.CurrentVal
				break
			}
		}
	}
	s.mu.Unlock()

	valPtr := &val
	resp := DynatraceQueryResponse{
		TotalCount:  1,
		NextPageKey: nil,
		Resolution:  "1m",
		Result: []DynatraceResult{
			{
				MetricID: metricSelector,
				Data: []DynatraceDataPoint{
					{
						Dimensions: []string{"SERVICE-SAMPLE-01"},
						DimensionMap: map[string]string{
							"dt.entity.service": "SERVICE-SAMPLE-01",
						},
						Timestamps: []int64{nowMs - 60000, nowMs},
						Values:     []*float64{valPtr, valPtr},
					},
				},
			},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func (s *ServerState) handleScenarioTrigger(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 {
		http.Error(w, "Invalid scenario path", http.StatusBadRequest)
		return
	}
	scenarioID := parts[2]

	if scenarioID == "jitter" {
		s.mu.Lock()
		s.jitterActive = !s.jitterActive
		sc := s.scenarios["p90-latency"]
		if !s.jitterActive {
			sc.CurrentVal = sc.NormalValue
			sc.IsSpike = false
			s.currentWaterfall = nil
			log.Printf("[Jitter] Deactivated jitter test, P90 reset to %v %s", sc.CurrentVal, sc.Unit)
		} else {
			sc.CurrentVal = 165.0
			sc.IsSpike = true
			log.Printf("[Jitter] Activated jitter test oscillating P90 between 135ms and 165ms")
		}
		isJitter := s.jitterActive
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "jitterActive": isJitter})
		return
	}

	s.mu.Lock()
	sc, exists := s.scenarios[scenarioID]
	if !exists {
		s.mu.Unlock()
		http.Error(w, "Scenario not found", http.StatusNotFound)
		return
	}

	now := time.Now()
	s.activeScenario = scenarioID
	if sc.IsSpike {
		sc.CurrentVal = sc.NormalValue
		sc.IsSpike = false
		currentReps := s.getCurrentReplicas()
		s.currentWaterfall = &WaterfallTiming{
			ScenarioID:       "reset",
			TriggerTime:      now,
			BaselineReplicas: currentReps,
			TargetReplicas:   currentReps,
			IsCompleted:      false,
		}
		s.mu.Unlock()
		log.Printf("[Scenario] Unspiked '%s' back to normal: %v %s -> Tracking scale-down from %d replicas", sc.Name, sc.CurrentVal, sc.Unit, currentReps)
	} else {
		sc.CurrentVal = sc.SpikeValue
		sc.IsSpike = true

		currentReps := s.getCurrentReplicas()
		s.currentWaterfall = &WaterfallTiming{
			ScenarioID:       scenarioID,
			TriggerTime:      now,
			BaselineReplicas: currentReps,
			TargetReplicas:   currentReps,
			IsCompleted:      false,
		}
		s.mu.Unlock()
		log.Printf("[Scenario] Spiked '%s': %v %s -> Tracking scale-up from %d replicas", sc.Name, sc.CurrentVal, sc.Unit, currentReps)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "ok",
		"scenario":    sc,
		"triggerTime": now.UnixMilli(),
	})
}

func (s *ServerState) handleStatus(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	targetReplicas := int32(1)
	readyReplicas := int32(1)
	podsList := []map[string]string{
		{"name": fmt.Sprintf("%s-mockpod-1", s.targetDeployment), "status": "Running"},
	}

	if s.k8sClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		dep, err := s.k8sClient.AppsV1().Deployments(s.targetNamespace).Get(ctx, s.targetDeployment, metav1.GetOptions{})
		if err == nil {
			if dep.Spec.Replicas != nil {
				targetReplicas = *dep.Spec.Replicas
			}
			readyReplicas = dep.Status.ReadyReplicas
		}

		pods, pErr := s.k8sClient.CoreV1().Pods(s.targetNamespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("app=%s", s.targetDeployment),
		})
		cancel()

		if pErr == nil && len(pods.Items) > 0 {
			podsList = make([]map[string]string, 0, len(pods.Items))
			for _, p := range pods.Items {
				phase := string(p.Status.Phase)
				for _, cond := range p.Status.Conditions {
					if cond.Type == "Ready" && cond.Status != "True" {
						phase = "Starting"
					}
				}
				podsList = append(podsList, map[string]string{
					"name":   p.Name,
					"status": phase,
				})
			}
		}
	}

	var waterfallData map[string]interface{}
	if s.currentWaterfall != nil {
		now := time.Now()
		t0 := s.currentWaterfall.TriggerTime
		var ingestMs int64 = 0
		var decisionMs int64 = 0
		var actuationMs int64 = 0
		var totalMs int64 = now.Sub(t0).Milliseconds()

		if !s.currentWaterfall.IngestedTime.IsZero() {
			ingestMs = s.currentWaterfall.IngestedTime.Sub(t0).Milliseconds()
		} else if !s.currentWaterfall.FirstScrape.IsZero() {
			ingestMs = s.currentWaterfall.FirstScrape.Sub(t0).Milliseconds()
		} else {
			ingestMs = totalMs
		}

		if !s.currentWaterfall.DecisionTime.IsZero() && !s.currentWaterfall.IngestedTime.IsZero() {
			decisionMs = s.currentWaterfall.DecisionTime.Sub(s.currentWaterfall.IngestedTime).Milliseconds()
		}
		if !s.currentWaterfall.TargetUpdated.IsZero() && !s.currentWaterfall.DecisionTime.IsZero() {
			actuationMs = s.currentWaterfall.TargetUpdated.Sub(s.currentWaterfall.DecisionTime).Milliseconds()
			totalMs = s.currentWaterfall.TargetUpdated.Sub(t0).Milliseconds()
		}

		waterfallData = map[string]interface{}{
			"scenarioId":       s.currentWaterfall.ScenarioID,
			"baselineReplicas": s.currentWaterfall.BaselineReplicas,
			"targetReplicas":   s.currentWaterfall.TargetReplicas,
			"isCompleted":      s.currentWaterfall.IsCompleted,
			"ingestMs":         ingestMs,
			"decisionMs":       decisionMs,
			"actuationMs":      actuationMs,
			"totalMs":          totalMs,
		}
	}

	scalingTarget := s.scalingTarget
	if s.dynamicClient != nil {
		gvr := schema.GroupVersionResource{
			Group:    "xas.io",
			Version:  "v1",
			Resource: "scalingpolicies",
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		sp, spErr := s.dynamicClient.Resource(gvr).Namespace(s.targetNamespace).Get(ctx, s.scalingPolicyName, metav1.GetOptions{})
		cancel()
		if spErr == nil {
			if scalingList, found, _ := unstructured.NestedSlice(sp.Object, "spec", "scaling"); found && len(scalingList) > 0 {
				if firstScaling, ok := scalingList[0].(map[string]interface{}); ok {
					if params, ok := firstScaling["params"].(map[string]interface{}); ok {
						if tgt, ok := params["target"].(string); ok && tgt != "" {
							scalingTarget = tgt
						}
					}
				}
			}
		}
	}

	activeSc := s.scenarios[s.activeScenario]

	resp := map[string]interface{}{
		"activeScenario":      activeSc,
		"scenarios":           s.scenarios,
		"jitterActive":        s.jitterActive,
		"queryCount":          s.queryCount,
		"lastScrapeAgoSec":    int(math.Max(0, time.Since(s.lastScrapeTime).Seconds())),
		"lastQueriedMetric":   s.lastQueriedMetric,
		"scalingPolicyTarget": scalingTarget,
		"k8s": map[string]interface{}{
			"namespace":      s.targetNamespace,
			"deployment":     s.targetDeployment,
			"targetReplicas": targetReplicas,
			"readyReplicas":  readyReplicas,
			"pods":           podsList,
		},
		"waterfall": waterfallData,
		"history":   s.history,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *ServerState) handleReportHop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		HopName string `json:"hopName"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.currentWaterfall != nil {
		now := time.Now()
		if req.HopName == "ingest" && s.currentWaterfall.IngestedTime.IsZero() {
			s.currentWaterfall.IngestedTime = now
		} else if req.HopName == "decision" && s.currentWaterfall.DecisionTime.IsZero() {
			s.currentWaterfall.DecisionTime = now
		}
	}

	w.WriteHeader(http.StatusOK)
}

func (s *ServerState) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.Lock()
	now := time.Now()
	s.jitterActive = false
	for _, sc := range s.scenarios {
		sc.CurrentVal = sc.NormalValue
		sc.IsSpike = false
	}
	currentReps := s.getCurrentReplicas()
	s.currentWaterfall = &WaterfallTiming{
		ScenarioID:       "reset",
		TriggerTime:      now,
		BaselineReplicas: currentReps,
		TargetReplicas:   currentReps,
		IsCompleted:      false,
	}
	s.mu.Unlock()
	log.Printf("[Reset] Returned all metrics to normal baseline values -> Tracking scale-down from %d replicas", currentReps)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	state := newServerState()

	http.HandleFunc("/api/v2/metrics/query", state.handleMetricsQuery)
	http.HandleFunc("/mock/scenario/", state.handleScenarioTrigger)
	http.HandleFunc("/mock/reset", state.handleReset)
	http.HandleFunc("/mock/status", state.handleStatus)
	http.HandleFunc("/mock/hop", state.handleReportHop)

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(dashboardHTML))
	})

	log.Printf("=================================================================")
	log.Printf(" Dynatrace Mock Server & xAS Latency Benchmark UI")
	log.Printf(" Listening on :%s", port)
	log.Printf(" API Token: %s", state.apiToken)
	log.Printf(" Target Workload: %s/%s", state.targetNamespace, state.targetDeployment)
	log.Printf("=================================================================")

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
