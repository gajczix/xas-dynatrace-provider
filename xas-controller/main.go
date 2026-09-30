package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

const (
	defaultNamespace  = "default"
	defaultPolicyName = "sample-workload-scaling"
)

var scalingPolicyGVR = schema.GroupVersionResource{
	Group:    "xas.io",
	Version:  "v1",
	Resource: "scalingpolicies",
}

type XASController struct {
	k8sClient     *kubernetes.Clientset
	dynamicClient dynamic.Interface
	namespace     string
	policyName    string
}

func newXASController() (*XASController, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			if home := homedir.HomeDir(); home != "" {
				kubeconfig = home + "/.kube/config"
			}
		}
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("failed to build kubeconfig: %w", err)
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes clientset: %w", err)
	}

	dynClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	return &XASController{
		k8sClient:     clientset,
		dynamicClient: dynClient,
		namespace:     defaultNamespace,
		policyName:    defaultPolicyName,
	}, nil
}

func (c *XASController) Start(ctx context.Context) {
	log.Printf("==========================================================")
	log.Printf(" xAS Controller (Extensible Workload Autoscaler)")
	log.Printf(" Namespace: %s, Target Policy: %s", c.namespace, c.policyName)
	log.Printf("==========================================================")

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[xAS Controller] Shutting down controller loop...")
			return
		case <-ticker.C:
			c.reconcileOnce(ctx)
		}
	}
}

func (c *XASController) reconcileOnce(ctx context.Context) {
	// 1. Fetch ScalingPolicy CR
	sp, err := c.dynamicClient.Resource(scalingPolicyGVR).Namespace(c.namespace).Get(ctx, c.policyName, metav1.GetOptions{})
	if err != nil {
		return
	}

	// 2. Parse Target Deployment, Bounds, and Target Latency
	targetDeployment, _, _ := unstructured.NestedString(sp.Object, "spec", "scaleTargetRef", "name")
	if targetDeployment == "" {
		targetDeployment = "sample-workload"
	}

	// Read minReplicas and maxReplicas directly from policy (check spec.horizontal or spec)
	minReplicas, foundMin, _ := unstructured.NestedInt64(sp.Object, "spec", "horizontal", "minReplicas")
	if !foundMin {
		minReplicas, foundMin, _ = unstructured.NestedInt64(sp.Object, "spec", "minReplicas")
		if !foundMin {
			minReplicas = 1 // Standard Kubernetes default when omitted
		}
	}

	maxReplicas, foundMax, _ := unstructured.NestedInt64(sp.Object, "spec", "horizontal", "maxReplicas")
	if !foundMax {
		maxReplicas, foundMax, _ = unstructured.NestedInt64(sp.Object, "spec", "maxReplicas")
		if !foundMax || maxReplicas < minReplicas {
			log.Printf("[xAS Controller] Error: maxReplicas not specified or invalid in ScalingPolicy")
			return
		}
	}

	var targetMetricVal float64
	scalingRules, foundRules, _ := unstructured.NestedSlice(sp.Object, "spec", "scaling")
	if foundRules && len(scalingRules) > 0 {
		if ruleMap, ok := scalingRules[0].(map[string]interface{}); ok {
			if params, ok := ruleMap["params"].(map[string]interface{}); ok {
				if tStr, ok := params["target"].(string); ok {
					cleanStr := strings.TrimSuffix(strings.TrimSpace(tStr), "ms")
					if val, pErr := strconv.ParseFloat(cleanStr, 64); pErr == nil && val > 0 {
						targetMetricVal = val
					}
				}
			}
		}
	}
	if targetMetricVal <= 0 {
		log.Printf("[xAS Controller] Error: target metric not specified or invalid in ScalingPolicy")
		return
	}

	// 3. Read current metric scraped by dynatrace-metric-provider in cluster
	currentMetricVal := c.getLatestScrapedMetric(ctx)
	if currentMetricVal <= 0 {
		return
	}

	// 4. Fetch target Deployment current replicas
	dep, err := c.k8sClient.AppsV1().Deployments(c.namespace).Get(ctx, targetDeployment, metav1.GetOptions{})
	if err != nil {
		return
	}

	var currentReplicas int32 = 1
	if dep.Spec.Replicas != nil {
		currentReplicas = *dep.Spec.Replicas
	}

	// 5. Evaluate Linear Recommender math:
	// Scale-up: ceil (conservative, ensures capacity)
	// Scale-down: floor (ensures returning cleanly to minReplicas)
	ratio := currentMetricVal / targetMetricVal
	var desiredReplicas int32
	if ratio >= 1.0 {
		desiredReplicas = int32(math.Ceil(float64(currentReplicas) * ratio))
	} else {
		desiredReplicas = int32(math.Floor(float64(currentReplicas) * ratio))
	}

	if desiredReplicas < int32(minReplicas) {
		desiredReplicas = int32(minReplicas)
	}
	if desiredReplicas > int32(maxReplicas) {
		desiredReplicas = int32(maxReplicas)
	}

	// 6. Actuate: Update Deployment replicas if recommendation changed
	if desiredReplicas != currentReplicas {
		log.Printf("[xAS Controller] Scaling action: currentMetric=%.1f ms, target=%.1f ms -> Scaling '%s' from %d to %d replicas",
			currentMetricVal, targetMetricVal, targetDeployment, currentReplicas, desiredReplicas)

		dep.Spec.Replicas = &desiredReplicas
		_, updateErr := c.k8sClient.AppsV1().Deployments(c.namespace).Update(ctx, dep, metav1.UpdateOptions{})
		if updateErr != nil {
			log.Printf("[xAS Controller] Error scaling deployment: %v", updateErr)
		}
	}
}

var metricLogRegex = regexp.MustCompile(`->\s*([0-9.]+)\s*ms`)

func (c *XASController) getLatestScrapedMetric(ctx context.Context) float64 {
	pods, err := c.k8sClient.CoreV1().Pods(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=dynatrace-metric-provider",
	})
	if err != nil || len(pods.Items) == 0 {
		return 0
	}

	podName := pods.Items[0].Name
	tailLines := int64(10)
	req := c.k8sClient.CoreV1().Pods(c.namespace).GetLogs(podName, &corev1.PodLogOptions{
		TailLines: &tailLines,
	})

	logBytes, err := req.DoRaw(ctx)
	if err != nil {
		return 0
	}

	lines := strings.Split(string(logBytes), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		matches := metricLogRegex.FindStringSubmatch(lines[i])
		if len(matches) > 1 {
			if val, pErr := strconv.ParseFloat(matches[1], 64); pErr == nil {
				return val
			}
		}
	}
	return 0
}

func main() {
	ctrl, err := newXASController()
	if err != nil {
		log.Fatalf("Initialization error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	ctrl.Start(ctx)
}
