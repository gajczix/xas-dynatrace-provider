package dynatrace

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	pb "github.com/gke-labs/extensible-workload-autoscaler/api/proto/v1alpha"
	xasv1 "github.com/gke-labs/extensible-workload-autoscaler/pkg/apis/xas/v1"
	listers "github.com/gke-labs/extensible-workload-autoscaler/pkg/client/listers/xas/v1"
)

// Provider implements the xAS Metric Provider for Dynatrace.
// It watches MetricProviderClasses and active ScalingPolicies, queries Dynatrace,
// and pushes samples to the central xAS server via gRPC IngestMetrics.
type Provider struct {
	kubeClient     kubernetes.Interface
	providerLister listers.MetricProviderClassLister

	grpcConn   *grpc.ClientConn
	grpcClient pb.XASServerClient

	clusterName    string
	scrapeInterval time.Duration

	mu          sync.Mutex
	clientCache map[string]*Client
}

// NewProvider creates a new Dynatrace metric provider instance.
func NewProvider(
	kubeClient kubernetes.Interface,
	providerLister listers.MetricProviderClassLister,
	serverAddress, clusterName string,
	scrapeInterval time.Duration,
) *Provider {
	conn, err := grpc.NewClient(serverAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Error("Cannot connect to xAS Server", "address", serverAddress, "error", err)
		os.Exit(1)
	}
	client := pb.NewXASServerClient(conn)

	if scrapeInterval <= 0 {
		scrapeInterval = 5 * time.Second
	}

	return &Provider{
		kubeClient:     kubeClient,
		providerLister: providerLister,
		grpcConn:       conn,
		grpcClient:     client,
		clusterName:    clusterName,
		scrapeInterval: scrapeInterval,
		clientCache:    make(map[string]*Client),
	}
}

// Run starts the periodic metric polling and ingestion loop.
func (p *Provider) Run(ctx context.Context) {
	defer p.grpcConn.Close()
	ticker := time.NewTicker(p.scrapeInterval)
	defer ticker.Stop()

	slog.Info("xAS Dynatrace Metric Provider loop started", "interval", p.scrapeInterval, "cluster", p.clusterName)

	for {
		select {
		case <-ticker.C:
			p.scrapeAndSend(ctx)
		case <-ctx.Done():
			slog.Info("Shutting down Dynatrace Metric Provider loop...")
			return
		}
	}
}

func (p *Provider) scrapeAndSend(ctx context.Context) {
	slog.Debug("Starting Dynatrace scrape and ingestion cycle...")

	scrapeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// 1. Ask xAS Server for active ScalingPolicies in this cluster
	resp, err := p.grpcClient.ListPolicies(scrapeCtx, &pb.ListPoliciesRequest{
		ClusterName: p.clusterName,
	})
	if err != nil {
		slog.Error("Error listing policies from xAS Server", "error", err)
		return
	}

	var policyBatches []*pb.PolicyBatch

	// 2. Iterate through each policy and evaluate Dynatrace metrics
	for _, pol := range resp.Policies {
		var samples []*pb.MetricSample

		for _, m := range metricDefinitions(pol) {
			class, err := p.providerLister.Get(m.Provider)
			if err != nil {
				continue
			}

			// Only handle Dynatrace provider classes
			if class.Spec.Type != "Dynatrace" {
				continue
			}

			sample, err := p.collectSample(scrapeCtx, pol.Id.Namespace, m, class)
			if err != nil {
				slog.Warn("Failed to collect Dynatrace metric", "policy", pol.Id.Name, "metric", m.Name, "error", err)
				continue
			}
			if sample != nil {
				samples = append(samples, sample)
			}
		}

		if len(samples) > 0 {
			policyBatches = append(policyBatches, &pb.PolicyBatch{
				Namespace: pol.Id.Namespace,
				Name:      pol.Id.Name,
				Batches: []*pb.MetricBatch{
					{
						PodName: "", // Global policy-level metric
						Samples: samples,
					},
				},
			})
		}
	}

	// 3. Ingest collected metrics to xAS Server via gRPC
	if len(policyBatches) > 0 {
		slog.Info("Ingesting Dynatrace metrics to xAS Server", "policyCount", len(policyBatches))
		ingestResp, err := p.grpcClient.IngestMetrics(scrapeCtx, &pb.IngestMetricsRequest{
			ClusterName: p.clusterName,
			Timestamp:   time.Now().Unix(),
			Policies:    policyBatches,
		})
		if err != nil {
			slog.Error("Failed to ingest metrics to xAS Server", "error", err)
			return
		}
		if !ingestResp.Success {
			slog.Warn("xAS Server returned ingestion failure")
		}
	}
}

func (p *Provider) collectSample(ctx context.Context, namespace string, m *pb.MetricDefinition, class *xasv1.MetricProviderClass) (*pb.MetricSample, error) {
	endpoint := class.Spec.Config["endpoint"]
	if endpoint == "" {
		return nil, fmt.Errorf("MetricProviderClass '%s' missing 'endpoint' in config", class.Name)
	}

	// Resolve API token
	apiToken := os.Getenv("DYNATRACE_API_TOKEN")
	if apiToken == "" {
		secretRef := class.Spec.Config["apiTokenSecretRef"]
		if secretRef != "" && p.kubeClient != nil {
			sec, err := p.kubeClient.CoreV1().Secrets(namespace).Get(ctx, secretRef, metav1.GetOptions{})
			if err != nil {
				// Fallback to default namespace
				sec, err = p.kubeClient.CoreV1().Secrets("default").Get(ctx, secretRef, metav1.GetOptions{})
			}
			if err == nil {
				if t, ok := sec.Data["api-token"]; ok {
					apiToken = string(t)
				}
			}
		}
	}

	metricSelector := m.Params["metricSelector"]
	if metricSelector == "" {
		metricSelector = m.Params["metric"]
	}
	if metricSelector == "" {
		metricSelector = class.Spec.Config["metricSelector"]
	}
	if metricSelector == "" {
		metricSelector = class.Spec.Config["metric"]
	}
	if metricSelector == "" {
		return nil, fmt.Errorf("metricSelector not defined in metric params or provider class")
	}

	entitySelector := m.Params["entitySelector"]
	if entitySelector == "" {
		entitySelector = m.Params["selector"]
	}
	if entitySelector == "" {
		entitySelector = class.Spec.Config["entitySelector"]
	}
	if entitySelector == "" {
		if serviceEntity := class.Spec.Config["serviceEntity"]; serviceEntity != "" {
			entitySelector = fmt.Sprintf("type(SERVICE),entityId(%s)", serviceEntity)
		}
	}

	clientKey := fmt.Sprintf("%s|%s", endpoint, apiToken)
	p.mu.Lock()
	dtClient, exists := p.clientCache[clientKey]
	if !exists {
		dtClient = NewClient(endpoint, apiToken)
		p.clientCache[clientKey] = dtClient
	}
	p.mu.Unlock()

	val, ts, err := dtClient.QueryMetric(ctx, metricSelector, entitySelector)
	if err != nil {
		return nil, err
	}

	slog.Info("Scraped Dynatrace metric",
		"metric", m.Name,
		"selector", metricSelector,
		"value", val,
		"timestamp", ts,
	)

	return &pb.MetricSample{
		Name:            m.Name,
		RecommenderName: m.RecommenderName,
		Value:           val,
		Timestamp:       ts,
	}, nil
}

// metricDefinitions extracts all metric definitions referenced by a policy.
func metricDefinitions(p *pb.Policy) []*pb.MetricDefinition {
	metrics := make([]*pb.MetricDefinition, 0, len(p.Metrics))
	metrics = append(metrics, p.Metrics...)
	for _, ml := range p.RecommenderMetrics {
		metrics = append(metrics, ml.GetDefinitions()...)
	}
	return metrics
}
