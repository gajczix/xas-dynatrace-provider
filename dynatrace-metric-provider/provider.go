package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// DynatraceMetricProvider manages polling Dynatrace according to a MetricProviderClass spec.
type DynatraceMetricProvider struct {
	classSpec      *MetricProviderClass
	client         *DynatraceClient
	scrapeInterval time.Duration
	metricSelector string
	serviceEntity  string
	latestMetric   *IngestedMetric
}

// NewDynatraceMetricProvider initializes the provider from a MetricProviderClass configuration.
func NewDynatraceMetricProvider(class *MetricProviderClass, apiToken string) (*DynatraceMetricProvider, error) {
	if class.Spec.Type != "Dynatrace" {
		return nil, fmt.Errorf("unsupported provider type '%s', expected 'Dynatrace'", class.Spec.Type)
	}

	endpoint := class.Spec.Config["endpoint"]
	if endpoint == "" {
		return nil, fmt.Errorf("metric provider endpoint is required in configuration")
	}

	selector := class.Spec.Config["metricSelector"]
	if selector == "" {
		selector = "builtin:service.response.time:percentile(90)"
	}

	intervalStr := class.Spec.Config["scrapeInterval"]
	interval := 5 * time.Second
	if intervalStr != "" {
		if d, err := time.ParseDuration(intervalStr); err == nil {
			interval = d
		}
	}

	serviceEntity := class.Spec.Config["serviceEntity"]
	if serviceEntity == "" {
		serviceEntity = "SERVICE-SAMPLE-01"
	}

	client := NewDynatraceClient(endpoint, apiToken)

	return &DynatraceMetricProvider{
		classSpec:      class,
		client:         client,
		scrapeInterval: interval,
		metricSelector: selector,
		serviceEntity:  serviceEntity,
	}, nil
}

// StartPolling initiates the periodic metric collection loop.
func (p *DynatraceMetricProvider) StartPolling(ctx context.Context) {
	log.Printf("[DynatraceProvider] Starting polling loop for MetricProviderClass '%s'...", p.classSpec.Name)
	log.Printf("[DynatraceProvider] Target Metric: %s (Interval: %v)", p.metricSelector, p.scrapeInterval)

	ticker := time.NewTicker(p.scrapeInterval)
	defer ticker.Stop()

	// Initial scrape on startup
	p.scrapeOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[DynatraceProvider] Stopping polling loop.")
			return
		case <-ticker.C:
			p.scrapeOnce(ctx)
		}
	}
}

func (p *DynatraceMetricProvider) scrapeOnce(ctx context.Context) {
	val, ts, err := p.client.FetchP90Latency(ctx, p.metricSelector)
	if err != nil {
		log.Printf("[DynatraceProvider] Warning: Scrape failed for %s: %v", p.metricSelector, err)
		return
	}

	metric := &IngestedMetric{
		ProviderName: p.classSpec.Name,
		MetricName:   "p90_latency",
		Value:        val,
		Unit:         "ms",
		TimestampMs:  ts,
		Labels: map[string]string{
			"dt.entity.service": p.serviceEntity,
			"metric_id":         p.metricSelector,
		},
	}
	p.latestMetric = metric

	log.Printf("[DynatraceProvider] Scraped %s -> %.1f ms (timestamp: %d)", p.metricSelector, val, ts)

	// Notify xAS Ingestion / Benchmark Tracker of successful hop
	p.reportIngestHop(metric)
}

func (p *DynatraceMetricProvider) reportIngestHop(m *IngestedMetric) {
	endpoint := p.classSpec.Spec.Config["endpoint"]
	if endpoint == "" {
		return
	}

	payload, _ := json.Marshal(map[string]string{"hopName": "ingest"})
	req, err := http.NewRequest(http.MethodPost, endpoint+"/mock/hop", bytes.NewBuffer(payload))
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		client := &http.Client{Timeout: 1 * time.Second}
		resp, rErr := client.Do(req)
		if rErr == nil {
			resp.Body.Close()
		}
	}
}
