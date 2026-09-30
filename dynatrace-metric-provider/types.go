package main

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MetricProviderClass defines a reusable configuration for a metric source.
// This strictly conforms to the xAS schema (xas.io/v1).
type MetricProviderClass struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec MetricProviderClassSpec `json:"spec,omitempty"`
}

// MetricProviderClassSpec defines the provider type and its key-value parameters.
type MetricProviderClassSpec struct {
	// Type defines the provider implementation (e.g., "Dynatrace")
	Type string `json:"type"`

	// Config is a map of static configuration passed to the provider.
	Config map[string]string `json:"config,omitempty"`
}

// DynatraceQueryResponse models the authentic Dynatrace Metrics API v2 response schema.
type DynatraceQueryResponse struct {
	TotalCount int               `json:"totalCount"`
	Resolution string            `json:"resolution"`
	Result     []DynatraceResult `json:"result"`
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

// IngestedMetric is the standardized metric payload passed to the xAS Ingestion engine.
type IngestedMetric struct {
	ProviderName string            `json:"providerName"`
	MetricName   string            `json:"metricName"`
	Value        float64           `json:"value"`
	Unit         string            `json:"unit"`
	TimestampMs  int64             `json:"timestampMs"`
	Labels       map[string]string `json:"labels"`
}
