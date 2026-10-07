package dynatrace

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestQueryMetricSuccess(t *testing.T) {
	mockResponse := `{
		"totalCount": 1,
		"resolution": "1m",
		"result": [
			{
				"metricId": "builtin:service.response.time:avg",
				"data": [
					{
						"dimensions": ["SERVICE-12345"],
						"timestamps": [1727700000000, 1727700060000],
						"values": [120.5, 145.2]
					}
				]
			}
		]
	}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v2/metrics/query" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if auth := r.Header.Get("Authorization"); auth != "Api-Token test-token" {
			t.Errorf("unexpected auth header: %s", auth)
		}
		if selector := r.URL.Query().Get("metricSelector"); selector != "builtin:service.response.time:avg" {
			t.Errorf("unexpected metricSelector: %s", selector)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	client := NewClient(server.URL, "test-token")
	val, ts, err := client.QueryMetric(context.Background(), "builtin:service.response.time:avg", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if val != 145.2 {
		t.Errorf("expected value 145.2, got %f", val)
	}
	if ts != 1727700060 {
		t.Errorf("expected timestamp 1727700060, got %d", ts)
	}
}

func TestQueryMetricErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error": {"code": 401, "message": "Invalid token"}}`))
	}))
	defer server.Close()

	client := NewClient(server.URL, "invalid-token")
	_, _, err := client.QueryMetric(context.Background(), "builtin:service.response.time:avg", "")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}
