package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// DynatraceClient communicates with the Dynatrace Metrics API v2.
type DynatraceClient struct {
	endpoint   string
	apiToken   string
	httpClient *http.Client
}

func NewDynatraceClient(endpoint, apiToken string) *DynatraceClient {
	return &DynatraceClient{
		endpoint: endpoint,
		apiToken: apiToken,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// FetchP90Latency queries builtin:service.response.time:percentile(90) from Dynatrace.
func (c *DynatraceClient) FetchP90Latency(ctx context.Context, metricSelector string) (float64, int64, error) {
	if metricSelector == "" {
		metricSelector = "builtin:service.response.time:percentile(90)"
	}

	reqURL := fmt.Sprintf("%s/api/v2/metrics/query?metricSelector=%s", c.endpoint, url.QueryEscape(metricSelector))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create http request: %w", err)
	}

	// Dynatrace API v2 requires 'Api-Token <token>' authorization header
	req.Header.Set("Authorization", fmt.Sprintf("Api-Token %s", c.apiToken))
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to execute request to Dynatrace: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, 0, fmt.Errorf("dynatrace API returned HTTP %d: %s", resp.StatusCode, string(body))
	}

	var queryResp DynatraceQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return 0, 0, fmt.Errorf("failed to parse Dynatrace JSON response: %w", err)
	}

	if len(queryResp.Result) == 0 || len(queryResp.Result[0].Data) == 0 {
		return 0, 0, fmt.Errorf("no metric series returned for selector %s", metricSelector)
	}

	dataSeries := queryResp.Result[0].Data[0]
	if len(dataSeries.Values) == 0 {
		return 0, 0, fmt.Errorf("metric series returned empty values array")
	}

	// Grab the latest data point from the series
	lastIdx := len(dataSeries.Values) - 1
	valPtr := dataSeries.Values[lastIdx]
	if valPtr == nil {
		return 0, 0, fmt.Errorf("latest metric sample value is null")
	}

	var timestamp int64 = time.Now().UnixMilli()
	if len(dataSeries.Timestamps) > lastIdx {
		timestamp = dataSeries.Timestamps[lastIdx]
	}

	return *valPtr, timestamp, nil
}
