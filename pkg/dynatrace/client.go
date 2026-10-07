package dynatrace

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"
)

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
	Values       []float64         `json:"values"`
}

// Client interacts with the Dynatrace Metrics API v2.
type Client struct {
	endpoint   string
	apiToken   string
	httpClient *http.Client
}

// NewClient initializes a new Dynatrace API client.
func NewClient(endpoint, apiToken string) *Client {
	return &Client{
		endpoint: endpoint,
		apiToken: apiToken,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// QueryMetric executes a query against Dynatrace /api/v2/metrics/query and extracts the latest value.
func (c *Client) QueryMetric(ctx context.Context, metricSelector, entitySelector string) (float64, int64, error) {
	reqURL, err := url.Parse(fmt.Sprintf("%s/api/v2/metrics/query", c.endpoint))
	if err != nil {
		return 0, 0, fmt.Errorf("invalid Dynatrace endpoint URL: %w", err)
	}

	params := url.Values{}
	params.Add("metricSelector", metricSelector)
	params.Add("resolution", "1m")
	if entitySelector != "" {
		params.Add("entitySelector", entitySelector)
	}
	reqURL.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Api-Token %s", c.apiToken))
	req.Header.Set("Accept", "application/json")

	slog.Debug("Executing Dynatrace API query", "url", reqURL.String(), "metricSelector", metricSelector)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, 0, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("Dynatrace API returned status %d: %s", resp.StatusCode, string(body))
	}

	var dtResp DynatraceQueryResponse
	if err := json.Unmarshal(body, &dtResp); err != nil {
		return 0, 0, fmt.Errorf("failed to parse JSON response: %w", err)
	}

	if len(dtResp.Result) == 0 || len(dtResp.Result[0].Data) == 0 {
		return 0, 0, fmt.Errorf("no data returned for metric %s", metricSelector)
	}

	dataPoint := dtResp.Result[0].Data[0]
	if len(dataPoint.Values) == 0 {
		return 0, 0, fmt.Errorf("empty values array in Dynatrace datapoint")
	}

	latestVal := dataPoint.Values[len(dataPoint.Values)-1]
	var ts int64 = time.Now().Unix()
	if len(dataPoint.Timestamps) > 0 {
		rawTs := dataPoint.Timestamps[len(dataPoint.Timestamps)-1]
		if rawTs > 1e11 {
			ts = rawTs / 1000 // Convert ms to seconds
		} else {
			ts = rawTs
		}
	}

	return latestVal, ts, nil
}
