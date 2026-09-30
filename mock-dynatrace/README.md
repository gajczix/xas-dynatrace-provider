# High-Fidelity Mock Dynatrace & Autoscaling Latency Benchmark

This service provides an authentic simulation of the **Dynatrace Metrics API v2** paired with an interactive Web Dashboard and **hop-by-hop autoscaling reaction-time waterfall** for benchmarking Kubernetes autoscalers (such as xAS).

It is designed to run as an **external service** (e.g. on a standalone Compute Engine VM or locally) outside the target Kubernetes cluster to simulate a real third-party observability platform.

---

## Capabilities

1. **Authentic Dynatrace Metrics API v2 (`GET /api/v2/metrics/query`):**
   - Enforces standard Dynatrace token authorization (`Authorization: Api-Token <token>`).
   - Returns standard Dynatrace JSON payloads with Unix epoch **millisecond** timestamps, dimension maps, and value arrays.
2. **5 Built-In Scaling Scenarios & Jitter Stress Test:**
   - **P90 Response Latency:** `builtin:service.response.time:percentile(90)` (Normal: 90ms | Spike: 420ms | Target: 150ms)
   - **P90 Latency Jitter / Flapping:** Dynamically oscillates between 135ms and 165ms every 2s to test stabilization windows.
   - **Queue Backlog Depth:** `custom:queue.messages_pending` (Normal: 10 msgs | Surge: 1,500 msgs | Target: 50 msgs/pod)
   - **Kafka Consumer Lag:** `custom:queue.consumer_lag_seconds` (Normal: 2s | Backlog: 120s | Target: 10s)
   - **Throughput Request Rate (QPS):** `builtin:service.requestCount.rate` (Normal: 50 req/s | Surge: 350 req/s | Target: 80 req/s)
   - **Service Failure / Error Rate:** `builtin:service.errors.total.rate` (Normal: 0.1 err/s | Spike: 55.0 err/s | Target: 5.0 err/s)
3. **Hop-by-Hop Reaction Waterfall Stopwatch:**
   - **Hop 1 (Ingest Delay):** Time from user click until the metric provider polls Dynatrace.
   - **Hop 2 (Decision Delay):** Time for the xAS recommender algorithm to compute the new target.
   - **Hop 3 (Actuation Delay):** Time until the controller updates Kubernetes `spec.replicas`.
   - **Total Reaction Time:** End-to-end latency timer and historical run log.
4. **Interactive Web Dashboard:**
   - Accessible at `http://<host>:8080/` with 1-click scenario triggers, live metric payload inspection, and live Kubernetes pod pills.

---

## Running Locally

To run the mock server locally:

```bash
cd mock-dynatrace
go run .
```

Open `http://localhost:8080` in your browser.

> ⚠️ **Important Network Note:** `http://localhost:8080` only works if the Kubernetes cluster is running on the same local machine (e.g. Minikube or Kind with host networking) or if run in the same cluster. If your Kubernetes cluster is running remotely (e.g. GKE, EKS, AKS), in-cluster pods cannot connect to your laptop's `localhost`. For remote clusters, use the hosted quickstart server or run on an external VM as shown below.

---

## Running Externally (e.g., Cloud VM / Compute Engine)

If you are benchmarking against a remote Kubernetes cluster (such as GKE) and want to host your own isolated mock server:

### 1. Open Firewall Port 8080
Cloud providers block incoming traffic by default. Create a firewall rule allowing TCP port 8080:

```bash
# Example for Google Cloud:
gcloud compute firewall-rules create allow-mock-dynatrace-8080 \
    --allow=tcp:8080 \
    --target-tags=mock-dynatrace \
    --description="Allow port 8080 for mock Dynatrace server"
```

### 2. Copy Code to Your VM
Copy the `mock-dynatrace` directory to your VM:

```bash
scp -r ./mock-dynatrace user@<VM_IP>:~
```

### 3. Start the Server on the VM
SSH into your VM, install Go if needed, and run:

```bash
ssh user@<VM_IP>

# If Go is not yet installed (Debian/Ubuntu):
sudo apt-get update && sudo apt-get install -y golang-go

cd mock-dynatrace
go run .
```

The server listens on port `:8080` and is accessible at `http://<VM_IP>:8080/`.

### 4. (Optional) Connect UI to Cluster for Live Pod Status
The mock server serves Dynatrace metrics out of the box without needing any Kubernetes credentials. However, if you want Card 3 in the web UI to display live pod status badges from outside the cluster, pass:

```bash
export K8S_HOST="https://<K8S_API_SERVER_IP>"
export K8S_TOKEN="<SERVICE_ACCOUNT_BEARER_TOKEN>"
export TARGET_NAMESPACE="default"
export TARGET_DEPLOYMENT="sample-workload"
export SCALING_POLICY_NAME="sample-workload-scaling"

go run .
```

### Configuration Environment Variables

| Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` | `8080` | HTTP listen port |
| `DYNATRACE_API_TOKEN` | `dt0c01.samplemocktoken123456789.mocksecret` | Expected Dynatrace API token |
| `K8S_HOST` | *(kubeconfig)* | Kubernetes API master URL |
| `K8S_TOKEN` | *(kubeconfig)* | Kubernetes ServiceAccount bearer token |
| `TARGET_NAMESPACE` | `default` | Kubernetes namespace of target workload |
| `TARGET_DEPLOYMENT` | `sample-workload` | Name of Kubernetes deployment to observe |
| `SCALING_POLICY_NAME` | `sample-workload-scaling` | Name of `ScalingPolicy` custom resource |

---

## API Endpoints & cURL Testing

```bash
# Query metric (using the Dynatrace API token)
curl -H "Authorization: Api-Token dt0c01.samplemocktoken123456789.mocksecret" \
     "http://localhost:8080/api/v2/metrics/query?metricSelector=builtin:service.response.time:percentile(90)"

# Trigger the P90 latency spike (90ms -> 420ms)
curl -X POST http://localhost:8080/mock/scenario/p90-latency

# Toggle P90 jitter / flapping test
curl -X POST http://localhost:8080/mock/scenario/jitter

# Reset all metrics to normal baseline
curl -X POST http://localhost:8080/mock/reset

# Inspect current status & waterfall metrics in JSON
curl http://localhost:8080/mock/status
```
