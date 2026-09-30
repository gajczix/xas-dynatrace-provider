# xAS Dynatrace Metric Provider & Autoscaling Benchmark

An end-to-end prototype validating how [**xAS (Extensible Workload Autoscaler)**](https://github.com/gke-labs/extensible-workload-autoscaler) ingests external application telemetry from **Dynatrace** to drive predictive and responsive Kubernetes autoscaling.

> 🚀 **Live Demo Available:** You can explore the running benchmark dashboard without any local setup at: [http://34.140.132.84:8080/](http://34.140.132.84:8080/)
>
> 🔗 **Upstream xAS Prototype:** [github.com/gke-labs/extensible-workload-autoscaler](https://github.com/gke-labs/extensible-workload-autoscaler)
---

## Architecture Overview

```
┌─────────────────────────────────────────┐
│     EXTERNAL OBSERVABILITY LAYER        │
│  (Compute Engine VM / SaaS Dynatrace)   │
│                                         │
│   Mock Dynatrace Server & Benchmark UI  │
│   • Serves Dynatrace Metrics API v2     │
│   • Simulates P90 Latency Spikes (420ms)│
│   • Tracks Hop-by-Hop Latency Waterfall │
└───────────────────┬─────────────────────┘
                    │ HTTP GET /api/v2/metrics/query
                    │ (Scraped every 5s)
                    ▼
┌───────────────────┴─────────────────────┐
│          KUBERNETES CLUSTER             │
│                                         │
│   1. dynatrace-metric-provider Pod      │
│      • Ingests external P90 signal      │
│      • Governed by MetricProviderClass  │
│                                         │
│   2. xas-controller Pod                 │
│      • Evaluates ScalingPolicy (150ms)  │
│      • Linear recommender algorithm     │
│                                         │
│   3. sample-workload Deployment         │
│      • Target deployment (1 → 5 pods)   │
└─────────────────────────────────────────┘
```

---

## Prerequisites

- A running Kubernetes cluster with `kubectl` configured.
- Docker installed locally to build container images.

---

## Step 1: Build & Push Container Images

Build and push the in-cluster components to your container registry (e.g. Docker Hub, GHCR, or Artifact Registry):

```bash
export REGISTRY="<YOUR_CONTAINER_REGISTRY>"

docker build -t ${REGISTRY}/dynatrace-metric-provider:latest ./dynatrace-metric-provider
docker push ${REGISTRY}/dynatrace-metric-provider:latest

docker build -t ${REGISTRY}/xas-controller:latest ./xas-controller
docker push ${REGISTRY}/xas-controller:latest
```

---

## Step 2: Configure Your Dynatrace Endpoint

Select your Dynatrace telemetry source:

```bash
# Option A: Quickstart using the live hosted mock server & benchmark UI
export DYNATRACE_ENDPOINT="http://34.140.132.84:8080"

# Option B: Real Dynatrace SaaS tenant
# export DYNATRACE_ENDPOINT="https://<TENANT>.live.dynatrace.com"
```

> 💡 **Self-Hosting the Mock Server:** If you prefer running your own private mock instance on a separate VM instead of using the hosted server, see [`mock-dynatrace/README.md`](mock-dynatrace/README.md).

---

## Step 3: Deploy to Kubernetes

Deploy the Custom Resource Definitions, controllers, and workload to your cluster:

```bash
# 1. Register the xAS CRDs (MetricProviderClass and ScalingPolicy)
kubectl apply -f crds/

# 2. Grant controller permissions
kubectl create clusterrolebinding default-admin --clusterrole=cluster-admin --serviceaccount=default:default

# 3. Deploy components
kubectl apply -f deploy/

# 4. Point deployments to your pushed images
kubectl set image deployment/dynatrace-metric-provider provider="${REGISTRY}/dynatrace-metric-provider:latest"
kubectl set image deployment/xas-controller controller="${REGISTRY}/xas-controller:latest"

# 5. Configure the metric provider endpoint
kubectl set env deployment/dynatrace-metric-provider DYNATRACE_ENDPOINT="$DYNATRACE_ENDPOINT"
```

---

## Step 4 (Optional): Test Autoscaling & Benchmark Dashboard

If using the mock server (`http://34.140.132.84:8080/`), open it in your browser:
1. Click **P90 Response Latency** to simulate a latency surge (`420 ms` vs `150 ms` target).
2. Click **Return All Metrics to Normal** to scale back down to **1 pod**.
3. The benchmark shows you how sample GKE cluster reacts to the changes. To see your own cluster, use kubectl get pods.

---

## Repository Structure

```
├── crds/                         # xAS Custom Resource Definitions
│   ├── metricproviderclass.yaml
│   └── scalingpolicy.yaml
├── deploy/                       # Kubernetes deployment manifests
│   ├── deployment-metric-provider.yaml
│   ├── deployment-xas-controller.yaml
│   ├── metricproviderclass-dynatrace.yaml
│   ├── sample-workload.yaml
│   └── secret-dynatrace-token.yaml
├── dynatrace-metric-provider/    # In-cluster metric ingestion daemon
├── mock-dynatrace/               # External Dynatrace server & benchmark UI
│   ├── dashboard.html
│   └── main.go
└── xas-controller/               # Recommender & scaling actuator controller
```
