# Dynatrace Metric Provider for xAS

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![xAS Compatible](https://img.shields.io/badge/xAS-v1alpha-green)](https://github.com/gke-labs/extensible-workload-autoscaler)

A standalone **Metric Provider** for the **[Extensible Workload Autoscaler (xAS)](https://github.com/gke-labs/extensible-workload-autoscaler)** that integrates **Dynatrace APM** observability directly into Kubernetes autoscaling.

---

## Overview

xAS decouples metric collection, recommendation algorithms, and workload actuation in Kubernetes. Rather than running a custom controller or duplicating autoscaling logic, this provider functions as a native plug-in metric collector:

- **Zero Modifications to xAS Core**: Integrates cleanly via official xAS gRPC APIs (`IngestMetrics`, `ListPolicies`) and standard `MetricProviderClass` Custom Resources.
- **Enterprise APM Grounding**: Scales workloads based on true business and user-experience signals from Dynatrace (e.g., P90/P99 service response time, failure rates, queue depth) rather than coarse CPU/memory spikes.
- **Dynamic Policy Resolution**: Automatically discovers which workloads and metrics require Dynatrace telemetry by querying the centralized `xas-server`.

---

## Architecture

```
                  +-----------------------------------+
                  |         Dynatrace SaaS /          |
                  |         ActiveGate API            |
                  +-----------------+-----------------+
                                    |
                                    | HTTP GET /api/v2/metrics/query
                                    | (Api-Token auth)
                                    v
+-------------------------------------------------------------------------+
| Kubernetes Cluster                                                      |
|                                                                         |
|  +----------------------------------+                                   |
|  |  xas-dynatrace-metrics-provider  | (This Repository)                 |
|  |  - Watches MetricProviderClasses |                                   |
|  |  - Scrapes Dynatrace metrics     |                                   |
|  +-----------------+----------------+                                   |
|                    |                                                    |
|                    | gRPC IngestMetrics (Port 8080)                     |
|                    v                                                    |
|  +----------------------------------+                                   |
|  |            xas-server            | (Official xAS Control Plane)      |
|  |  - Aggregates metric timeseries  |                                   |
|  |  - Coordinates recommenders      |                                   |
|  +-----------------+----------------+                                   |
|                    |                                                    |
|                    | gRPC GetRecommendations                            |
|                    v                                                    |
|  +----------------------------------+      Scale Deployment             |
|  |          xas-controller          | ------------------------> [ App ] |
|  |  - Actuates Kubernetes replicas  |                                   |
|  +----------------------------------+                                   |
+-------------------------------------------------------------------------+
```

---

## Project Structure

```
├── cmd/
│   └── dynatrace-provider/
│       └── main.go                  # Daemon entrypoint, flags, informers
├── pkg/
│   └── dynatrace/
│       ├── client.go                # Dynatrace v2 Metrics API HTTP client
│       ├── client_test.go           # Unit tests with mock HTTP server
│       └── provider.go              # Scrape loop & gRPC IngestMetrics client
├── deploy/
│   ├── xas-dynatrace-metrics-provider.yaml # RBAC & Deployment for provider
│   ├── metricproviderclass-dynatrace.yaml  # MetricProviderClass definition
│   ├── secret-dynatrace-token.yaml         # Secret template for Dynatrace token
│   ├── sample-workload.yaml                # Sample Deployment to scale
│   └── sample-scalingpolicy.yaml           # Sample ScalingPolicy manifest
├── mock-dynatrace/                  # Standalone mock Dynatrace server & UI for testing
├── Dockerfile                       # Multi-stage distroless container build
├── go.mod                           # Go module importing upstream xAS
└── LICENSE                          # Apache 2.0 License
```

---

## Quickstart & Deployment

### 1. Prerequisites

- A running Kubernetes cluster (v1.28+)
- Official xAS control plane installed in `xas-system` namespace. If not already installed, follow the [official xAS Quickstart](https://github.com/gke-labs/extensible-workload-autoscaler):
  ```bash
  git clone https://github.com/gke-labs/extensible-workload-autoscaler.git
  cd extensible-workload-autoscaler

  # 1. Install xAS CRDs
  kubectl apply -f deploy/crd/

  # 2. Deploy xAS control plane & recommenders
  # On a local KinD cluster:
  ./hack/deploy.sh
  # Or on any remote Kubernetes cluster (GKE, EKS, AKS, on-prem):
  export KO_DOCKER_REPO="<your-container-registry>" # e.g. docker.io/<user>, gcr.io/<project>, ECR, or ACR
  ./hack/run-tool.sh ko resolve -f deploy/xas-control-plane.yaml | kubectl apply -f -
  ./hack/run-tool.sh ko resolve -f deploy/xas-core-recommenders.yaml | kubectl apply -f -
  ```

### 2. Configure Dynatrace API Secret

Store your Dynatrace API token (requires `metrics.read` permission) in a Kubernetes secret:

```yaml
# deploy/secret-dynatrace-token.yaml
apiVersion: v1
kind: Secret
metadata:
  name: dynatrace-token
  namespace: default
type: Opaque
stringData:
  api-token: "dt0c01.YOUR_DYNATRACE_API_TOKEN"
```

Apply the secret:
```bash
kubectl apply -f deploy/secret-dynatrace-token.yaml
```

### 3. Register the MetricProviderClass

Declare Dynatrace as an available metric provider in your cluster using [`deploy/metricproviderclass-dynatrace.yaml`](deploy/metricproviderclass-dynatrace.yaml).

> **Note**: By default, the provided manifest points to the public mock testing server (`http://34.140.132.84:8080`). To connect to real Dynatrace, change `endpoint` to your SaaS tenant URL (e.g., `https://<env-id>.live.dynatrace.com`).

Apply the class:
```bash
kubectl apply -f deploy/metricproviderclass-dynatrace.yaml
```

### 4. Deploy the Dynatrace Provider

Deploy the provider daemon to `xas-system`. You can deploy directly using `ko` or build via Docker:

```bash
# Option A: Deploy using ko (standard for xAS)
export KO_DOCKER_REPO="<your-container-registry>"
ko resolve -f deploy/xas-dynatrace-metrics-provider.yaml | kubectl apply -f -

# Option B: Build with Docker
docker build -t <your-registry>/xas-dynatrace-provider:latest .
docker push <your-registry>/xas-dynatrace-provider:latest
kubectl apply -f deploy/xas-dynatrace-metrics-provider.yaml
```

Check the logs to verify connection to `xas-server`:
```bash
kubectl logs -n xas-system -l app=xas-dynatrace-metrics-provider -f
```

---

## Example: Autoscaling a Workload

Once the provider is running, application teams can scale any Kubernetes Deployment by referencing `provider: "dynatrace"` in a standard xAS `ScalingPolicy`.

See [`deploy/sample-scalingpolicy.yaml`](deploy/sample-scalingpolicy.yaml) for a complete example. The key configuration fields are:

* **`scaleTargetRef`**: The Deployment to autoscale (e.g. `sample-workload`).
* **`provider: "dynatrace"`**: Tells xAS to fetch metrics from this Dynatrace provider.
* **`params.metricSelector`**: The Dynatrace metric ID (e.g. `builtin:service.response.time:avg` or `builtin:service.response.time:percentile(90)`).
* **`params.entitySelector`** *(optional)*: Filter by entity type or tag (e.g. `type(SERVICE),tag(production)`).
* **`target`**: The desired threshold (e.g. maintain response time around `150.0` ms).

To deploy the sample workload and its scaling policy:
```bash
# 1. Deploy the sample workload (nginx)
kubectl apply -f deploy/sample-workload.yaml

# 2. Attach the Dynatrace autoscaling policy
kubectl apply -f deploy/sample-scalingpolicy.yaml
```

---

## Configuration Reference

### Provider CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--server-address` | `xas-server.xas-system.svc.cluster.local:80` | gRPC address of the central xAS server |
| `--cluster-name` | `default` | Cluster identifier matching xAS policy definitions |
| `--scrape-interval` | `5s` | Interval between Dynatrace metric queries |
| `--debug` | `false` | Enable verbose debug logging |

### Dynatrace Parameters

| Location | Parameter | Description |
|----------|-----------|-------------|
| `MetricProviderClass.spec.config` | `endpoint` | Base URL of Dynatrace environment (or ActiveGate/mock) |
| `MetricProviderClass.spec.config` | `apiTokenSecretRef` | Name of Kubernetes Secret containing `api-token` |
| `ScalingPolicy.metrics[].params` | `metricSelector` | Dynatrace metric identifier (alias: `metric`) |
| `ScalingPolicy.metrics[].params` | `entitySelector` | Dynatrace entity filter (alias: `selector`) |

---

## Development & Testing

### Build Binary
```bash
go build -o bin/dynatrace-provider ./cmd/dynatrace-provider
```

### Run Tests
```bash
go test -v ./...
```

### Build Container Image
```bash
docker build -t xas-dynatrace-provider:latest .
```

---

## License

This project is licensed under the [Apache 2.0 License](LICENSE).
