# Duihua Background Worker

Rust worker that consumes `background=true` OpenAI Responses API jobs from a Valkey stream via [responses-api-store](https://github.com/beranekio/responses-api-store), calls upstream inference, and persists completed responses.

This repository was extracted from [beranekio/duihua-ai-services](https://github.com/beranekio/duihua-ai-services) so the background worker can be developed and released independently.

## Repository layout

| Path | Purpose |
| --- | --- |
| `src/` | Background worker service source |
| `charts/duihua-background-worker/` | Helm subchart for Kubernetes deployment |
| `scripts/helm-smoke-kind.sh` | Local/CI Helm smoke test against kind |
| `Dockerfile` | Multi-stage build: `rust:1-bookworm` builder, distroless runtime |

## How it works

1. The gateway enqueues background Responses API jobs into a Valkey stream when `RESPONSES_BACKGROUND_ENABLED=true`.
2. This worker joins a consumer group, claims jobs, and atomically transitions each response to `in_progress` in the store.
3. It POSTs the stored upstream request to `{upstream}/responses`, then writes the completed (or failed) response back to the store.
4. Stale `in_progress` entries reclaimed after worker crashes are marked failed.

The worker has no HTTP server. Startup completion is logged as:

```text
background worker startup: recommended terminationGracePeriodSeconds=<n>
```

## Configuration

| Variable | Default | Role |
| --- | --- | --- |
| `RESPONSES_API_STORE_ENDPOINT` | `http://responses-api-store:50051` | gRPC store endpoint |
| `RESPONSE_ID_STORE_TTL_SECONDS` | `86400` | Stored response TTL |
| `BACKGROUND_QUEUE_CONSUMER_GROUP` | `duihua-background` | Valkey stream consumer group |
| `BACKGROUND_QUEUE_CONSUMER_NAME` | `<hostname>-<pid>` | Consumer name (Helm sets pod name) |
| `BACKGROUND_QUEUE_BLOCK_MS` | `5000` | `XREADGROUP` block timeout |
| `BACKGROUND_QUEUE_AUTOCLAIM_MIN_IDLE_MS` | derived from upstream timeout | `XAUTOCLAIM` min idle |
| `BACKGROUND_QUEUE_PENDING_RETRY_SECONDS` | `30` | Backoff for retryable messages |
| `BACKGROUND_QUEUE_MAX_CONCURRENT_JOBS` | `1` | Max parallel upstream calls per pod |
| `BACKGROUND_UPSTREAM_TIMEOUT_SECONDS` | `600` | Upstream HTTP timeout |
| `UPSTREAM_API_KEY` | (none) | Optional bearer token for upstream |

`terminationGracePeriodSeconds` in Helm should exceed `BACKGROUND_UPSTREAM_TIMEOUT_SECONDS + blockMs/1000 + 60`. The worker logs the recommended value at startup.

## Local development

### Rust (native)

Requires a running `responses-api-store` and Valkey (see Docker Compose below).

```bash
cargo build
RESPONSES_API_STORE_ENDPOINT=http://127.0.0.1:50051 cargo run
```

### Docker Compose

```bash
docker compose up --build
```

### Validation

```bash
make ci
```

Build a local container image:

```bash
make docker
```

## Helm deployment

Install the chart directly (requires a reachable `responses-api-store` endpoint):

```bash
helm upgrade --install duihua-background-worker charts/duihua-background-worker \
  --namespace duihua \
  --create-namespace \
  --set responsesApiStore.endpoint=http://responses-api-store:50051
```

With KEDA autoscaling via store metrics:

```yaml
autoscaling:
  enabled: true
  driver: store-metrics
  metricsUrl: http://responses-api-store:8080/metrics/background-queue?consumer_group=duihua-background
  replicas:
    min: 0
    max: 4
```

When embedded in [duihua-ai-services](https://github.com/beranekio/duihua-ai-services), add an OCI chart dependency (same pattern as `duihua-gateway`):

```yaml
dependencies:
  - name: duihua-background-worker
    version: 0.0.0-<git-sha>
    repository: oci://ghcr.io/beranekio/charts
```

Parent chart values use the `duihua-background-worker:` key (replacing the former inline `backgroundWorker` templates). Wire `responsesApiStore.endpoint` to the `responses-api-store` subchart Service and align `consumerGroup` with `duihua-gateway.responsesApiStore.backgroundJobs.consumerGroup`.

## CI and releases

- **Validate** (PRs and `main`): Rust fmt/clippy/tests, Helm lint, Dockerfile lint, Docker build, kind Helm smoke test.
- **Publish** (after Validate succeeds on `main` push): pushes `ghcr.io/beranekio/duihua-background-worker:<git-sha>` and publishes the Helm chart to `oci://ghcr.io/beranekio/charts` as `0.0.0-<git-sha>`.

## Integration with duihua-ai-services

The background worker is intended to replace the inline `background-worker-*` templates in `charts/duihua-ai-services`. Parent charts should:

- Depend on this repo's published OCI Helm chart.
- Enable the worker only when gateway Responses API store and background jobs are enabled.
- Set `responsesApiStore.endpoint` from the `responses-api-store` subchart Service.
- Set `autoscaling.metricsUrl` when using KEDA `store-metrics` driver.
- Keep ingress and gateway configuration in the parent chart.