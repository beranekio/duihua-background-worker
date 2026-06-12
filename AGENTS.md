# AGENTS.md

Guidance for human and AI contributors working in this repository.

## Project overview

This repo provides a **background worker** (Rust, Tokio) that consumes async OpenAI Responses API jobs from a Valkey stream via the [responses-api-store](https://github.com/beranekio/responses-api-store) gRPC client, executes upstream inference calls, and persists results.

It was extracted from [beranekio/duihua-ai-services](https://github.com/beranekio/duihua-ai-services) for independent development and release.

## Repository layout

| Path | Purpose |
| --- | --- |
| `src/` | Worker service source (`main.rs`, `queue.rs`, `worker.rs`, `responses_store.rs`) |
| `charts/duihua-background-worker/` | Helm subchart |
| `scripts/helm-smoke-kind.sh` | kind-based Helm smoke test |
| `Dockerfile` | Multi-stage build: `rust:1-bookworm` builder, `gcr.io/distroless/cc-debian12:nonroot` runtime |

## Recommended workflow

1. Read `README.md` and the relevant source module before editing.
2. Keep changes focused and minimal to the requested task.
3. Update `README.md` and Helm values/templates when behavior or configuration changes.
4. Run targeted validation for the areas you modified (see [Validation commands](#validation-commands)).

## Validation commands

Run checks that match the files you changed. From the repository root:

### Full CI parity

```bash
make ci
```

### Rust

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test
```

### Helm chart

```bash
helm lint charts/duihua-background-worker
helm template duihua-background-worker charts/duihua-background-worker \
  --set responsesApiStore.endpoint=http://responses-api-store:50051 \
  --debug >/tmp/duihua-background-worker-rendered.yaml
```

### Docker

```bash
docker build -t duihua-background-worker:local .
```

The builder stage requires `protobuf-compiler` (for the `responses-api-store-client` git dependency). The runtime image is distroless; do not add a shell or package manager to the runtime stage.

### Docker Compose

```bash
docker compose up --build
```

### Helm chart smoke test (kind)

```bash
make helm-smoke
```

Requires `kind`, `kubectl`, `helm`, `docker`, and network access to pull `responses-api-store` and Valkey images. CI runs this in the `helm-smoke` job after `docker-build`.

## Editing conventions

- Preserve existing naming and style; match patterns from `duihua-gateway` and `duihua-ai-services` where domains overlap.
- Avoid unrelated refactors in the same commit.
- Keep Kubernetes defaults cloud-provider-neutral unless explicitly required.
- Document user-visible changes in `README.md`.

## Integration with duihua-ai-services

When wiring this service back into `duihua-ai-services`:

- Replace inline `background-worker-deployment.yaml` / `background-worker-scaledobject.yaml` templates with an OCI subchart dependency on `duihua-background-worker`.
- Parent chart values should use the `duihua-background-worker:` key (not `backgroundWorker:`).
- Parent chart should wire `responsesApiStore.endpoint` to the `responses-api-store` subchart Service.
- Align `consumerGroup` with `duihua-gateway.responsesApiStore.backgroundJobs.consumerGroup`.
- Set `autoscaling.metricsUrl` for KEDA when using the `store-metrics` driver.
- Ingress and gateway remain in the parent chart.

## Agent-specific notes

### Opening pull requests

When creating a PR, **add a GitHub label that identifies the agent** (or tooling) that authored it.

| Agent / tool | Label |
| --- | --- |
| ChatGPT Codex | `codex` |
| Cursor | `cursor` |
| Claude | `claude` |
| Grok | `grok` |

```bash
gh pr create --label grok ...
```

Include in the PR description:

- What changed and why
- How it was validated (exact commands)
- Whether Helm changes affect downstream consumers in `duihua-ai-services`