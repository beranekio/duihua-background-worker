#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CLUSTER_NAME="${CLUSTER_NAME:-duihua-background-worker-smoke}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-kind-${CLUSTER_NAME}}"
MANAGE_KIND_CLUSTER="${MANAGE_KIND_CLUSTER:-true}"
SKIP_DOCKER_BUILD="${SKIP_DOCKER_BUILD:-false}"
KEEP_CLUSTER="${KEEP_CLUSTER:-false}"
RELEASE="${RELEASE:-duihua-background-worker}"
CHART="${CHART:-charts/duihua-background-worker}"
IMAGE_REPOSITORY="${IMAGE_REPOSITORY:-duihua-background-worker}"
IMAGE_TAG="${IMAGE_TAG:-ci}"
IMAGE="${IMAGE_REPOSITORY}:${IMAGE_TAG}"
HELM_TIMEOUT="${HELM_TIMEOUT:-10m}"
STORE_RELEASE="${STORE_RELEASE:-responses-api-store-smoke}"
STORE_IMAGE="${STORE_IMAGE:-ghcr.io/beranekio/responses-api-store:latest}"
VALKEY_IMAGE="${VALKEY_IMAGE:-valkey/valkey:9.1.0-alpine}"
STARTUP_MARKER="background worker startup: recommended terminationGracePeriodSeconds="
CREATED_CLUSTER=false

log() {
  printf '==> %s\n' "$*"
}

kubectl_ctx() {
  kubectl --context "${KUBECTL_CONTEXT}" "$@"
}

dump_cluster_state() {
  log "cluster state (release=${RELEASE})"
  kubectl_ctx get pods,svc,deploy -l "app.kubernetes.io/instance=${RELEASE}" -o wide 2>/dev/null || true
  kubectl_ctx describe pods -l "app.kubernetes.io/name=duihua-background-worker,app.kubernetes.io/instance=${RELEASE}" 2>/dev/null || true
  kubectl_ctx logs -l "app.kubernetes.io/name=duihua-background-worker,app.kubernetes.io/instance=${RELEASE}" --tail=200 2>/dev/null || true
}

cleanup() {
  if [[ "${KEEP_CLUSTER}" == "true" ]]; then
    log "keeping kind cluster ${CLUSTER_NAME} for debugging"
    return
  fi
  if [[ "${CREATED_CLUSTER}" == "true" ]]; then
    log "deleting kind cluster ${CLUSTER_NAME}"
    kind delete cluster --name "${CLUSTER_NAME}"
  fi
}

trap cleanup EXIT

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

for cmd in kind kubectl helm docker; do
  require_cmd "$cmd"
done

if [[ "${MANAGE_KIND_CLUSTER}" == "true" ]]; then
  if kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
    echo "kind cluster '${CLUSTER_NAME}' already exists; choose a different CLUSTER_NAME or delete it manually" >&2
    exit 1
  fi
  log "creating kind cluster ${CLUSTER_NAME}"
  kind create cluster --name "${CLUSTER_NAME}"
  CREATED_CLUSTER=true
else
  log "using existing kind cluster ${CLUSTER_NAME} (context ${KUBECTL_CONTEXT})"
fi

if [[ "${SKIP_DOCKER_BUILD}" == "true" ]]; then
  log "using pre-built image ${IMAGE}"
  if ! docker image inspect "${IMAGE}" >/dev/null 2>&1; then
    echo "pre-built image ${IMAGE} not found locally; build it or unset SKIP_DOCKER_BUILD" >&2
    exit 1
  fi
else
  log "building image ${IMAGE}"
  docker build -t "${IMAGE}" .
fi

log "loading worker image into kind"
kind load docker-image --name "${CLUSTER_NAME}" "${IMAGE}"

log "deploying Valkey for smoke test"
kubectl_ctx apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${STORE_RELEASE}-valkey
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${STORE_RELEASE}-valkey
  template:
    metadata:
      labels:
        app: ${STORE_RELEASE}-valkey
    spec:
      containers:
        - name: valkey
          image: ${VALKEY_IMAGE}
          args: ["--save", "", "--appendonly", "no"]
          ports:
            - containerPort: 6379
---
apiVersion: v1
kind: Service
metadata:
  name: ${STORE_RELEASE}-valkey
  namespace: default
spec:
  selector:
    app: ${STORE_RELEASE}-valkey
  ports:
    - port: 6379
      targetPort: 6379
EOF

kubectl_ctx rollout status "deployment/${STORE_RELEASE}-valkey" --timeout=180s

log "deploying responses-api-store for smoke test"
kubectl_ctx apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${STORE_RELEASE}
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${STORE_RELEASE}
  template:
    metadata:
      labels:
        app: ${STORE_RELEASE}
    spec:
      containers:
        - name: responses-api-store
          image: ${STORE_IMAGE}
          imagePullPolicy: IfNotPresent
          env:
            - name: GRPC_LISTEN_ADDR
              value: "0.0.0.0:50051"
            - name: RESPONSE_ID_STORE_URL
              value: "redis://${STORE_RELEASE}-valkey:6379"
            - name: RESPONSE_ID_STORE_KEY_PREFIX
              value: "responses-api-store:responses"
            - name: RESPONSE_ID_STORE_TTL_SECONDS
              value: "86400"
            - name: BACKGROUND_QUEUE_STREAM_KEY
              value: "responses-api-store:background"
            - name: BACKGROUND_RESPONSE_STALE_SECONDS
              value: "3600"
          ports:
            - containerPort: 50051
---
apiVersion: v1
kind: Service
metadata:
  name: ${STORE_RELEASE}
  namespace: default
spec:
  selector:
    app: ${STORE_RELEASE}
  ports:
    - port: 50051
      targetPort: 50051
EOF

kubectl_ctx rollout status "deployment/${STORE_RELEASE}" --timeout=300s

log "installing Helm release ${RELEASE}"
helm upgrade --install "${RELEASE}" "${CHART}" \
  --kube-context "${KUBECTL_CONTEXT}" \
  --namespace default \
  --set "image.repository=${IMAGE_REPOSITORY}" \
  --set "image.tag=${IMAGE_TAG}" \
  --set image.pullPolicy=Never \
  --set "responsesApiStore.endpoint=http://${STORE_RELEASE}:50051" \
  --wait \
  --timeout "${HELM_TIMEOUT}"

log "waiting for background worker startup log marker"
for attempt in $(seq 1 90); do
  logs="$(kubectl_ctx logs -l "app.kubernetes.io/name=duihua-background-worker,app.kubernetes.io/instance=${RELEASE}" --tail=200 2>/dev/null || true)"
  if [[ "${logs}" == *"${STARTUP_MARKER}"* ]]; then
    log "background worker finished startup"
    log "helm kind smoke test passed"
    exit 0
  fi
  if [[ "${attempt}" -eq 90 ]]; then
    dump_cluster_state
    echo "timed out waiting for background worker startup marker" >&2
    exit 1
  fi
  sleep 2
done