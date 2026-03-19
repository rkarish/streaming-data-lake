#!/usr/bin/env bash
# =============================================================================
# redeploy-sql-application.sh — Redeploy Flink SQL jobs in application mode
#
# Rebuilds the Docker image (picking up local SQL changes) and restarts the
# target FlinkDeployment(s). Each FlinkDeployment is deleted and recreated,
# so the job starts fresh without prior state.
#
# Usage: redeploy-sql-application.sh <ingestion|aggregation|funnel|all>
# =============================================================================

set -euo pipefail

JOB="${1:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
NAMESPACE="flink"
OVERLAY_DIR="$PROJECT_ROOT/k8s/flink/overlays/application-mode"

case "$JOB" in
  ingestion|aggregation|funnel|all) ;;
  *)
    cat <<'USAGE'
Usage: redeploy-sql-application.sh <ingestion|aggregation|funnel|all>

Redeploy Flink SQL jobs in application mode by rebuilding the Docker image
and restarting the FlinkDeployment(s). SQL changes are picked up from the
local workspace.

Jobs:
  ingestion   — Raw Kafka-to-Iceberg inserts  (insert_jobs.sql)
  aggregation — Windowed metrics              (aggregation_jobs.sql)
  funnel      — Funnel analytics              (funnel_jobs.sql)
  all         — All of the above

NOTE: This only works with application mode (FLINK_MODE=application).
      Session mode can hot-deploy SQL without an image rebuild; use
      redeploy-sql.sh instead.
USAGE
    exit 1
    ;;
esac

if [[ "$JOB" == "all" ]]; then
  JOBS=(ingestion aggregation funnel)
else
  JOBS=("$JOB")
fi

# ---------------------------------------------------------------------------
# Step 1: Rebuild the Flink Docker image
# ---------------------------------------------------------------------------
echo "==> Rebuilding Flink Docker image..."
docker build -t adtech-flink:latest "$PROJECT_ROOT/streaming/flink/"

# Docker Desktop's Kind-based K8s backend runs the node as a Docker container
# with its own containerd. Images must be explicitly imported into it.
if docker ps --filter name=desktop-control-plane --filter status=running --format '{{.Names}}' | grep -q desktop-control-plane; then
  echo "==> Loading image into Kubernetes node's containerd..."
  docker save adtech-flink:latest | docker exec -i desktop-control-plane ctr --namespace k8s.io images import -
fi

# ---------------------------------------------------------------------------
# Step 2: Delete and recreate FlinkDeployment(s)
# ---------------------------------------------------------------------------
for j in "${JOBS[@]}"; do
  DEPLOYMENT="flink-${j}"

  echo "==> Redeploying '$DEPLOYMENT'..."

  echo "    Deleting FlinkDeployment..."
  kubectl -n "$NAMESPACE" delete flinkdeployment "$DEPLOYMENT" --ignore-not-found --timeout=60s

  echo "    Waiting for pods to terminate..."
  kubectl -n "$NAMESPACE" wait --for=delete pod -l "app=$DEPLOYMENT" --timeout=120s 2>/dev/null || true

  echo "    Applying FlinkDeployment..."
  kubectl apply -f "$OVERLAY_DIR/flink-${j}.yaml"
done

echo ""
echo "==> Waiting for pods to be ready..."
for j in "${JOBS[@]}"; do
  kubectl wait --for=condition=Ready pod -l "app=flink-${j}" -n "$NAMESPACE" --timeout=180s 2>/dev/null || true
done

# ---------------------------------------------------------------------------
# Step 3: Restart port-forwards for redeployed jobs
# ---------------------------------------------------------------------------
get_local_port() {
  case "$1" in
    ingestion)   echo 8081 ;;
    aggregation) echo 8082 ;;
    funnel)      echo 8083 ;;
  esac
}

echo "==> Restarting port-forwards..."
for j in "${JOBS[@]}"; do
  local_port=$(get_local_port "$j")
  pkill -f "kubectl port-forward svc/flink-${j}-rest" 2>/dev/null || true
  kubectl port-forward "svc/flink-${j}-rest" -n "$NAMESPACE" "${local_port}:8081" &>/dev/null &
done

echo ""
echo "==> Done. Redeployed: ${JOBS[*]}"
echo ""
echo "Port-forwards running in background:"
for j in "${JOBS[@]}"; do
  label="$(tr '[:lower:]' '[:upper:]' <<< "${j:0:1}")${j:1}"
  echo "    - Flink ${label}:  http://localhost:$(get_local_port "$j")"
done
echo ""
echo "Press Ctrl+C to stop port-forwards."
wait
