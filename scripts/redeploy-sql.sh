#!/usr/bin/env bash
# =============================================================================
# redeploy-sql.sh — Hot-redeploy Flink SQL jobs without rebuilding the image
#
# Session mode only. Cancels the running Flink job for the specified group,
# then submits updated SQL from your local workspace via a temporary pod
# connected to the session cluster's REST endpoint.
#
# Usage: redeploy-sql.sh <ingestion|aggregation|funnel|all>
# =============================================================================

set -euo pipefail

JOB="${1:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_DIR="$SCRIPT_DIR/../streaming/flink/sql"
NAMESPACE="flink"

case "$JOB" in
  ingestion|aggregation|funnel|all) ;;
  *)
    cat <<'USAGE'
Usage: redeploy-sql.sh <ingestion|aggregation|funnel|all>

Hot-redeploy Flink SQL jobs in session mode without rebuilding the image.
Cancels the running job and resubmits updated SQL via a temporary pod.

Jobs:
  ingestion   — Raw Kafka-to-Iceberg inserts  (insert_jobs.sql)
  aggregation — Windowed metrics              (aggregation_jobs.sql)
  funnel      — Funnel analytics              (funnel_jobs.sql)
  all         — All of the above

NOTE: This only works with session mode (FLINK_MODE=session).
      Application mode bakes SQL into the image; changes require an image
      rebuild and operator-managed savepoint-upgrade cycle.
USAGE
    exit 1
    ;;
esac

# ---------------------------------------------------------------------------
# Resolve session cluster jobmanager pod
# ---------------------------------------------------------------------------
echo "==> Finding Flink session cluster..."
SESSION_POD=$(kubectl -n "$NAMESPACE" get pods \
  -l "app=flink-session,component=jobmanager" \
  --field-selector=status.phase=Running \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null) || true

if [[ -z "$SESSION_POD" ]]; then
  echo "ERROR: No running Flink session cluster found in namespace '$NAMESPACE'."
  echo "       This script only works with session mode (FLINK_MODE=session)."
  echo "       Start the system with: FLINK_MODE=session bash scripts/setup.sh"
  exit 1
fi
echo "    Pod: $SESSION_POD"

# ---------------------------------------------------------------------------
# Job → SQL file mapping
# ---------------------------------------------------------------------------
get_dml_file() {
  case "$1" in
    ingestion)   echo "insert_jobs.sql" ;;
    aggregation) echo "aggregation_jobs.sql" ;;
    funnel)      echo "funnel_jobs.sql" ;;
  esac
}

# Pipeline name set in each SQL file via `SET 'pipeline.name'`, used to match
# against `flink list -r` output for targeted cancellation.
get_marker() {
  case "$1" in
    ingestion)   echo "adtech-ingestion" ;;
    aggregation) echo "adtech-aggregation" ;;
    funnel)      echo "adtech-funnel" ;;
  esac
}

# ---------------------------------------------------------------------------
# Cancel a running Flink job by matching its name against a marker table
# ---------------------------------------------------------------------------
cancel_job() {
  local name="$1"
  local marker
  marker=$(get_marker "$name")

  echo "==> Cancelling '$name' job (marker: $marker)..."

  local job_list
  job_list=$(kubectl -n "$NAMESPACE" exec "$SESSION_POD" -- \
    /opt/flink/bin/flink list -r 2>/dev/null) || true

  local job_id
  job_id=$(echo "$job_list" | grep "$marker" | grep -oE '[a-f0-9]{32}' | head -1) || true

  if [[ -n "$job_id" ]]; then
    if kubectl -n "$NAMESPACE" exec "$SESSION_POD" -- \
      wget -qO /dev/null --method=PATCH "http://localhost:8081/jobs/$job_id?mode=cancel" 2>/dev/null; then
      echo "    Cancelled $job_id"
    else
      echo "    WARNING: Failed to cancel job $job_id"
    fi
  else
    echo "    No running job found for '$name' — skipping cancel."
  fi
}

# ---------------------------------------------------------------------------
# Submit SQL via a temporary pod (same pattern as the K8s session-mode Jobs).
# Local SQL files are piped in via stdin; the pod connects to the session
# cluster's REST endpoint, submits the SQL, and is cleaned up automatically.
# ---------------------------------------------------------------------------
submit_job() {
  local name="$1"
  local dml_file
  dml_file=$(get_dml_file "$name")

  echo "==> Deploying '$name' ($dml_file)..."

  cat "$SQL_DIR/create_tables.sql" "$SQL_DIR/$dml_file" | \
    kubectl -n "$NAMESPACE" run "redeploy-${name}" --rm -i --restart=Never \
      --image=adtech-flink:latest \
      --image-pull-policy=Never \
      --env=AWS_ACCESS_KEY_ID=admin \
      --env=AWS_SECRET_ACCESS_KEY=password \
      --env=AWS_REGION=us-east-1 \
      -- bash -c "cat > /tmp/combined.sql && \
                  /opt/flink/bin/sql-client.sh -f /tmp/combined.sql \
                  -Drest.address=flink-session-rest.flink -Drest.port=8081" 2>&1 | sed 's/^/    /'

  echo "    '$name' submitted."
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if [[ "$JOB" == "all" ]]; then
  JOBS=(ingestion aggregation funnel)
else
  JOBS=("$JOB")
fi

for j in "${JOBS[@]}"; do
  cancel_job "$j"
done

echo ""

for j in "${JOBS[@]}"; do
  submit_job "$j"
  echo ""
done

echo "==> Done. Verify with:"
echo "    kubectl -n $NAMESPACE exec $SESSION_POD -- /opt/flink/bin/flink list -r"
