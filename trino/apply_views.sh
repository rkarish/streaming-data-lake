#!/usr/bin/env bash
# Apply Trino views from individual SQL files in trino/sql/.
# Each file contains a single CREATE OR REPLACE VIEW statement.
#
# NOTE: The canonical execution path for this workflow is the Airflow
# "view_deployment" DAG. This script is retained as a manual fallback.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_DIR="$SCRIPT_DIR/sql"

if [ ! -d "$SQL_DIR" ]; then
  echo "ERROR: $SQL_DIR not found."
  exit 1
fi

view_count=0
errors=0

for sql_file in "$SQL_DIR"/*.sql; do
  [ -f "$sql_file" ] || continue
  filename=$(basename "$sql_file" .sql)

  if docker exec trino trino --execute "$(cat "$sql_file")" 2>/dev/null; then
    echo "    OK    $filename"
    view_count=$((view_count + 1))
  else
    echo "    FAIL  $filename"
    errors=$((errors + 1))
  fi
done

echo ""
echo "    $view_count views created, $errors errors."
exit $((errors > 0 ? 1 : 0))
