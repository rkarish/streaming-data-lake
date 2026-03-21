#!/usr/bin/env bash
# Apply Trino views from views.sql. Each CREATE OR REPLACE VIEW statement
# is executed individually so failures are isolated.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VIEWS_FILE="$SCRIPT_DIR/views.sql"

if [ ! -f "$VIEWS_FILE" ]; then
  echo "ERROR: $VIEWS_FILE not found."
  exit 1
fi

# Split on semicolons, skip empty/comment-only statements
view_count=0
errors=0
current_stmt=""

while IFS= read -r line; do
  # Skip comment-only lines for display but include them in the statement
  current_stmt+="$line"$'\n'

  # If line ends with semicolon, execute the accumulated statement
  if [[ "$line" =~ \;[[:space:]]*$ ]]; then
    # Extract view name for logging
    view_name=$(echo "$current_stmt" | grep -oiE 'VIEW [a-z0-9_.]+' | head -1 | awk '{print $2}')
    if [ -n "$view_name" ]; then
      if docker exec trino trino --execute "$current_stmt" 2>/dev/null; then
        echo "    OK    $view_name"
        view_count=$((view_count + 1))
      else
        echo "    FAIL  $view_name"
        errors=$((errors + 1))
      fi
    fi
    current_stmt=""
  fi
done < "$VIEWS_FILE"

echo ""
echo "    $view_count views created, $errors errors."
exit $((errors > 0 ? 1 : 0))
