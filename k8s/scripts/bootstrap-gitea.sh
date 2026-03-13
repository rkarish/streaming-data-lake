#!/bin/bash
set -euo pipefail

# Bootstrap Gitea with the adtech user and k8s-manifests repository.
# Idempotent: safe to re-run.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
K8S_DIR="$PROJECT_ROOT/k8s"

GITEA_URL="http://localhost:3000"
GITEA_USER="adtech"
GITEA_PASS="adtech123"
REPO_NAME="k8s-manifests"
AUTH_HEADER="Authorization: Basic $(printf '%s:%s' "$GITEA_USER" "$GITEA_PASS" | base64)"

echo "  [gitea] Waiting for Gitea to be healthy..."
for i in $(seq 1 30); do
  if curl -sf "$GITEA_URL/api/v1/settings/api" >/dev/null 2>&1; then
    echo "  [gitea] Gitea is ready"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "  [gitea] ERROR: Gitea did not become healthy after 30 attempts"
    exit 1
  fi
  sleep 2
done

# Create admin user (idempotent -- skip if already exists)
echo "  [gitea] Creating user '$GITEA_USER'..."
if docker exec --user git gitea gitea admin user create \
  --username "$GITEA_USER" \
  --password "$GITEA_PASS" \
  --email "${GITEA_USER}@local.dev" \
  --admin \
  --must-change-password=false 2>/dev/null; then
  echo "  [gitea] User created"
else
  echo "  [gitea] User already exists, skipping"
fi

# Create repository (idempotent -- skip if already exists)
echo "  [gitea] Creating repository '$REPO_NAME'..."
REPO_STATUS=$(curl -s -o /dev/null -w '%{http_code}' \
  -H "$AUTH_HEADER" \
  "$GITEA_URL/api/v1/repos/$GITEA_USER/$REPO_NAME")

if [ "$REPO_STATUS" = "200" ]; then
  echo "  [gitea] Repository already exists, skipping creation"
else
  curl -sf -X POST "$GITEA_URL/api/v1/user/repos" \
    -H "$AUTH_HEADER" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"$REPO_NAME\",\"auto_init\":false,\"default_branch\":\"main\"}"
  echo ""
  echo "  [gitea] Repository created"
fi

# Push k8s/flink/ manifests into the Gitea repo
echo "  [gitea] Pushing Flink manifests to Gitea..."
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

cd "$TMPDIR"
git init -b main
git config user.email "adtech@local.dev"
git config user.name "adtech"

# Copy the flink directory structure into the temp repo
cp -r "$K8S_DIR/flink" "$TMPDIR/flink"

git add -A
git commit -m "Flink Kubernetes manifests" --allow-empty

GIT_AUTH="$(printf '%s:%s' "$GITEA_USER" "$GITEA_PASS" | base64)"
git -c "http.extraHeader=Authorization: Basic $GIT_AUTH" \
  push --force "$GITEA_URL/$GITEA_USER/$REPO_NAME.git" main

echo "  [gitea] Manifests pushed to $GITEA_URL/$GITEA_USER/$REPO_NAME.git"
