#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== Flink on Kubernetes Teardown ==="
echo ""

# Verify kubectl context
CONTEXT=$(kubectl config current-context 2>/dev/null || true)
if [ "$CONTEXT" != "docker-desktop" ]; then
  echo "ERROR: kubectl context is '$CONTEXT', expected 'docker-desktop'"
  exit 1
fi

echo "[1/6] Killing port-forwards..."
pkill -f "kubectl port-forward.*-n flink" 2>/dev/null || true
pkill -f "kubectl port-forward.*-n argocd" 2>/dev/null || true
echo ""

# Delete Flink CRs first and wait for the operator to finalize them
# before removing the RBAC/namespace that the operator needs.
echo "[2/6] Deleting Flink custom resources..."
kubectl delete flinkdeployments,flinksessionjobs --all -n flink --timeout=120s 2>/dev/null || true
echo ""

echo "[3/6] Deleting Argo CD..."
kubectl delete applications.argoproj.io --all -n argocd --ignore-not-found 2>/dev/null || true
kubectl delete namespace argocd --ignore-not-found 2>/dev/null || true
echo ""

echo "[4/6] Deleting Flink namespace and RBAC..."
kubectl delete -k "$K8S_DIR/flink/base/" --ignore-not-found 2>/dev/null || true
echo ""

echo "[5/6] Uninstalling Flink Kubernetes Operator..."
helm uninstall flink-kubernetes-operator -n flink-operator 2>/dev/null || true
kubectl delete namespace flink-operator --ignore-not-found 2>/dev/null || true
echo ""

echo "[6/6] Removing cert-manager..."
kubectl delete -f "https://github.com/cert-manager/cert-manager/releases/download/v1.17.2/cert-manager.yaml" --ignore-not-found 2>/dev/null || true
echo ""

echo "=== Teardown complete ==="
