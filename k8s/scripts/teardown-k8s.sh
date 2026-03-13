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

# Argo CD must be removed first — its selfHeal will recreate Flink resources during teardown
echo "[2/6] Deleting Argo CD..."
kubectl delete applications.argoproj.io --all -n argocd --ignore-not-found 2>/dev/null || true
kubectl delete namespace argocd --ignore-not-found --timeout=30s 2>/dev/null || true
echo ""

# Uninstall operator before deleting CRs so it stops re-adding finalizers
echo "[3/6] Uninstalling Flink Kubernetes Operator..."
helm uninstall flink-kubernetes-operator -n flink-operator 2>/dev/null || true
kubectl delete namespace flink-operator --ignore-not-found --timeout=30s 2>/dev/null || true
echo ""

# Now safe to delete Flink CRs — no operator or Argo CD to interfere
echo "[4/6] Deleting Flink custom resources..."
for cr in $(kubectl get flinkdeployments,flinksessionjobs -n flink -o name 2>/dev/null); do
  kubectl patch "$cr" -n flink -p '{"metadata":{"finalizers":null}}' --type=merge 2>/dev/null || true
  kubectl delete "$cr" -n flink --timeout=30s 2>/dev/null || true
done
echo ""

echo "[5/6] Deleting Flink namespace..."
kubectl delete namespace flink --ignore-not-found --timeout=30s 2>/dev/null || true
echo ""

echo "[6/6] Removing cert-manager..."
kubectl delete -f "https://github.com/cert-manager/cert-manager/releases/download/v1.17.2/cert-manager.yaml" --ignore-not-found 2>/dev/null || true
echo ""

echo "=== Teardown complete ==="
