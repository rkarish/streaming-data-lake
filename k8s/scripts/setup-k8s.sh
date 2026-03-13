#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
K8S_DIR="$PROJECT_ROOT/k8s"

CERT_MANAGER_VERSION="v1.17.2"
FLINK_OPERATOR_VERSION="1.11.0"

# Parse --mode argument (application or session, default: application)
FLINK_MODE="${FLINK_MODE:-application}"
while [[ $# -gt 0 ]]; do
  case $1 in
    --mode) FLINK_MODE="$2"; shift 2 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

if [[ "$FLINK_MODE" != "application" && "$FLINK_MODE" != "session" ]]; then
  echo "ERROR: Invalid mode '$FLINK_MODE'. Must be 'application' or 'session'."
  exit 1
fi

echo "=== Flink on Kubernetes Setup (${FLINK_MODE} mode) ==="
echo ""

# Verify kubectl context is docker-desktop
CONTEXT=$(kubectl config current-context 2>/dev/null || true)
if [ "$CONTEXT" != "docker-desktop" ]; then
  echo "ERROR: kubectl context is '$CONTEXT', expected 'docker-desktop'"
  echo "Run: kubectl config use-context docker-desktop"
  exit 1
fi

echo "[1/6] Installing cert-manager $CERT_MANAGER_VERSION..."
# Clean up stale webhook configs/certs from previous runs to avoid x509 errors
kubectl delete validatingwebhookconfiguration cert-manager-webhook --ignore-not-found 2>/dev/null
kubectl delete mutatingwebhookconfiguration cert-manager-webhook --ignore-not-found 2>/dev/null
kubectl delete secret cert-manager-webhook-ca -n cert-manager --ignore-not-found 2>/dev/null
kubectl apply -f "https://github.com/cert-manager/cert-manager/releases/download/$CERT_MANAGER_VERSION/cert-manager.yaml"
echo "  Waiting for cert-manager webhook to become ready (up to 90s)..."
for i in $(seq 1 30); do
  if kubectl apply --dry-run=server -f - <<'EOF' 2>/dev/null; then
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: webhook-probe
  namespace: cert-manager
spec:
  selfSigned: {}
EOF
    kubectl delete issuer webhook-probe -n cert-manager --ignore-not-found 2>/dev/null
    break
  fi
  sleep 5
done
echo "  cert-manager is ready"
echo ""

echo "[2/6] Building Flink Docker image..."
docker build -t adtech-flink:latest "$PROJECT_ROOT/streaming/flink/"
echo "  Extracting SQL Runner JAR for operator access..."
CONTAINER_ID=$(docker create adtech-flink:latest)
mkdir -p /tmp/flink/usrlib
docker cp "$CONTAINER_ID:/opt/flink/usrlib/flink-sql-runner.jar" /tmp/flink/usrlib/flink-sql-runner.jar
docker rm "$CONTAINER_ID" >/dev/null
echo ""

echo "[3/6] Creating flink namespace and installing Flink Kubernetes Operator $FLINK_OPERATOR_VERSION..."
# Wait for flink namespace to finish terminating from a previous run
if kubectl get namespace flink -o jsonpath='{.status.phase}' 2>/dev/null | grep -q Terminating; then
  echo "  Waiting for flink namespace to finish terminating..."
  kubectl wait --for=delete namespace/flink --timeout=120s 2>/dev/null || true
fi
kubectl create namespace flink 2>/dev/null || true
helm repo add flink-operator "https://archive.apache.org/dist/flink/flink-kubernetes-operator-$FLINK_OPERATOR_VERSION/" --force-update
helm upgrade --install flink-kubernetes-operator flink-operator/flink-kubernetes-operator \
  --namespace flink-operator --create-namespace \
  -f "$K8S_DIR/flink/operator/values.yaml"
echo "  Waiting for Flink Operator to be ready..."
kubectl wait --for=condition=Available deployment/flink-kubernetes-operator -n flink-operator --timeout=120s
echo "  Flink Operator is ready"
echo ""

echo "[4/6] Deploying Flink (${FLINK_MODE} mode)..."
kubectl apply -f "$K8S_DIR/flink/base/external-endpoints.yaml"
kubectl apply -k "$K8S_DIR/flink/overlays/${FLINK_MODE}-mode/"
echo ""

echo "[5/6] Bootstrapping Gitea repository..."
docker compose up -d gitea
bash "$SCRIPT_DIR/bootstrap-gitea.sh"
echo ""

echo "[6/6] Installing Argo CD..."
if kubectl get namespace argocd &>/dev/null; then
  echo "  argocd namespace already exists, skipping install"
else
  kubectl create namespace argocd
  kubectl apply -n argocd --server-side -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
fi
echo "  Waiting for Argo CD to be ready..."
kubectl wait --for=condition=Available deployment/argocd-server -n argocd --timeout=180s
echo "  Setting Argo CD admin password..."
ARGOCD_PASSWORD_HASH=$(htpasswd -nbBC 10 "" "password" | tr -d ':\n' | sed 's/$2y/$2a/')
kubectl -n argocd patch secret argocd-secret -p "{\"stringData\":{\"admin.password\":\"${ARGOCD_PASSWORD_HASH}\",\"admin.passwordMtime\":\"$(date -u +%FT%TZ)\"}}"
kubectl -n argocd rollout restart deployment argocd-server
kubectl wait --for=condition=Available deployment/argocd-server -n argocd --timeout=120s
echo "  Argo CD is ready (admin / password)"
echo ""
echo "  Applying Argo CD Application CRD..."
ARGOCD_APP=$(mktemp)
sed "s|flink/overlays/application-mode|flink/overlays/${FLINK_MODE}-mode|" "$K8S_DIR/argocd/application.yaml" > "$ARGOCD_APP"
kubectl apply -f "$ARGOCD_APP"
rm -f "$ARGOCD_APP"
echo ""

echo "Waiting for Flink pods to be ready..."
if [ "$FLINK_MODE" = "application" ]; then
  kubectl wait --for=condition=Ready pod -l app=flink-ingestion -n flink --timeout=180s 2>/dev/null || true
  kubectl wait --for=condition=Ready pod -l app=flink-aggregation -n flink --timeout=180s 2>/dev/null || true
  kubectl wait --for=condition=Ready pod -l app=flink-funnel -n flink --timeout=180s 2>/dev/null || true
else
  kubectl wait --for=condition=Ready pod -l app=flink-session -n flink --timeout=180s 2>/dev/null || true
fi

echo "  K8s setup complete (${FLINK_MODE} mode)"
echo ""
