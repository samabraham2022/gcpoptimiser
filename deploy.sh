#!/bin/bash
set -e

# ==========================================
# CONFIGURATION
# ==========================================
PROJECT_ID=$(gcloud config get-value project)
ZONE="us-central1-a"
REGION="${ZONE%-*}"
CLUSTER_NAME="gke-ml-cluster"
REPO_NAME="vertex-sentinel-repo"
CUSTOM_DOMAIN="${CUSTOM_DOMAIN:-}"

UI_IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/optimizer-ui:latest"
ML_IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/optimizer-ml:latest"

echo "Using GCP Project: ${PROJECT_ID}"
echo "Target Zone:       ${ZONE}"
echo "Target Region:     ${REGION}"

echo "=== 1. Enabling GCP Service APIs ==="
gcloud services enable \
    container.googleapis.com \
    artifactregistry.googleapis.com \
    bigquery.googleapis.com

echo "=== 2. Provisioning GKE Cluster ==="
if ! gcloud container clusters describe "${CLUSTER_NAME}" --zone "${ZONE}" >/dev/null 2>&1; then
  gcloud container clusters create "${CLUSTER_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${ZONE}" \
    --num-nodes=4 \
    --machine-type="e2-medium" \
    --disk-type="pd-standard" \
    --disk-size="30" \
    --release-channel="regular" \
    --no-enable-autoupgrade \
    --no-enable-autorepair \
    --enable-ip-alias
else
    echo "Cluster exists. Skipping creation."
fi

echo "=== 2a. Ensuring firewall rules for ingress ==="
if ! gcloud compute firewall-rules describe allow-gke-public-ui >/dev/null 2>&1; then
    gcloud compute firewall-rules create allow-gke-public-ui \
        --network=default \
        --direction=INGRESS \
        --priority=1000 \
        --action=ALLOW \
        --rules=tcp:80,tcp:443,tcp:3000 \
        --source-ranges=0.0.0.0/0
fi

echo "=== 2b. Reserving static external IP ==="
STATIC_IP_NAME="optimizer-ui-ip"
if ! gcloud compute addresses describe "${STATIC_IP_NAME}" --project="${PROJECT_ID}" --region="${REGION}" >/dev/null 2>&1; then
    gcloud compute addresses create "${STATIC_IP_NAME}" \
        --project="${PROJECT_ID}" \
        --region="${REGION}"
fi
STATIC_IP=$(gcloud compute addresses describe "${STATIC_IP_NAME}" --project="${PROJECT_ID}" --region="${REGION}" --format='value(address)')

if [ -n "${CUSTOM_DOMAIN}" ]; then
    HTTPS_HOST="${CUSTOM_DOMAIN}"
elif [ -n "${STATIC_IP}" ]; then
    HTTPS_HOST="${STATIC_IP}.nip.io"
else
    HTTPS_HOST="optimizer.local"
fi

gcloud container clusters get-credentials "${CLUSTER_NAME}" --zone "${ZONE}"

echo "=== 3. Setting up Artifact Registry ==="
if ! gcloud artifacts repositories describe "${REPO_NAME}" --location="${REGION}" >/dev/null 2>&1; then
    gcloud artifacts repositories create "${REPO_NAME}" \
        --repository-format=docker \
        --location="${REGION}"
fi
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

echo "=== 4. Creating BigQuery service account and secret ==="
BQ_SA_NAME="bq-ml-sa"
BQ_SA_EMAIL="${BQ_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
BQ_KEY_FILE="/tmp/${BQ_SA_NAME}.json"
K8S_BQ_SECRET_NAME="bq-sa-key"

if ! gcloud iam service-accounts describe "${BQ_SA_EMAIL}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
    gcloud iam service-accounts create "${BQ_SA_NAME}" \
        --project="${PROJECT_ID}" \
        --display-name="BigQuery ML Service Account"
fi

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${BQ_SA_EMAIL}" \
    --role="roles/bigquery.admin" \
    --quiet

gcloud iam service-accounts keys create "${BQ_KEY_FILE}" \
    --iam-account="${BQ_SA_EMAIL}" \
    --project="${PROJECT_ID}" \
    --quiet

if ! kubectl get secret "${K8S_BQ_SECRET_NAME}" >/dev/null 2>&1; then
    kubectl create secret generic "${K8S_BQ_SECRET_NAME}" \
        --from-file=key.json="${BQ_KEY_FILE}"
else
    echo "Secret '${K8S_BQ_SECRET_NAME}' already exists. Skipping creation."
fi

echo "=== 5. Building and Pushing UI Container ==="
cd ui
docker buildx build --platform linux/amd64 -t "${UI_IMAGE_URI}" --push .
cd ..

kubectl delete secret artifact-registry-secret --ignore-not-found=true
kubectl create secret docker-registry artifact-registry-secret \
    --docker-server="${REGION}-docker.pkg.dev" \
    --docker-username="oauth2accesstoken" \
    --docker-password="$(gcloud auth print-access-token)" \
    --docker-email="test@example.com"

echo "=== 6. Deploying Microservices, UI & Ingress ==="
sed "s|__UI_IMAGE_URI__|${UI_IMAGE_URI}|g" k8s/02-spring-boot-app.yaml > /tmp/02-spring-boot-app-rendered.yaml

kubectl apply -f k8s/01-kafka-broker.yaml
kubectl apply -f /tmp/02-spring-boot-app-rendered.yaml
kubectl rollout restart deployment/order-processor deployment/payment-gateway deployment/notification-worker || true

sed -e "s|__HOSTNAME__|${HTTPS_HOST}|g" \
    -e "s|__STATIC_IP_NAME__|${STATIC_IP_NAME}|g" \
    -e "s|__UI_IMAGE_URI__|${UI_IMAGE_URI}|g" \
    k8s/03-ui-and-bff.yaml > /tmp/03-ui-and-bff-rendered.yaml

kubectl apply -f /tmp/03-ui-and-bff-rendered.yaml
kubectl rollout status deployment/optimizer-ui --timeout=180s

kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.12.0/deploy/static/provider/cloud/deploy.yaml


kubectl wait --namespace ingress-nginx \
  --for=condition=ready pod \
  --selector=app.kubernetes.io/component=controller \
  --timeout=120s


echo "=== 6a. Deploying Cloudflare Quick Tunnel in dedicated namespace ==="
kubectl create namespace ingress-support --dry-run=client -o yaml | kubectl apply -f -
cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cloudflare-tunnel
  namespace: ingress-support
spec:
  replicas: 1
  selector:
    matchLabels:
      app: cloudflare-tunnel
  template:
    metadata:
      labels:
        app: cloudflare-tunnel
    spec:
      containers:
      - name: cloudflared
        image: cloudflare/cloudflared:latest
        args:
        - tunnel
        - --no-autoupdate
        - --url
        - http://optimizer-ui-service.default.svc.cluster.local:80
        resources:
          requests:
            cpu: 50m
            memory: 64Mi
          limits:
            cpu: 100m
            memory: 128Mi
EOF

echo "Waiting for Cloudflare Tunnel to initialize..."
kubectl -n ingress-support rollout status deployment/cloudflare-tunnel --timeout=60s

CLOUDFLARE_URL=""
for _ in {1..20}; do
    CLOUDFLARE_URL=$(kubectl -n ingress-support logs -l app=cloudflare-tunnel --tail=100 2>/dev/null | grep -o 'https://[-a-zA-Z0-9]*\.trycloudflare\.com' | head -n 1 || true)
    if [ -n "${CLOUDFLARE_URL}" ]; then
        break
    fi
    sleep 3
done

echo "=== 7. Running Dataset Generation & ML Training via GKE Job ==="
cd ml
docker buildx build --platform linux/amd64 -t "${ML_IMAGE_URI}" --push .
cd ..

kubectl delete job ml-training-job --ignore-not-found=true

cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ml-training-job
spec:
  backoffLimit: 0
  template:
    spec:
      imagePullSecrets:
      - name: artifact-registry-secret
      restartPolicy: Never
      containers:
      - name: ml-pipeline
        image: ${ML_IMAGE_URI}
        env:
        - name: PROJECT_ID
          value: "${PROJECT_ID}"
        - name: GOOGLE_APPLICATION_CREDENTIALS
          value: "/etc/gcp/key.json"
        volumeMounts:
        - name: sa-key-volume
          mountPath: "/etc/gcp"
          readOnly: true
      volumes:
      - name: sa-key-volume
        secret:
          secretName: bq-sa-key
EOF

echo "Waiting for BigQuery ML Training Job to initialize..."
sleep 5
kubectl logs -f job/ml-training-job || true 

echo "Waiting for Job completion..."
kubectl wait --for=condition=complete job/ml-training-job --timeout=300s

echo "=========================================================="
echo "DEPLOYMENT COMPLETE (GLOBAL SECURE TLS)"
if [ -n "${CLOUDFLARE_URL}" ]; then
    echo "Public Trusted UI URL: ${CLOUDFLARE_URL}"
else
    echo "Direct Ingress Host:    https://${HTTPS_HOST}"
    echo "Fetch Cloudflare URL:   kubectl logs -l app=cloudflare-tunnel | grep -o 'https://.*\.trycloudflare\.com'"
fi

INGRESS_IP=$(kubectl get svc -n ingress-nginx ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
echo "GKE Public Ingress IP: ${INGRESS_IP}"
echo "=========================================================="