#!/usr/bin/env bash
# deploy/deploy.sh
#
# Builds the Rapid-QC-MS Docker image, pushes it to ECR, and forces a new ECS
# Fargate deployment. Run from the repository root.
#
# Usage:
#   AWS_PROFILE=sci-data-prod ./deploy/deploy.sh
#
# All variables below can be overridden via environment or a .env file.
# The defaults match the Terraform-managed resource names.
#
# First-time setup:
#   1. Run `terraform apply` in sci-data-infra terraform/envs/private-prod/rapid-qc-ms/
#   2. Set AWS_PROFILE (or ensure your default profile targets sci-data-prod).
#   3. Run this script.

set -euo pipefail

# ── config ────────────────────────────────────────────────────────────────────
AWS_REGION="${AWS_REGION:-us-west-2}"
ECS_CLUSTER="${ECS_CLUSTER:-rapid-qc-ms}"
ECS_SERVICE="${ECS_SERVICE:-rapid-qc-ms}"
IMAGE_TAG="${IMAGE_TAG:-$(git rev-parse --short HEAD)}"

AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
ECR_REPO="${ECR_REPO:-${ECR_REGISTRY}/rapidqcms}"

echo "Account : ${AWS_ACCOUNT_ID}"
echo "Image   : ${ECR_REPO}:${IMAGE_TAG}"
echo "Service : ${ECS_CLUSTER}/${ECS_SERVICE}"
echo ""

# ── build ─────────────────────────────────────────────────────────────────────
echo "→ Building image (linux/amd64)..."
docker build \
  --platform linux/amd64 \
  --tag "${ECR_REPO}:${IMAGE_TAG}" \
  --tag "${ECR_REPO}:latest" \
  .

# ── push ──────────────────────────────────────────────────────────────────────
echo "→ Authenticating with ECR..."
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$ECR_REGISTRY"

echo "→ Pushing ${ECR_REPO}:${IMAGE_TAG}..."
docker push "${ECR_REPO}:${IMAGE_TAG}"
docker push "${ECR_REPO}:latest"

# ── deploy ────────────────────────────────────────────────────────────────────
echo "→ Forcing new ECS deployment..."
aws ecs update-service \
  --region     "$AWS_REGION" \
  --cluster    "$ECS_CLUSTER" \
  --service    "$ECS_SERVICE" \
  --force-new-deployment \
  --query "service.deployments[0].{status:status,desired:desiredCount,running:runningCount}" \
  --output table

echo "→ Waiting for service to stabilize (~2 min)..."
aws ecs wait services-stable \
  --region   "$AWS_REGION" \
  --cluster  "$ECS_CLUSTER" \
  --services "$ECS_SERVICE"

echo ""
echo "✓ Deployed ${IMAGE_TAG} → ${ECS_CLUSTER}/${ECS_SERVICE}"
