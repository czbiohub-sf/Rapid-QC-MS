#!/bin/bash
# Build all container images for the MS-DIAL annotation pipeline.
#
# Supports two modes:
#   --docker      Build Docker images (default)
#   --apptainer   Build Apptainer .sif files from the Dockerfiles
#
# Each image can also be built individually by cd'ing into its directory.
#
# Prerequisites:
#   - MS-DIAL binary: see containers/msdial/README
#   - MIST-CF repo + SIRIUS: see containers/mist-cf/README
#   - msknit source: see containers/msknit/README

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MODE="${1:---docker}"
REGISTRY="${REGISTRY:-ghcr.io/your-org/rapid-qc-ms}"
TAG="${TAG:-latest}"

IMAGES=(msdial pipeline-tools msknit mist-cf)

build_docker() {
    local name=$1
    local dir="$SCRIPT_DIR/$name"
    local tag="${REGISTRY}/${name}:${TAG}"

    echo "=== Building Docker image: $tag ==="
    docker build -t "$tag" "$dir"
    echo "  Done: $tag"
}

build_apptainer() {
    local name=$1
    local dir="$SCRIPT_DIR/$name"
    local sif="$SCRIPT_DIR/${name}.sif"

    echo "=== Building Apptainer image: $sif ==="
    # Build .sif directly from Dockerfile
    apptainer build "$sif" "docker-daemon://${REGISTRY}/${name}:${TAG}"
    echo "  Done: $sif"
}

case "$MODE" in
    --docker)
        for img in "${IMAGES[@]}"; do
            build_docker "$img"
        done
        echo ""
        echo "All Docker images built. Push with:"
        echo "  docker push ${REGISTRY}/<image>:${TAG}"
        ;;
    --apptainer)
        echo "Building Docker images first (required for Apptainer conversion)..."
        for img in "${IMAGES[@]}"; do
            build_docker "$img"
        done
        echo ""
        echo "Converting to Apptainer .sif files..."
        for img in "${IMAGES[@]}"; do
            build_apptainer "$img"
        done
        echo ""
        echo "All .sif files built in $SCRIPT_DIR/"
        ;;
    --apptainer-direct)
        echo "Building .sif files directly from Dockerfiles (no Docker daemon)..."
        for img in "${IMAGES[@]}"; do
            echo "=== Building: ${img}.sif ==="
            cd "$SCRIPT_DIR/$img"
            apptainer build "$SCRIPT_DIR/${img}.sif" .
            cd "$SCRIPT_DIR"
        done
        ;;
    *)
        echo "Usage: $0 [--docker|--apptainer|--apptainer-direct]"
        exit 1
        ;;
esac
