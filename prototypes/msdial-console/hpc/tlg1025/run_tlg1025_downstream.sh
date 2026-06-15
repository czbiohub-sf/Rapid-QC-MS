#!/bin/bash
#SBATCH --job-name=nf_tlg1025_downstream
#SBATCH --output=/hpc/projects/mass_spec_chi/Team/Tony/msdial-tlg1025/logs/nf_tlg1025_downstream_%j.out
#SBATCH --error=/hpc/projects/mass_spec_chi/Team/Tony/msdial-tlg1025/logs/nf_tlg1025_downstream_%j.err
#SBATCH -p cpu
#SBATCH --time=12:00:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=2

# Stage 2: Downstream analysis on cached MS-DIAL results.
# Resumes from a previous detect run — MS-DIAL steps are skipped.
#
# Toggle steps with environment variables before submitting:
#   MSKNIT=true sbatch run_tlg1025_downstream.sh        # networking only
#   ANNOTATE=true sbatch run_tlg1025_downstream.sh      # annotation only
#   DIFFMS=true ANNOTATE=true sbatch ...                 # annotation + DiffMS
#   (no vars) sbatch run_tlg1025_downstream.sh           # all steps

set -euo pipefail

module load nextflow/24.10.5

PIPELINE_DIR="/hpc/mydata/anthony.goering/repos/Rapid-QC-MS/prototypes/msdial-console"
CONFIG_DIR="$PIPELINE_DIR/config/tlg1025"
OUTDIR="/hpc/projects/mass_spec_chi/Team/Tony/msdial-tlg1025/v6_reflib_diffms"
LIBS_DIR="/hpc/projects/mass_spec_chi/Team/Tony/metabolomics_libraries"
MODELS_DIR="/hpc/mydata/anthony.goering/models/diffms"

# Defaults: all on unless selectively overridden
RUN_MSKNIT="${MSKNIT:-true}"
RUN_PYCUTTER="${PYCUTTER:-true}"
RUN_ANNOTATE="${ANNOTATE:-true}"
DIFFMS_CKPT="${DIFFMS_CKPT:-$MODELS_DIR/checkpoints/diffms_msg.ckpt}"

# If DIFFMS=false, clear the checkpoint so the pipeline skips it
if [[ "${DIFFMS:-true}" == "false" ]]; then
    DIFFMS_CKPT=""
fi

echo "============================================"
echo "Nextflow MS-DIAL TLG1025 — downstream"
echo "============================================"
echo "Node:      $(hostname)"
echo "Started:   $(date)"
echo "Job ID:    ${SLURM_JOB_ID:-local}"
echo "Output:    $OUTDIR"
echo "MSKnit:    $RUN_MSKNIT"
echo "PyCutter:  $RUN_PYCUTTER"
echo "Annotate:  $RUN_ANNOTATE"
echo "DiffMS:    ${DIFFMS_CKPT:-disabled}"
echo "============================================"

mkdir -p "$OUTDIR" /hpc/projects/mass_spec_chi/Team/Tony/msdial-tlg1025/logs

cd "$PIPELINE_DIR"

DOWNSTREAM_ARGS=()

if [[ "$RUN_MSKNIT" == "true" ]]; then
    DOWNSTREAM_ARGS+=(--run_msknit true)
fi

if [[ "$RUN_PYCUTTER" == "true" ]]; then
    DOWNSTREAM_ARGS+=(--run_pycutter true)
fi

if [[ "$RUN_ANNOTATE" == "true" ]]; then
    DOWNSTREAM_ARGS+=(
        --run_annotation true
        --reference_library "$LIBS_DIR/internal/Jan2026/combined_HILIC_Jan2026.msp"
        --predicted_library "$LIBS_DIR/hmdb/hmdb_predicted.msp"
    )
fi

if [[ -n "$DIFFMS_CKPT" ]]; then
    DOWNSTREAM_ARGS+=(--diffms_checkpoint "$DIFFMS_CKPT")
fi

nextflow run main.nf \
    --manifest_pos "$PIPELINE_DIR/hpc/tlg1025/input_manifest_pos.csv" \
    --manifest_neg "$PIPELINE_DIR/hpc/tlg1025/input_manifest_neg.csv" \
    --params_pos   "$CONFIG_DIR/lcms_pos_v5_reflib.txt" \
    --params_neg   "$CONFIG_DIR/lcms_neg_v5_reflib.txt" \
    --outdir       "$OUTDIR" \
    "${DOWNSTREAM_ARGS[@]}" \
    -resume \
    -profile standard

echo ""
echo "============================================"
echo "Downstream complete at $(date)"
echo "============================================"
