#!/bin/bash
#SBATCH --job-name=msdial_tlg1025
#SBATCH --output=/hpc/projects/mass_spec_chi/Team/Tony/msdial-tlg1025/logs/msdial_tlg1025_%j.out
#SBATCH --error=/hpc/projects/mass_spec_chi/Team/Tony/msdial-tlg1025/logs/msdial_tlg1025_%j.err
#SBATCH -p cpu
#SBATCH --time=12:00:00
#SBATCH --mem=64G
#SBATCH --cpus-per-task=16

set -euo pipefail

# --- Paths ---
# NOTE: Use absolute paths — SLURM copies the script to a spool directory,
# so BASH_SOURCE / dirname tricks resolve to /var/spool/slurm/... at runtime.
MSDIAL_CONSOLE="/hpc/mydata/anthony.goering/opt/msdial4/MsdialConsoleApp"
REPO_DIR="/home/anthony.goering/repos/ms-pipelines/prototypes/msdial-console"
HPC_DIR="$REPO_DIR/hpc/tlg1025"
OUTPUT_ROOT="/hpc/projects/mass_spec_chi/Team/Tony/msdial-tlg1025"

MANIFEST_POS="$HPC_DIR/input_manifest_pos.csv"
MANIFEST_NEG="$HPC_DIR/input_manifest_neg.csv"
PARAMS_POS="$REPO_DIR/config/tlg1025/lcms_pos_v3_n100_qcoff.txt"
PARAMS_NEG="$REPO_DIR/config/tlg1025/lcms_neg_v3_n100_qcoff.txt"

OUT_POS="$OUTPUT_ROOT/pos"
OUT_NEG="$OUTPUT_ROOT/neg"

echo "============================================"
echo "MS-DIAL Console TLG1025 HPC Run"
echo "============================================"
echo "Node:      $(hostname)"
echo "Started:   $(date)"
echo "Job ID:    ${SLURM_JOB_ID:-local}"
echo "CPUs:      ${SLURM_CPUS_PER_TASK:-$(nproc)}"
echo "Memory:    ${SLURM_MEM_PER_NODE:-unknown} MB"
echo "Binary:    $MSDIAL_CONSOLE"
echo "Output:    $OUTPUT_ROOT"
echo "============================================"

# --- Create output directories ---
mkdir -p "$OUT_POS" "$OUT_NEG" "$OUTPUT_ROOT/logs"

# --- Positive mode ---
echo ""
echo "[pos] Starting positive-mode processing..."
echo "[pos] Manifest: $MANIFEST_POS"
echo "[pos] Params:   $PARAMS_POS"
echo "[pos] Output:   $OUT_POS"
time "$MSDIAL_CONSOLE" lcmsdda \
    -i "$MANIFEST_POS" \
    -o "$OUT_POS" \
    -m "$PARAMS_POS"
echo "[pos] Done at $(date)"

# --- Negative mode ---
echo ""
echo "[neg] Starting negative-mode processing..."
echo "[neg] Manifest: $MANIFEST_NEG"
echo "[neg] Params:   $PARAMS_NEG"
echo "[neg] Output:   $OUT_NEG"
time "$MSDIAL_CONSOLE" lcmsdda \
    -i "$MANIFEST_NEG" \
    -o "$OUT_NEG" \
    -m "$PARAMS_NEG"
echo "[neg] Done at $(date)"

# --- Standardize output ---
echo ""
echo "[standardize] Combining AlignResult files..."
ALIGN_POS=$(find "$OUT_POS" -name 'AlignResult-*.msdial' -print -quit)
ALIGN_NEG=$(find "$OUT_NEG" -name 'AlignResult-*.msdial' -print -quit)

if [[ -z "$ALIGN_POS" ]]; then
    echo "WARNING: No AlignResult found in $OUT_POS"
fi
if [[ -z "$ALIGN_NEG" ]]; then
    echo "WARNING: No AlignResult found in $OUT_NEG"
fi

STANDARDIZE_ARGS=()
if [[ -n "$ALIGN_POS" ]]; then
    STANDARDIZE_ARGS+=(--pos "$ALIGN_POS")
fi
if [[ -n "$ALIGN_NEG" ]]; then
    STANDARDIZE_ARGS+=(--neg "$ALIGN_NEG")
fi

if [[ ${#STANDARDIZE_ARGS[@]} -gt 0 ]]; then
    python3 "$REPO_DIR/scripts/standardize_msdial_console.py" \
        "${STANDARDIZE_ARGS[@]}" \
        --tool-name msdial-console-v3-n100-qcoff \
        --output "$OUTPUT_ROOT/msdial-console-v3-n100-qcoff_feature_matrix.csv"
    echo "[standardize] Feature matrix written to $OUTPUT_ROOT/msdial-console-v3-n100-qcoff_feature_matrix.csv"
else
    echo "ERROR: No AlignResult files found. Standardization skipped."
    exit 1
fi

echo ""
echo "============================================"
echo "All done at $(date)"
echo "============================================"
