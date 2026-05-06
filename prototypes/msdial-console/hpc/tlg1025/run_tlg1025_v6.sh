#!/bin/bash
#SBATCH --job-name=nf_tlg1025_v6
#SBATCH --output=/hpc/projects/mass_spec_chi/Team/Tony/msdial-tlg1025/logs/nf_tlg1025_v6_%j.out
#SBATCH --error=/hpc/projects/mass_spec_chi/Team/Tony/msdial-tlg1025/logs/nf_tlg1025_v6_%j.err
#SBATCH -p cpu
#SBATCH --time=12:00:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=2

set -euo pipefail

module load nextflow/24.10.5

# --- Paths ---
PIPELINE_DIR="/hpc/mydata/anthony.goering/repos/Rapid-QC-MS/prototypes/msdial-console"
CONFIG_DIR="$PIPELINE_DIR/config/tlg1025"
OUTDIR="/hpc/projects/mass_spec_chi/Team/Tony/msdial-tlg1025/v6_annotated"
LIBS_DIR="/hpc/projects/mass_spec_chi/Team/Tony/metabolomics_libraries"

echo "============================================"
echo "Nextflow MS-DIAL TLG1025 v6 (annotated)"
echo "============================================"
echo "Node:      $(hostname)"
echo "Started:   $(date)"
echo "Job ID:    ${SLURM_JOB_ID:-local}"
echo "Output:    $OUTDIR"
echo "============================================"

mkdir -p "$OUTDIR" /hpc/projects/mass_spec_chi/Team/Tony/msdial-tlg1025/logs

cd "$PIPELINE_DIR"

nextflow run main.nf \
    --manifest_pos "$PIPELINE_DIR/hpc/tlg1025/input_manifest_pos.csv" \
    --manifest_neg "$PIPELINE_DIR/hpc/tlg1025/input_manifest_neg.csv" \
    --params_pos   "$CONFIG_DIR/lcms_pos_v5_reflib.txt" \
    --params_neg   "$CONFIG_DIR/lcms_neg_v5_reflib.txt" \
    --outdir       "$OUTDIR" \
    --run_msknit         true \
    --run_pycutter       true \
    --run_annotation     true \
    --curated_library       "$LIBS_DIR/internal/Jan2026/combined_HILIC_Jan2026.msp" \
    --experimental_library  "$LIBS_DIR/hmdb/hmdb_experimental.msp" \
    --predicted_library     "$LIBS_DIR/hmdb/hmdb_predicted.msp" \
    -resume \
    -profile standard

echo ""
echo "============================================"
echo "All done at $(date)"
echo "============================================"
