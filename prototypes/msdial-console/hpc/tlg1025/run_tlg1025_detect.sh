#!/bin/bash
#SBATCH --job-name=nf_tlg1025_detect
#SBATCH --output=/hpc/projects/mass_spec_chi/Team/Tony/msdial-tlg1025/logs/nf_tlg1025_detect_%j.out
#SBATCH --error=/hpc/projects/mass_spec_chi/Team/Tony/msdial-tlg1025/logs/nf_tlg1025_detect_%j.err
#SBATCH -p cpu
#SBATCH --time=12:00:00
#SBATCH --mem=4G
#SBATCH --cpus-per-task=2

# Stage 1: Feature detection + standardize only.
# Downstream steps can be added later with run_tlg1025_downstream.sh -resume.

set -euo pipefail

module load nextflow/24.10.5

PIPELINE_DIR="/hpc/mydata/anthony.goering/repos/Rapid-QC-MS/prototypes/msdial-console"
CONFIG_DIR="$PIPELINE_DIR/config/tlg1025"
OUTDIR="/hpc/projects/mass_spec_chi/Team/Tony/msdial-tlg1025/v6_reflib_diffms"

echo "============================================"
echo "Nextflow MS-DIAL TLG1025 — detect"
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
    -profile standard

echo ""
echo "============================================"
echo "Detection complete at $(date)"
echo "Run run_tlg1025_downstream.sh to add networking/annotation."
echo "============================================"
