#!/bin/bash
# =============================================================================
# SLURM batch script — Rapid-QC-MS mzML watcher
#
# Watches a directory for HILIC metabolomics mzML files and runs pre-search
# QC automatically.  Designed for HPC environments where mzML files land on a
# shared filesystem (NFS, Lustre, GPFS).
#
# Usage:
#   sbatch scripts/slurm_mzml_watch.sh
#
# Or override the watch path at submission time:
#   sbatch --export=ALL,MZML_PATH=/hpc/projects/ms/2024-run-01/mzml \
#          scripts/slurm_mzml_watch.sh
# =============================================================================

#SBATCH --job-name=rapidqcms-mzml-watch
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=24:00:00
#SBATCH --output=logs/slurm-%j.out
#SBATCH --error=logs/slurm-%j.err
# Send SIGTERM 60 s before SIGKILL so the watcher can finish in-flight QC
#SBATCH --signal=B:SIGTERM@60

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

# Adjust to match your HPC module system / conda setup:
# module load anaconda3/2023.09
# conda activate rapidqcms

# Or if installed in a venv:
# source /hpc/projects/mass_spec/envs/rapidqcms/bin/activate

# ---------------------------------------------------------------------------
# Watch path — set MZML_PATH at submission time or edit the default below
# ---------------------------------------------------------------------------
MZML_PATH="${MZML_PATH:-/hpc/projects/mass_spec/mzml}"

# ---------------------------------------------------------------------------
# Rapid-QC-MS settings (env vars override all defaults in config/__init__.py)
# ---------------------------------------------------------------------------

# Database (PostgreSQL in prod; SQLite for local testing)
export RAPIDQCMS_DB_URL="${RAPIDQCMS_DB_URL:-sqlite:///data/rapidqcms.db}"

# Storage backend
export RAPIDQCMS_STORAGE_BACKEND="${RAPIDQCMS_STORAGE_BACKEND:-local}"

# Instrument / run identification
export RAPIDQCMS_INSTRUMENT_ID="${RAPIDQCMS_INSTRUMENT_ID:-orbitrap_01}"
# Use the SLURM job ID as the run identifier (auto-resolved when set to "auto")
export RAPIDQCMS_RUN_ID="auto"   # → slurm_<SLURM_JOB_ID>

# QC parameters
export RAPIDQCMS_POLARITY="${RAPIDQCMS_POLARITY:-Pos}"
export RAPIDQCMS_CHROMATOGRAPHY="${RAPIDQCMS_CHROMATOGRAPHY:-HILIC}"

# Filename tokens that must appear to trigger QC (comma-separated)
export RAPIDQCMS_FILENAME_FILTERS="HILIC,Metabolome"

# REQUIRED on NFS/Lustre/GPFS — use stat-based polling instead of inotify
export RAPIDQCMS_USE_POLLING=1

# Stability check interval in seconds (mzML files are written atomically by
# msconvert, so 10-30 s is usually sufficient)
export RAPIDQCMS_MD5_INTERVAL=30

# ---------------------------------------------------------------------------
# Create log directory if it doesn't exist
# ---------------------------------------------------------------------------
mkdir -p logs

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
echo "[$(date -u +%FT%TZ)] Starting Rapid-QC-MS mzML watcher"
echo "  Watch path  : ${MZML_PATH}"
echo "  Filters     : HILIC, Metabolome"
echo "  Polarity    : ${RAPIDQCMS_POLARITY}"
echo "  SLURM job   : ${SLURM_JOB_ID}"
echo ""

rapidqcms mzml-watch \
    --path "${MZML_PATH}" \
    --polling

echo "[$(date -u +%FT%TZ)] Watcher exited (status $?)"
