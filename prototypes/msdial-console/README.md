# MS-DIAL Console Pipeline

Nextflow pipeline for LC-MS metabolomics feature detection with MS-DIAL Console, plus optional molecular networking, QC preprocessing, and tiered annotation.

## Pipeline Steps

| Step | Module | Flag | Description |
|---|---|---|---|
| Feature detection | `msdial.nf` | always on | MS-DIAL Console `lcmsdda` per polarity |
| Standardize | `standardize.nf` | always on | Combine AlignResults into a unified feature matrix |
| Molecular networking | `msknit.nf` | `--run_msknit` | MSKnit spectral similarity networks |
| PyCutter QC | `pycutter.nf` | `--run_pycutter` | Blank subtraction, CV filtering, polarity merge |
| Tiered annotation | `spectral_search.nf`, `sirius.nf`, `diffms.nf`, etc. | `--run_annotation` | Tier 1 (reference library), Tier 2 (predicted library), Tier 3 (SIRIUS + DiffMS) |

## Directory Layout

```text
prototypes/msdial-console/
  main.nf                    # Pipeline entrypoint
  nextflow.config            # Default params, SLURM profiles, resource labels
  modules/                   # Nextflow process definitions
  scripts/                   # Python/bash helpers called by processes
  config/tlg1025/            # MS-DIAL parameter files per version (v3, v4, v5)
  hpc/tlg1025/               # SLURM run scripts and absolute-path manifests
  notebooks/                 # Analysis notebooks (GUI vs CLI comparison, etc.)
  docs/                      # Runbook, parameter docs, build notes
  containers/                # Apptainer/Singularity definitions
```

## Quick Start

```bash
# Load environment
module load nextflow/24.10.5

# Feature detection only
nextflow run main.nf \
    --manifest_pos hpc/tlg1025/input_manifest_pos.csv \
    --manifest_neg hpc/tlg1025/input_manifest_neg.csv \
    --params_pos   config/tlg1025/lcms_pos_v5_reflib.txt \
    --params_neg   config/tlg1025/lcms_neg_v5_reflib.txt \
    --outdir       results/tlg1025_v5 \
    -profile standard

# Full pipeline (networking + QC + annotation)
nextflow run main.nf \
    --manifest_pos hpc/tlg1025/input_manifest_pos.csv \
    --manifest_neg hpc/tlg1025/input_manifest_neg.csv \
    --params_pos   config/tlg1025/lcms_pos_v5_reflib.txt \
    --params_neg   config/tlg1025/lcms_neg_v5_reflib.txt \
    --outdir       results/tlg1025_v5 \
    --run_msknit        true \
    --run_pycutter      true \
    --run_annotation    true \
    --reference_library /path/to/tier1_reference.msp \
    --predicted_library /path/to/tier2_predicted.msp \
    -profile standard
```

Or submit via SLURM: `sbatch hpc/tlg1025/run_tlg1025_v5.sh`

## Manifests

The manifest CSV format expected by MS-DIAL Console:

```csv
file_path,file_name,type,class_id,batch,analytical_order,inject_volume
/absolute/path/to/sample.mzML,sample_name,Sample,group1,1,1,5
```

- `file_path`: absolute path to mzML (required for Nextflow — relative paths break in work dirs)
- `type`: one of `Sample`, `QC`, or `Blank`
- `class_id`: biological group used by `N% detected in at least one group`

Manifests with absolute paths live in `hpc/tlg1025/`. The originals with relative paths are in `config/tlg1025/`.

## Parameter Versions

See [docs/runbook.md](docs/runbook.md) for the full run history with configs, rationale, and outcomes.

| Version | Key Change | Config Files |
|---|---|---|
| v3 | Recall-optimized baseline, no blank filtering | `lcms_{pos,neg}_v3_n100_qcoff.txt` |
| v4 | Blank subtraction on, stricter filtering | `lcms_{pos,neg}_v4_blankon_refmatchoff.txt` |
| v5 | + Jan2026 HILIC reference library for spectral matching | `lcms_{pos,neg}_v5_reflib.txt` |

## Patched MS-DIAL Fork

Uses the CZI fork with console patches for manifest-based runs:

```text
https://github.com/chanzuckerberg/msdial-fork
branch: console-hpc-manifest-qc
```

Binary location on HPC: `/hpc/mydata/anthony.goering/opt/msdial4/MsdialConsoleApp`

See [docs/console_hpc_build_notes.md](docs/console_hpc_build_notes.md) for build details.

## Analysis Notebooks

- `notebooks/02_gui_vs_cli_head_to_head.ipynb` — Feature-level and behavior-level comparison of GUI vs CLI output. Run with `module load anaconda && conda activate omni`.
