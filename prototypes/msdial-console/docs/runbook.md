# MS-DIAL Console Runbook — TLG1025

Tracks each pipeline run against the TLG1025 dataset: config, execution, job IDs, output paths, and outcomes.

## Dataset

- **Project**: TLG1025 (ZIKV/DENV time-course, HILIC, QE2)
- **Samples**: 116 mzML (58 pos, 58 neg) — Blank, QC, Sample
- **Raw data**: `/hpc/projects/mass_spec/projects/TLG1025/processed_raw_data/`
- **Alignment reference (pos)**: `QC_Pos_019821_TLG1025_QE2` (ID 58)
- **Alignment reference (neg)**: `QC_Neg_019822_TLG1025_QE2` (ID 58)

## Reference Libraries

| Library | Path | Size |
|---|---|---|
| Jan2026 HILIC (pos) | `.../metabolomics_libraries/internal/Jan2026/posMSP_HILIC_Jan2026.msp` | 33M lines |
| Jan2026 HILIC (neg) | `.../metabolomics_libraries/internal/Jan2026/negMSP_HILIC_Jan2026.msp` | 10.8M lines |
| Jan2026 HILIC (combined) | `.../metabolomics_libraries/internal/Jan2026/combined_HILIC_Jan2026.msp` | 43.8M lines |
| HMDB predicted | `.../metabolomics_libraries/hmdb/hmdb_predicted.msp` | 11.8M lines |
| HMDB experimental | `.../metabolomics_libraries/hmdb/hmdb_experimental.msp` | 1.1M lines |

All under `/hpc/projects/mass_spec_chi/Team/Tony/`.

---

## v3 — Recall-optimized baseline

- **Config**: `lcms_pos_v3_n100_qcoff.txt` / `lcms_neg_v3_n100_qcoff.txt`
- **Execution**: SLURM bash script (`hpc/tlg1025/run_tlg1025.sh`), pre-Nextflow
- **SLURM jobs**: 31614538 (failed, path issue), 31614539 (failed, crash), 31776261 (MS-DIAL succeeded, standardization failed on python 3.6)
- **Output**: `/hpc/projects/mass_spec_chi/Team/Tony/msdial-tlg1025/{pos,neg}/`
- **Feature matrix**: `/hpc/projects/mass_spec_chi/Team/Tony/msdial-tlg1025/msdial-console-v4-blankon-refmatchoff_feature_matrix.csv` (mislabeled — actually v3 data, standardized post-hoc with python3.9)
- **Key settings**:
  - `N% detected in at least one group: 100`
  - `QC at least filter: FALSE`
  - `Remove feature based on peak height fold-change: FALSE` (blank subtraction off)
  - No reference library (`MSP file` not set)
- **Pipeline steps**: Feature detection + standardize only
- **Rationale**: Maximize recall for GUI parity benchmarking.
- **Outcome**: Best 1:1 parity with GUI. Results:
  - Pos: 8,224 features, 7,258 matched to GUI (92.8%), median Pearson 1.0
  - Neg: 6,402 features, 6,083 matched to GUI (97.6%), median Pearson 1.0
  - 135 discordant features (Pearson < 0.5)
  - No pos MSP file was exported (neg MSP exported successfully)
  - Analysis: `notebooks/02_gui_vs_cli_head_to_head.ipynb`, output in `notebooks/output/gui-vs-cli-v4-head-to-head/` (dir name reflects the mislabeled matrix)

---

## v4 — Stricter filtering (blank subtraction on)

- **Config**: `lcms_pos_v4_blankon_refmatchoff.txt` / `lcms_neg_v4_blankon_refmatchoff.txt`
- **Execution**: Not yet run as a standalone job. v4 configs exist but no dedicated SLURM/Nextflow run was completed.
- **Key changes from v3**:
  - `Remove feature based on peak height fold-change: TRUE`
  - `Sample max / blank average: 5`
  - `Sample average / blank average: 5`
  - `Keep identified and annotated metabolites: FALSE`
- **Pipeline steps**: Feature detection + standardize only
- **Rationale**: Test whether stricter blank filtering improves precision without unacceptable recall loss.
- **Outcome**: Per workflow plan docs, reduced feature counts and recall substantially; precision stayed high. v3 remains the better operating point for GUI parity.

---

## v5 — Full pipeline with reference library (Jan2026 HILIC)

- **Config**: `lcms_pos_v5_reflib.txt` / `lcms_neg_v5_reflib.txt`
- **Execution**: Nextflow (`main.nf`, `-profile standard`), via `hpc/tlg1025/run_tlg1025_v5.sh`
- **Manifests**: `hpc/tlg1025/input_manifest_{pos,neg}.csv` (absolute paths)
- **SLURM jobs**: 32000864 (failed, nextflow not found), 32000924 (failed, relative mzML paths in Nextflow work dir), 32021201 (submitted, pending)
- **Output**: `/hpc/projects/mass_spec_chi/Team/Tony/msdial-tlg1025/v5_reflib/`
- **Key changes from v4**:
  - Added `MSP file` in MS-DIAL params pointing to Jan2026 HILIC library (polarity-specific)
  - All other MS-DIAL parameters identical to v4
- **Pipeline steps** (all enabled):
  - Feature detection (with reference library matching)
  - Standardize
  - MSKnit molecular networking
  - PyCutter QC/preprocessing
  - Tiered annotation:
    - Tier 1: Jan2026 HILIC combined library (`combined_HILIC_Jan2026.msp`)
    - Tier 2: HMDB predicted library (`hmdb_predicted.msp`)
    - Tier 3: SIRIUS formulas (DiffMS skipped — no checkpoint available)
- **Nextflow flags**:
  ```
  --run_msknit true
  --run_pycutter true
  --run_annotation true
  --reference_library .../internal/Jan2026/combined_HILIC_Jan2026.msp
  --predicted_library .../hmdb/hmdb_predicted.msp
  ```
- **Rationale**: First full pipeline run. Test reference library impact on feature identification plus end-to-end annotation workflow.
- **Outcome**: _pending (job 32021201)_

---

## v6 — Full pipeline + DiffMS structure prediction

- **Config**: Same MS-DIAL params as v5 (`lcms_{pos,neg}_v5_reflib.txt`)
- **Execution**: Nextflow (`main.nf`, `-profile standard`, `-resume`), via `hpc/tlg1025/run_tlg1025_v6.sh`
- **Manifests**: `hpc/tlg1025/input_manifest_{pos,neg}.csv` (absolute paths)
- **SLURM jobs**: _not yet submitted — waiting for v5 to finish_
- **Output**: `/hpc/projects/mass_spec_chi/Team/Tony/msdial-tlg1025/v6_reflib_diffms/`
- **Key changes from v5**:
  - Added `--diffms_checkpoint` pointing to DiffMS MSG model
  - Uses `-resume` to skip steps already cached from v5
- **Pipeline steps** (all enabled):
  - Feature detection (cached from v5)
  - Standardize (cached from v5)
  - MSKnit molecular networking (cached from v5)
  - PyCutter QC/preprocessing (cached from v5)
  - Tiered annotation:
    - Tier 1: Jan2026 HILIC combined library (cached from v5)
    - Tier 2: HMDB predicted library (cached from v5)
    - Tier 3: SIRIUS formulas (cached from v5) → **DiffMS structure prediction (new)** → pseudolibrary build
- **DiffMS checkpoint**: `/hpc/mydata/anthony.goering/models/diffms/checkpoints/diffms_msg.ckpt` (325MB, MSG-trained)
- **Rationale**: Add de novo structure prediction to complete the Tier 3 annotation. Resume from v5 cache to avoid re-running MS-DIAL.
- **Outcome**: _pending_

---

## Lessons Learned

- **Absolute paths in manifests**: Nextflow stages files into work directories, so relative mzML paths in manifests break. Use `hpc/tlg1025/input_manifest_*.csv` (absolute paths) for Nextflow runs.
- **Module loading**: SLURM jobs need `module load nextflow/24.10.5` explicitly. System python3 is 3.6.8; use `python3.9` or `module load anaconda && conda activate omni` for scripts needing pandas/numpy.
- **Head job time limit**: The Nextflow head process must stay alive for the full pipeline. Set `--time=12:00:00` for the submitting SLURM job.
- **MSP export quirk**: v3 run only exported MSP for neg mode, not pos. May be a patched console issue or timing artifact.
