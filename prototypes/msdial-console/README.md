# MS-DIAL Console Prototype

This prototype carries forward the MS-DIAL Console work from `metabolomics-sandbox` so it can be rebuilt and tested on HPC before becoming a proper Nextflow module.

## What Is Here

```text
prototypes/msdial-console/
  config/tlg1025/
    input_manifest_pos.csv
    input_manifest_neg.csv
    lcms_pos_v3_n100_qcoff.txt
    lcms_neg_v3_n100_qcoff.txt
    lcms_pos_v4_blankon_refmatchoff.txt
    lcms_neg_v4_blankon_refmatchoff.txt
  docs/
    console_hpc_build_notes.md
    msdial_cli_hpc_workflow_plan.md
    v3_documented_parameter_summary.md
  scripts/
    run_msdial_console.sh
    standardize_msdial_console.py
```

The main operating point is `v3_n100_qcoff`:

- `QC at least filter: FALSE`
- `N% detected in at least one group: 100`
- blank fold-change removal off
- one MS-DIAL Console run per polarity

The stricter `v4_blankon_refmatchoff` configs are included as a comparison, but they reduced recall substantially in the TLG1025 benchmark.

## Patched MS-DIAL Fork

Use the CZI fork branch:

```text
https://github.com/chanzuckerberg/msdial-fork
branch: console-hpc-manifest-qc
```

That branch contains the console changes needed for manifest-based runs:

- parse `QC at least filter`
- resolve relative CSV manifest paths against the manifest directory
- route CSV-manifest internal project/temp paths to the output directory
- build the console app as `net8`

See `docs/console_hpc_build_notes.md` in the fork repo for build recipes (macOS, Linux/HPC), NuGet source requirements, and MSDIAL4 vs MSDIAL5 comparison.

## Running The Prototype

The copied TLG1025 manifests are examples from the sandbox. Before running on HPC, update or regenerate `file_path` values so they point to mzML files visible from compute nodes.

Example:

```bash
export MSDIAL_CONSOLE=/hpc/mydata/anthony.goering/opt/msdial4/MsdialConsoleApp

prototypes/msdial-console/scripts/run_msdial_console.sh \
  pos \
  prototypes/msdial-console/config/tlg1025/input_manifest_pos.csv \
  prototypes/msdial-console/config/tlg1025/lcms_pos_v3_n100_qcoff.txt \
  work/msdial-tlg1025/pos

prototypes/msdial-console/scripts/run_msdial_console.sh \
  neg \
  prototypes/msdial-console/config/tlg1025/input_manifest_neg.csv \
  prototypes/msdial-console/config/tlg1025/lcms_neg_v3_n100_qcoff.txt \
  work/msdial-tlg1025/neg
```

Each output directory should contain one `AlignResult-*.msdial` file after a successful run.

## Standardizing Output

Convert positive and negative `AlignResult-*.msdial` files to the feature matrix format:

```bash
python prototypes/msdial-console/scripts/standardize_msdial_console.py \
  --pos work/msdial-tlg1025/pos/AlignResult-*.msdial \
  --neg work/msdial-tlg1025/neg/AlignResult-*.msdial \
  --tool-name msdial-console-v3-n100-qcoff \
  --output work/msdial-tlg1025/msdial-console-v3-n100-qcoff_feature_matrix.csv
```

The output schema is:

```text
feature_id,mz,rt,polarity,adduct,isotope_parent_id,occurrence_overall,intensity_<sample>...
```

## Nextflow Direction

Once the HPC smoke test works:

1. Convert `run_msdial_console.sh` into a Nextflow process.
2. Generate manifests from staged mzML files inside the task work directory.
3. Set `Number of threads` in params to match `task.cpus`.
4. Publish `AlignResult-*.msdial`, logs, and the standardized feature matrix.
5. Keep bulky per-file `.msdial`, `.pai`, and `.dcl` intermediates in scratch unless needed for debugging.
