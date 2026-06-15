# MS-DIAL Console Workflow Plan for HPC

This note documents the working MS-DIAL Console approach used for the TLG1025 benchmark and a practical path for turning it into a Slurm/Nextflow workflow step.

## Current Best Approach

The strongest console result so far is `recall_probe_v3_n100_qcoff`:

- Use MS-DIAL Console `lcmsdda` once per polarity.
- Provide inputs through a CSV manifest rather than an input directory.
- Use sample metadata columns to preserve `Blank`, `QC`, and biological `class_id` information.
- Use a QC alignment reference selected by file name.
- Set `N% detected in at least one group: 100` for this cell-culture dataset.
- Set `QC at least filter: FALSE`.
- Keep blank fold-change removal disabled for the parity-oriented run.

The v4 run tested stricter GUI-like filtering:

- `Remove feature based on peak height fold-change: TRUE`
- `Keep identified and annotated metabolites: FALSE`

That reduced feature counts and recall substantially while precision stayed high, so v3 remains the better operating point for near-1:1 GUI parity.

## Required Inputs

Each polarity needs:

- `input_manifest_<polarity>.csv`
- `lcms_<polarity>_recall_probe_v3_n100_qcoff.txt`
- mzML files for that polarity
- patched MS-DIAL Console binary

The manifest format currently used is:

```csv
file_path,file_name,type,class_id,batch,analytical_order,inject_volume
../../../processed_raw_data/pos/BK_Pos_019699_TLG1025_QE2.mzML,BK_Pos_019699_TLG1025_QE2,Blank,blank,1,1,5
../../../processed_raw_data/pos/QC_Pos_019701_TLG1025_QE2.mzML,QC_Pos_019701_TLG1025_QE2,QC,pool,1,2,5
```

Important semantics:

- `type` must be one of `Sample`, `QC`, or `Blank`.
- `class_id` is the biological group used by `N% detected in at least one group`.
- `file_path` may be relative to the manifest location, but that only works with the patched console code.
- `Alignment reference file name` should match `file_name` without `.mzML`.

## Key v3 Parameters

The v3 parameter choice is:

```text
Ion mode: Positive|Negative
MS1 data type: Centroid
MS2 data type: Centroid
Number of threads: <cpus requested from Slurm/Nextflow>
Minimum peak height: 50000
Mass slice width: 0.01
Retention time tolerance for alignment: 0.1
MS1 tolerance for alignment: 0.015
Peak count filter: 0
QC at least filter: FALSE
N% detected in at least one group: 100
Remove feature based on peak height fold-change: FALSE
Sample max / blank average: 5
Sample average / blank average: 5
Alignment reference file name: <QC file name>
Alignment reference file ID: 58
```

For production, prefer setting `Alignment reference file name` and treating `Alignment reference file ID` as informational. The patched code resolves the ID from the file name after loading the manifest.

## MS-DIAL Code Changes Needed

The working console binary is not stock MS-DIAL. It is built from a patched checkout at:

```text
/Users/agoering/opt/msdial/MsdialWorkbench
```

Relevant patches:

- `src/MSDIAL4/MsdialConsoleAppCore/Parser/AnalysisFilesParser.cs`
  - Fixes CSV manifest relative path resolution.
  - Relative `file_path` entries are resolved against the manifest directory.
- `src/MSDIAL4/MsdialConsoleAppCore/Process/LcmsDdaProcess.cs`
  - Uses the output directory as the run root when `-i` points to a manifest file.
  - Prevents internal project/temp paths from being created under `input_manifest.csv/...`.
- `src/MSDIAL4/MsdialConsoleAppCore/Process/LcmsDiaProcess.cs`
  - Same run-root fix for DIA mode.
- `src/MSDIAL4/MsdialConsoleAppCore/Parser/ConfigParser.cs`
  - Exposes `QC at least filter` for LCMS configs.
  - Supports `Alignment reference file name` and resolves it to the loaded analysis file ID.
  - Supports explicit `Number of threads`.
- `src/MSDIAL4/MsdialConsoleAppCore/Export/ResultExportForLC.cs`
  - Ensures the output directory exists before per-file `.msdial` exports.
- `src/MSDIAL4/MsdialConsoleAppCore/MsdialConsoleAppCore.csproj`
  - Temporarily narrowed target frameworks to `net8` for local build simplicity.

The first four are workflow-critical. The project-file change is build-environment-specific and should be revisited when packaging for Linux/HPC.

## Source Control Recommendation

Do not rely on an untracked local copy for HPC.

Recommended path:

1. Fork the upstream MS-DIAL workbench repository.
2. Create a small branch, for example `console-hpc-manifest-support`.
3. Commit only the minimal console patches needed for this workflow.
4. Tag the forked commit used by the pipeline, for example `msdial4-console-hpc-v0.1`.
5. Build all HPC binaries or containers from that tag.
6. Record the fork URL, branch, commit SHA, and binary checksum in the pipeline docs.

Why a fork is better than a local copy:

- It makes the patched code reproducible.
- It gives the pipeline an auditable source version.
- It lets us rebase/cherry-pick onto newer MS-DIAL releases later.
- It avoids hidden state on one workstation.

Keep this sandbox repo responsible for:

- manifests
- parameter templates
- benchmark notebooks
- output standardization
- workflow wrappers

Keep the MS-DIAL fork responsible for:

- patched console source
- build instructions
- release tags
- binary/container provenance

### Practical Transition Plan

If the fork is not ready immediately, use a short-lived patch-export workflow only as a bridge:

```bash
git -C /path/to/MsdialWorkbench diff \
  -- src/MSDIAL4/MsdialConsoleAppCore/Parser/AnalysisFilesParser.cs \
     src/MSDIAL4/MsdialConsoleAppCore/Parser/ConfigParser.cs \
     src/MSDIAL4/MsdialConsoleAppCore/Process/LcmsDdaProcess.cs \
     src/MSDIAL4/MsdialConsoleAppCore/Process/LcmsDiaProcess.cs \
     src/MSDIAL4/MsdialConsoleAppCore/Export/ResultExportForLC.cs \
     src/MSDIAL4/MsdialConsoleAppCore/MsdialConsoleAppCore.csproj \
  > msdial-console-hpc.patch
```

Then apply it to a clean upstream checkout on the HPC/build machine:

```bash
git apply msdial-console-hpc.patch
```

This is acceptable for first HPC smoke tests, but it should not be the long-term deployment mechanism. Once the smoke test works, move the patch into a forked branch and build from a tagged commit.

## HPC Packaging Options

### Preferred: Apptainer/Singularity Image

Build a Linux image containing:

- patched MS-DIAL Console binary
- .NET runtime or self-contained publish output
- any required native/vendor libraries
- Python standardization script dependencies if desired

Use the image from Nextflow with:

```nextflow
process.container = 'oras://.../msdial-console-hpc:<tag>'
```

or a site-supported `.sif` path.

This is the most reproducible option if the HPC supports Apptainer.

### Acceptable: Slurm Module + Pinned Binary

Install a patched Linux build in a shared read-only location, for example:

```text
/opt/msdial-console-hpc/<tag>/MsdialConsoleApp
```

Then expose it through an environment module:

```bash
module load msdial-console-hpc/<tag>
```

The module should set:

```bash
MSDIAL_CONSOLE=/opt/msdial-console-hpc/<tag>/MsdialConsoleApp
```

This is simpler than a container, but harder to reproduce across clusters.

### Avoid: Patching and Building Inside Every Workflow Run

Do not clone, patch, and build MS-DIAL inside each Nextflow task. That makes runs slower, fragile, and harder to audit.

## Build Notes for Linux

The current local binary is built on macOS and should not be assumed portable to the HPC.

For Linux, test one of these from the forked MS-DIAL checkout:

```bash
dotnet publish src/MSDIAL4/MsdialConsoleAppCore/MsdialConsoleAppCore.csproj \
  -c Release \
  -f net8 \
  -r linux-x64 \
  --self-contained true \
  -o build/console-linux-x64
```

If self-contained publish fails because of vendor assemblies or native libraries, build inside the same Apptainer image that will run on the cluster and use framework-dependent `net8` with the .NET runtime installed in the image.

Before accepting a build, run a smoke test on a small subset:

```bash
MsdialConsoleApp lcmsdda \
  -i input_manifest_pos_smoke.csv \
  -o out/pos \
  -m lcms_pos.txt
```

Expected checks:

- Console prints `Asked for <N> threads`.
- Console prints `Setting alignment reference file to id ...`.
- Per-file `.msdial` files are written under the output directory.
- One `AlignResult-*.msdial` appears in the output directory.
- Exit code is `0`.

## Nextflow Process Sketch

Run MS-DIAL separately for each polarity. The process should request `cpus`, and the config file should set `Number of threads` to the same value.

```nextflow
process MSDIAL_CONSOLE_LCMSDDA {
  tag "${sample_set_id}:${polarity}"
  cpus 14
  memory '64 GB'
  time '12h'

  input:
  val sample_set_id
  val polarity
  path manifest
  path params
  path mzml_files

  output:
  path "msdial_${polarity}", emit: msdial_output

  script:
  """
  mkdir -p msdial_${polarity}

  MsdialConsoleApp lcmsdda \
    -i ${manifest} \
    -o msdial_${polarity} \
    -m ${params}
  """
}
```

Implementation notes:

- Stage mzML files into the task work directory or use absolute paths that are valid on compute nodes.
- If using relative paths in the manifest, generate the manifest after staging files or keep the manifest next to the staged file tree.
- Avoid hardcoded workstation paths in parameter files and scripts.
- Capture the full MS-DIAL stdout/stderr log as a process output for debugging.
- `AlignResult-*.msdial` includes a timestamp, so downstream steps should glob for `AlignResult-*.msdial` rather than relying on a fixed name.

## Standardization Step

After MS-DIAL completes, convert each polarity-specific `AlignResult-*.msdial` output to the benchmark feature matrix format:

```text
feature_id,mz,rt,polarity,adduct,isotope_parent_id,occurrence_overall,intensity_<sample>...
```

For production, the standardization script should be parameterized to accept:

- positive `AlignResult-*.msdial`
- negative `AlignResult-*.msdial`
- tool/run name
- output CSV path

The current benchmarking script, `benchmarking/analysis/standardize_feature_matrices.py`, works for this repo but contains hardcoded benchmark paths and should be split into a reusable CLI wrapper before being used in a general Nextflow pipeline.

## Proposed Pipeline Shape

1. Build or pull patched MS-DIAL container/module.
2. Prepare polarity-specific mzML channels.
3. Generate manifest CSV for each polarity.
4. Generate parameter file for each polarity from a template.
5. Run `MSDIAL_CONSOLE_LCMSDDA` for positive mode.
6. Run `MSDIAL_CONSOLE_LCMSDDA` for negative mode.
7. Standardize `AlignResult-*.msdial` outputs.
8. Run optional QC/parity/reporting steps.

## Open Questions Before HPC Deployment

- Does the HPC support Apptainer/Singularity?
- Is outbound internet disabled on compute nodes? If so, containers/binaries must be staged ahead of time.
- What Linux distribution and CPU architecture are used?
- Is .NET 8 already available as a module, or should it be bundled?
- Are MS-DIAL vendor/native assemblies compatible with the target Linux environment?
- Where should large intermediate `.msdial`, `.pai`, and `.dcl` files live: task scratch, project scratch, or persistent results?
- Should Nextflow publish only `AlignResult-*.msdial` and the standardized matrix, or also all per-file intermediates?

## Immediate Next Steps

1. Create the MS-DIAL fork and commit the minimal console patch set.
2. Build a Linux test binary or Apptainer image from the fork.
3. Run a tiny two-to-four-file smoke test on the HPC.
4. Run one full polarity on the HPC and compare row counts/checksums against the workstation run.
5. Parameterize the standardization script as a small CLI.
6. Add the MS-DIAL process and standardization process to the Nextflow pipeline.
