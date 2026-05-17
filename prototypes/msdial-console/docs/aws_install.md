# AWS ParallelCluster Install Guide

End-to-end setup for running the MS-DIAL Nextflow prototype on a CZB AWS
ParallelCluster (login node + SLURM partitions). Reflects the validated
TLG1025 run completed on 2026-05-16. Total wallclock for the full
pipeline (116 samples × 2 polarities + Tier 1–4 annotation) was ~3h
using the `hmm-search` partition.

## Prerequisites already on the cluster

These come with the AMI — no install needed:

| Tool      | Verified version |
|-----------|------------------|
| Nextflow  | 25.10.4          |
| Apptainer | 1.4.5            |
| Docker    | 29.1.3           |
| Java      | (system OpenJDK) |
| SLURM     | login + 200 m5.4xlarge nodes in `hmm-search` |

A login-node-local `cargo`/`rustc` ships, but it's too old (1.75) for
this build — see step 2.

## One-time setup

### 1. Install .NET SDK (login node, ~30s)

```bash
curl --proto '=https' --tlsv1.2 -sSf https://dot.net/v1/dotnet-install.sh \
  | sh -s -- --channel 10.0 --install-dir $HOME/.dotnet
export PATH="$HOME/.dotnet:$PATH"
dotnet --list-sdks   # → 10.0.300 [...]
```

### 2. Install Rust ≥1.85 via rustup (login node, ~1min)

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
  | sh -s -- -y --default-toolchain stable --profile minimal
source $HOME/.cargo/env
rustc --version   # → 1.95.0 or newer
```

The msknit Rust crate's `Cargo.lock` is format v4 (rust 1.82+) and uses
deps requiring `edition2024` (stabilized rust 1.85+). Stable is safest.

### 3. Clone the prototype branch

```bash
cd ~/repos
git clone -b nextflow-msdial-prototype \
  git@github.com:czbiohub-sf/Rapid-QC-MS.git Rapid-QC-MS-msdial
# or git worktree add from an existing checkout
```

### 4. Build MS-DIAL Console (linux-x64)

Clone the CZI fork (already-patched for manifest-driven QC):

```bash
cd ~/repos
git clone -b console-hpc-manifest-qc \
  https://github.com/chanzuckerberg/msdial-fork.git
```

Build framework-dependent linux-x64 binary (~3–5 min):

```bash
cd ~/repos/msdial-fork
dotnet publish ./src/MSDIAL4/MsdialConsoleAppCore/MsdialConsoleAppCore.csproj \
  --configuration "Release vendor unsupported" \
  -p:DebugType=None \
  --runtime linux-x64 \
  --framework net8 \
  --source ./Assemblies \
  --source https://api.nuget.org/v3/index.json \
  -o ./build/console-linux-x64
```

**Stage flat into the container build context** (the `mist-cf.def`
expects `containers/msdial/MsdialConsoleApp` directly, NOT a
`MsdialConsoleApp_linux-x64/` subdir — this differs from the Dockerfile
layout in the same dir):

```bash
PROTO=~/repos/Rapid-QC-MS-msdial/prototypes/msdial-console
cp -r ~/repos/msdial-fork/build/console-linux-x64/. $PROTO/containers/msdial/
```

### 5. Build msknit Rust binary

```bash
cd ~/repos/Rapid-QC-MS-msdial/prototypes/msknit
source $HOME/.cargo/env
cargo build --release
cp target/release/msknit \
  ~/repos/Rapid-QC-MS-msdial/prototypes/msdial-console/containers/msknit/msknit
```

### 6. Stage MIST-CF source + model checkpoints

```bash
cd ~/repos
git clone https://github.com/samgoldman97/mist-cf.git
cd mist-cf
bash quickstart/download_model.sh   # fetches ~2.5 MB of .ckpt files from Zenodo

# Wire into the container build context as a symlink:
ln -sfn ~/repos/mist-cf \
  ~/repos/Rapid-QC-MS-msdial/prototypes/msdial-console/containers/mist-cf
```

### 7. Download SIRIUS 5.8.6 (NOT 6.x — see Gotchas)

```bash
mkdir -p ~/opt && cd ~/opt
curl -fL -O \
  https://github.com/sirius-ms/sirius/releases/download/v5.8.6/sirius-5.8.6-linux64.zip
unzip -q sirius-5.8.6-linux64.zip && rm sirius-5.8.6-linux64.zip
# Produces ~/opt/sirius/bin/sirius

ln -sfn ~/opt/sirius \
  ~/repos/Rapid-QC-MS-msdial/prototypes/msdial-console/containers/sirius

# Pre-create SIRIUS workspace dirs so apptainer can bind-mount them:
mkdir -p ~/.sirius ~/.sirius-5.8
```

### 8. Stage spectral libraries

Copy from the CZB HPC. Paths the v5 param files expect:

```
~/data/metabolomics_libraries/internal/Jan2026/posMSP_HILIC_Jan2026.msp
~/data/metabolomics_libraries/internal/Jan2026/negMSP_HILIC_Jan2026.msp
~/data/metabolomics_libraries/internal/Jan2026/combined_HILIC_Jan2026.msp
~/data/metabolomics_libraries/hmdb/hmdb_experimental.msp     # for --experimental_library
~/data/metabolomics_libraries/hmdb/hmdb_predicted.msp        # for --predicted_library
```

### 9. Build the four apptainer images

```bash
cd ~/repos/Rapid-QC-MS-msdial/prototypes/msdial-console/containers
for img in msdial pipeline-tools msknit mist-cf ; do
  apptainer build ${img}.sif ${img}.def
done
```

**Do not use `build_all.sh --apptainer-direct`** — that mode passes `.`
(a directory) to `apptainer build` instead of the `.def` file and
produces empty 40 KB stub SIFs. Use the loop above. Total build time
~15–20 min (mist-cf is the heavy one).

Expected sizes:
- `msdial.sif` ~100 MB
- `pipeline-tools.sif` ~88 MB
- `msknit.sif` ~32 MB
- `mist-cf.sif` ~830 MB

Sanity check msdial: `apptainer run containers/msdial.sif` should print MS-DIAL CLI usage.

## Per-run setup

### 1. Generate input manifests

From the example manifests, rewriting the HPC path prefix:

```bash
mkdir -p ~/runs/<run_name>
PROTO=~/repos/Rapid-QC-MS-msdial/prototypes/msdial-console
for pol in pos neg ; do
  sed 's|/hpc/projects/mass_spec/projects/TLG1025/processed_raw_data/|~/data/TLG1025/processed_raw_data/|g; s|~|/shared/home/'$USER'|g' \
    $PROTO/hpc/tlg1025/input_manifest_${pol}.csv \
    > ~/runs/<run_name>/input_manifest_${pol}.csv
done
```

Verify all `file_path` values resolve:

```bash
awk -F, 'NR>1 {print $1}' ~/runs/<run_name>/input_manifest_pos.csv \
  | xargs -I{} test -f {} || echo "MISSING ABOVE"
```

### 2. Patch MS-DIAL param files for local paths

```bash
cp $PROTO/config/tlg1025/lcms_{pos,neg}_v5_reflib.txt ~/runs/<run_name>/
sed -i 's|/hpc/projects/mass_spec_chi/Team/Tony/metabolomics_libraries/|/shared/home/'$USER'/data/metabolomics_libraries/|g' \
  ~/runs/<run_name>/lcms_{pos,neg}_v5_reflib.txt
```

For a smoke-test subset (4 samples), also override the alignment
reference to a QC that's actually in the subset — see
`Smoke test` below.

### 3. Launch full run on SLURM

```bash
cd $PROTO
nextflow -log ~/runs/<run_name>/.nextflow.log run main.nf \
  --manifest_pos ~/runs/<run_name>/input_manifest_pos.csv \
  --manifest_neg ~/runs/<run_name>/input_manifest_neg.csv \
  --params_pos   ~/runs/<run_name>/lcms_pos_v5_reflib.txt \
  --params_neg   ~/runs/<run_name>/lcms_neg_v5_reflib.txt \
  --outdir       ~/runs/<run_name>/results \
  --run_annotation true \
  --run_msknit    true \
  --run_pycutter  false \
  --curated_library      ~/data/metabolomics_libraries/internal/Jan2026/combined_HILIC_Jan2026.msp \
  --experimental_library ~/data/metabolomics_libraries/hmdb/hmdb_experimental.msp \
  --predicted_library    ~/data/metabolomics_libraries/hmdb/hmdb_predicted.msp \
  -profile hmm_search \
  -ansi-log false
```

Profiles available:
- `hmm_search` — SLURM submission to AWS ParallelCluster `hmm-search`
  partition (m5.4xlarge, 16 CPU / 64 GB). Recommended.
- `aws_single` — Apptainer + local executor on the login node. Useful
  for smoke tests. Resource caps assume 8 CPU / 15 GB.

## Smoke test (recommended before any full run)

Build a 4-sample subset (1 Blank + 1 QC + 2 Samples per polarity) and
copy the v5 params to a `_smoke` variant with the alignment reference
overridden to a QC that's actually in the subset:

```bash
for pol in pos neg ; do
  head -1 ~/runs/<run_name>/input_manifest_${pol}.csv > \
    ~/runs/<run_name>/input_manifest_${pol}.small.csv
  awk -F, 'NR>1 && $3=="Blank" {print; exit}' ~/runs/<run_name>/input_manifest_${pol}.csv >> ~/runs/<run_name>/input_manifest_${pol}.small.csv
  awk -F, 'NR>1 && $3=="QC"    {print; exit}' ~/runs/<run_name>/input_manifest_${pol}.csv >> ~/runs/<run_name>/input_manifest_${pol}.small.csv
  awk -F, 'NR>1 && $3=="Sample"' ~/runs/<run_name>/input_manifest_${pol}.csv | head -2 >> ~/runs/<run_name>/input_manifest_${pol}.small.csv
done

cp ~/runs/<run_name>/lcms_{pos,neg}_v5_reflib.txt ~/runs/<run_name>/
# rename to _smoke and patch alignment ref to the QC at row 2 of the subset
# (e.g. QC_Pos_019701 / QC_Neg_019702 for TLG1025)
```

Run with `-profile aws_single -resume` first; once that's green, launch
the full run on `hmm_search`.

## Resource-sizing notes (full TLG1025 run on m5.4xlarge)

| Process       | Duration      | Peak RSS | Notes |
|---------------|---------------|----------|-------|
| MSDIAL_POS    | 1h 48m        | 5.7 GB   | 16 CPU / 56 GB request |
| MSDIAL_NEG    | 2h 12m        | 5.0 GB   | runs in parallel with POS |
| MIST_CF_POS   | 1h 39m        | **44 GB**| process_medium request was 32 GB — survives because m5.4xlarge has 64 GB; consider bumping to process_high if memory pressure grows |
| MIST_CF_NEG   | 1h 12m        | 38.7 GB  | same caveat |
| All others    | <10m each     | <2 GB    | |
| **Wallclock** | **~3h**       |          | parallelized pos/neg via SLURM |

If MIST-CF runs out of memory on larger datasets, edit
`hmm_search.process { withName: 'MIST_CF_.*' { memory = '56.GB' } }` in
`nextflow.config`.

## Gotchas (and why)

- **SIRIUS 6.x doesn't work.** Two regressions vs 5.x: (1) `decomp
  --mass` no longer accepts comma-separated lists (now `--mass m1
  --mass m2 ...`); (2) mandatory login even for offline `decomp`. MIST-CF
  was written against the SIRIUS 4/5 API. Stick with **5.8.6**.

- **SIRIUS workspace dirs must be bind-mounted.** Nextflow's apptainer
  integration passes `--no-home`, so `$HOME` inside the container is
  the (read-only) overlay rather than the host home. SIRIUS tries to
  create `~/.sirius` and `~/.sirius-5.8` and fails with "Read-only file
  system". The `aws_single` and `hmm_search` profiles in
  `nextflow.config` explicitly bind both paths; pre-create them on the
  host with `mkdir`.

- **MS-DIAL Console must be a framework-dependent build, NOT
  self-contained.** The msdial.def uses `mcr.microsoft.com/dotnet/runtime:8.0-noble`
  which provides the .NET 8 runtime. A self-contained build bloats the
  binary tree by 100+ MB unnecessarily.

- **`containers/msdial/` layout differs from Dockerfile vs .def.** The
  Dockerfile expects a `MsdialConsoleApp_linux-x64/` subdir; the .def
  expects the binary tree flat in `containers/msdial/`. We use the
  .def. Stage flat.

- **`build_all.sh --apptainer-direct` is broken.** It passes `.` to
  `apptainer build` instead of the `.def` file, producing empty 40 KB
  stub SIFs. Use the per-image loop in step 9 above instead.

- **MIST-CF needs pip deps not in its quickstart/requirements.txt.**
  Specifically `pathos` (provides `multiprocess`), `einops`,
  `hyperopt`. These are in the current `mist-cf.def`; if you rebuild
  from upstream, keep them.

- **PyCutter is currently broken under apptainer.** `params.pycutter_dir`
  defaults to `/opt/pycutter` but no PyCutter is installed in the
  pipeline-tools container. Use `--run_pycutter false` until PyCutter is
  baked in or made a separate container.

- **`--curated_library` is a single MSP for both polarities.** Use
  `combined_HILIC_Jan2026.msp`, not the per-polarity `pos/negMSP_...`
  files. `--experimental_library` and `--predicted_library` follow the
  same convention.

- **MIST-CF (Tier 4) is positive-mode only and skipped by default in
  negative polarity.** MIST-CF's `ION_LST` and trained model are
  hard-coded for `[M+H]+`, `[M+Na]+`, `[M+K]+`, `[M-H2O+H]+`,
  `[M+H3N+H]+`, `[M]+`, `[M-H4O2+H]+` (NPLIB1/GNPS positive training
  set). Running on negative-mode features without patching MIST-CF
  produces formulas off by H/H2 because `[M-H]-` is never considered.
  `main.nf` defaults `params.run_mist_cf_negative = false` and skips
  `MIST_CF_NEG`, providing a `NO_FILE` placeholder to `MERGE_NEG`.
  Override with `--run_mist_cf_negative true` only after patching
  `chem_utils.py` to add negative adducts (and accepting the model
  domain-shift, since training was positive-only).

## Tunable parameters

Beyond the standard `--manifest_*`, `--params_*`, `--*_library` flags, the
pipeline has a few knobs worth knowing:

| Param                          | Default | Purpose |
|--------------------------------|---------|---------|
| `--run_mist_cf_negative`       | `false` | Run MIST-CF on negative-polarity features. Off because MIST-CF's `ION_LST` and trained model are positive-only — neg formulas come out systematically off by H/H2. See gotchas. |
| `--merge_mistcf_top_k`         | `3`     | Number of MIST-CF formula candidates kept per feature in `merged_annotations.csv`. Rank 1 goes in `t4_*`; ranks 2..K go in `t4_alt2_*`..`t4_alt{K}_*`. Useful because at high mass accuracy the library formula is in MIST-CF top 3 ~96% of the time vs ~88% at rank 1 on TLG1025. |
| `--run_msknit`                 | `false` | Build molecular networks from MS-DIAL MSP libraries. Outputs `msknit_out/{nodes,edges}.csv`, `network.graphml`. Cheap (~6 min on full TLG1025). |
| `--run_pycutter`               | `false` | Blank subtraction / CV filtering. Currently **broken under apptainer** — see gotchas. Leave `false`. |
| `--run_annotation`             | `false` | Master switch for Tier 1-4. Set `true` for any of the tier libraries to be searched. |

## Per-run output layout

```
~/runs/<run_name>/results/
├── feature_matrix.csv                            # standardized merged table
├── pipeline_report.html
├── timeline_report.html
├── trace.txt
├── run_manifest.txt                              # params + container md5s
├── pos/
│   ├── output/AlignResult-*.msdial, AlignResult-*.msp
│   ├── annotated_feature_table.tsv               # MSDIAL + merged annotations (per-sample intensities + Tier 1-4 cols)
│   ├── annotations/
│   │   ├── merged_annotations.csv                # one row per feature, all tier hits joined
│   │   ├── tier1_curated/hits.csv                # raw msknit search output
│   │   ├── tier2_experimental/hits.csv
│   │   └── tier3_predicted/hits.csv
│   ├── msknit/msknit_out/{nodes,edges}.csv, network.graphml
│   └── mist_cf/mist_cf_out/formatted_output.tsv  # all MIST-CF candidates (top-K selected into merged_annotations)
└── neg/  # same layout; mist_cf/ absent when run_mist_cf_negative=false
```

### `merged_annotations.csv` schema

```
alignment_id, best_tier,
t1_name, t1_formula, t1_smiles, t1_inchikey, t1_cosine, t1_matched_peaks,
t2_*  (same shape as t1),
t3_*  (same shape as t1),
t4_formula, t4_adduct, t4_score,                   # MIST-CF rank 1
t4_alt2_formula, t4_alt2_adduct, t4_alt2_score,    # MIST-CF rank 2  (only if --merge_mistcf_top_k >= 2)
t4_alt3_formula, t4_alt3_adduct, t4_alt3_score     # MIST-CF rank 3
```

`best_tier` is the highest-confidence tier that produced a hit for that
feature: `tier1_curated` > `tier2_experimental` > `tier3_predicted` >
`tier4_formula`. For neg with `run_mist_cf_negative=false`, all `t4_*`
fields are blank.
