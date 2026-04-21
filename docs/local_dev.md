# Local Development Guide

This guide covers setting up Rapid-QC-MS on your laptop for development and testing.
No Docker required for local dev.

---

## Prerequisites

- Python 3.11 or later (`python --version`)
- Git
- Optional: `msconvert` (ProteoWizard) for raw → mzML conversion

---

## Install

```bash
git clone https://github.com/czbiohub-sf/Rapid-QC-MS.git
cd Rapid-QC-MS
pip install -e ".[dev]"
```

Verify the CLI is available:

```bash
rapidqcms --help
```

---

## Initialize the database and import data

The default local database is SQLite at `data/rapidqcms.db`. Create it and import a run from mzML files:

```bash
mkdir -p data/mzml

# Set the DB URL for all subsequent commands in this shell
export RAPIDQCMS_DB_URL="sqlite:////$(pwd)/data/rapidqcms.db"

# Import QC results from a directory of mzML files
python scripts/import_from_mzml.py --path data/mzml --instrument MY_INST --run-id RUN001
```

---

## Serve the dashboard

```bash
export RAPIDQCMS_DB_URL="sqlite:////$(pwd)/data/rapidqcms.db"
rapidqcms serve --no-browser
```

Open http://localhost:8050 in your browser.

To reload automatically while editing dashboard code, add `--debug`:

```bash
rapidqcms serve --debug --no-browser
```

---

## Run tests

```bash
pytest -q
```

Tests use an in-memory SQLite database and a mock storage backend — no credentials needed.

---

## Environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `RAPIDQCMS_DB_URL` | Yes (prod) | `sqlite:///rapidqcms.db` | SQLAlchemy database URL |
| `RAPIDQCMS_STORAGE_BACKEND` | No | `local` | `local` or `s3` |
| `RAPIDQCMS_S3_BUCKET` | If S3 | — | S3 bucket name |
| `RAPIDQCMS_S3_PREFIX` | No | `rapidqcms/` | Key prefix inside the bucket |
| `RAPIDQCMS_SLACK_BOT_TOKEN` | No | — | Slack bot token for run alerts |
| `RAPIDQCMS_OKTA_DOMAIN` | No | — | Okta domain (e.g. `czbiohub.okta.com`) |
| `RAPIDQCMS_OKTA_CLIENT_ID` | No | — | Okta app client ID |
| `RAPIDQCMS_OKTA_CLIENT_SECRET` | No | — | Okta app client secret |
| `RAPIDQCMS_SESSION_SECRET` | No | derived from Okta creds | Flask session secret (32 hex bytes) |
| `RAPIDQCMS_EXTENSION` | No | `.raw` | Raw file extension watched by `listen` |
| `RAPIDQCMS_EXPERIMENT_TYPE` | No | `metabolomics` | `metabolomics`, `lipidomics`, or `proteomics` |
| `RAPIDQCMS_CHROMATOGRAPHY` | No | `HILIC` | Chromatography method |
| `RAPIDQCMS_POLARITY` | No | `Pos` | `Pos` or `Neg` |
| `RAPIDQCMS_QC_STAGE` | No | `pre_search` | QC checkpoint stage |
| `RAPIDQCMS_MSCONVERT_EXE` | No | — | Path to `msconvert` binary |
| `RAPIDQCMS_MSDIAL_EXE` | No | — | Path to MS-DIAL binary |
| `RAPIDQCMS_MSDIAL_PARAMS` | No | — | Path to MS-DIAL parameter file |
| `RAPIDQCMS_MZML_WATCH_PATH` | No | `./data/mzml` | Default watch path for `mzml-watch` |
| `RAPIDQCMS_USE_POLLING` | No | `0` | Set to `1` on NFS/Lustre (HPC) |
| `RAPIDQCMS_INSTRUMENT_ID` | No | `unknown` | Instrument ID for `mzml-watch` |
| `RAPIDQCMS_RUN_ID` | No | `auto` | Run ID for `mzml-watch` |

---

## Registering a new instrument

Instruments are registered in `lab_config.toml` (used by `rapidqcms watch`):

```toml
[[instruments]]
id = "QEHF_01"
name = "Q Exactive HF #1"
type = "Orbitrap"
location = "Room 220"
```

Then restart the watcher:

```bash
rapidqcms watch --path /data/projects --config lab_config.toml
```

The watcher syncs instruments to the DB on startup.

---

## Troubleshooting

**Port 8050 already in use**

```bash
# Find the process using the port
lsof -i :8050
# Kill it, then restart
kill <PID>
```

Or start on a different port: `rapidqcms serve --port 8051`.

**Database file not found**

Make sure `RAPIDQCMS_DB_URL` points to an absolute path:

```bash
export RAPIDQCMS_DB_URL="sqlite:////$(pwd)/data/rapidqcms.db"
```

Note the four slashes (`////`) — three for the SQLite scheme, one for the absolute path.

**mzML files not detected by the watcher**

- Confirm the file extension is `.mzML` (case-sensitive on Linux).
- By default `mzml-watch` requires both `HILIC` and `Metabolome` in the filename. Override with `--filter`:
  ```bash
  rapidqcms mzml-watch --path ./data/mzml --filter HILIC
  ```
- On NFS / Lustre filesystems, inotify events may not fire. Add `--polling`:
  ```bash
  rapidqcms mzml-watch --path ./data/mzml --polling
  ```
