# Rapid-QC-MS

**Automated QC orchestrator for LC-MS instruments at CZ Biohub SF.**

Rapid-QC-MS sits between instrument data acquisition and downstream HPC pipelines. It evaluates raw data at four checkpoints and gates each step: a Pass lets the pipeline proceed; a Fail drops a `.qc_fail` sidecar next to the raw file and blocks the next step.

---

## Checkpoint architecture

```
Instrument acquisition
        │
  ┌─────▼──────┐
  │ C1: Pre-   │  Instrument health check (future)
  │ acquisition│
  └─────┬──────┘
        │ .qc_pass
  ┌─────▼──────────────┐
  │ C2: Post-raw-file  │  metabolomics_pre / proteomics_pre modules
  │    (pre-search)    │  Reads raw/.mzML; writes .qc_pass or .qc_fail
  └─────┬──────────────┘
        │ .qc_pass
        ▼
   FragPipe / MS-DIAL
        │
  ┌─────▼──────────────┐
  │ C3: Post-search    │  (future) post_search module
  │ (pre-stats)        │  Reads msstats.csv / report.tsv
  └─────┬──────────────┘
        │ .qc_pass
        ▼
   Statistical treatment
        │
  ┌─────▼──────────────┐
  │ C4: Post-stats     │  (future) post_stats module
  │ (pre-delivery)     │  Final delivery gate → ProteOhub
  └─────┬──────────────┘
        │ .qc_pass
        ▼
   CZB-MAP Nextflow / delivery
```

Gate files are JSON so downstream scripts can read the QC outcome and metrics without querying the database:

```json
{
  "status": "Pass",
  "stage": "pre_search",
  "module": "pipeline",
  "metrics": {"modules_run": 2, "per_module": {"metabolomics_pre": "Pass"}},
  "timestamp": "2025-01-15T09:32:11+00:00"
}
```

---

## Architecture

```
src/rapidqcms/
  config/           Settings dataclass (env vars), qc_config.yaml, lab_config.toml
  db/               SQLAlchemy models, connection, results, migration
  qc/               QCModule ABC + registry; metabolomics_pre, proteomics_pre modules
  service/
    pipeline.py     Orchestrator: load registry → run modules → write gate file
    processor.py    MSConvert + MS-DIAL subprocess wrappers
    events/         Gate file I/O (write_gate_file, read_gate_file, get_gate_status)
    watchers/       File-system watchers (listener, watcher, mzml_watcher)
  storage/          StorageBackend protocol; LocalStorageBackend, S3StorageBackend
  dashboard/        Dash 2.x web app (app, layout, callbacks, plots, auth)
```

**Database:** PostgreSQL on AWS RDS (prod) / SQLite (local dev).
**Storage:** S3 (prod) / local filesystem (dev).
**Auth:** Okta SSO (dashboard). No-op when Okta env vars are absent.

---

## Quick start

```bash
# 1. Install
pip install -e ".[dev]"

# 2. Serve dashboard (SQLite, local dev)
RAPIDQCMS_DB_URL="sqlite:////$(pwd)/data/rapidqcms.db" rapidqcms serve --no-browser

# 3. Watch a directory for mzML files and run metabolomics QC automatically
rapidqcms mzml-watch --path ./data/mzml
```

See [`docs/local_dev.md`](docs/local_dev.md) for full setup instructions.

---

## How to add a QC module

1. Create `src/rapidqcms/qc/my_module.py` and subclass `QCModule`:

```python
from rapidqcms.qc.base import QCModule, QCResult, QCStatus

class MyModule(QCModule):
    name = "my_module"

    def analyze(self, input_path, context):
        # ... your checks ...
        return QCResult(status=QCStatus.PASS, module=self.name, metrics={})
```

2. Register it in `src/rapidqcms/config/qc_modules.toml`:

```toml
[modules.my_module]
enabled = true
stage = "pre_search"
experiment_type = "metabolomics"
```

3. Add checkpoint mapping in `src/rapidqcms/config/qc_config.yaml` under `checkpoints.c2.modules`.

The `pipeline.run_qc()` orchestrator picks up registered modules automatically.

---

## Configuration files

| File | Purpose |
|------|---------|
| `src/rapidqcms/config/qc_config.yaml` | IS library, checkpoint definitions |
| `src/rapidqcms/config/qc_modules.toml` | Module registry (enabled, stage, experiment_type) |
| `lab_config.toml` | Instrument registration (used by `rapidqcms watch`) |

Key environment variables: `RAPIDQCMS_DB_URL`, `RAPIDQCMS_STORAGE_BACKEND`, `RAPIDQCMS_S3_BUCKET`, `RAPIDQCMS_SLACK_BOT_TOKEN`, `RAPIDQCMS_OKTA_DOMAIN/CLIENT_ID/CLIENT_SECRET`.

Full env var table: [`docs/local_dev.md`](docs/local_dev.md#environment-variables).
