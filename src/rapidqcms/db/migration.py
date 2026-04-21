"""One-shot migration helper: import data from the old per-instrument Settings.db
into the new consolidated database.

Usage (CLI):
    rapidqcms migrate --settings-db /path/to/Settings.db

Programmatic:
    from rapidqcms.db.migration import import_qc_configurations
    from sqlalchemy.orm import Session

    n_qc = import_qc_configurations(Path("/path/to/Settings.db"), session)

Note: internal_standards are no longer stored in the database.
      They are defined in config/qc_config.yaml.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from sqlalchemy.orm import Session

from .features import upsert_qc_configuration


def import_qc_configurations(settings_db_path: Path, session: Session) -> int:
    """Read the qc_parameters table from a legacy Settings.db and upsert into qc_configurations.

    Returns the number of records imported.
    """
    conn = sqlite3.connect(settings_db_path)
    try:
        cursor = conn.execute("SELECT * FROM qc_parameters")
        cols = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
    finally:
        conn.close()

    # Column name mapping: old Settings.db → new QCConfiguration fields
    _col_map = {
        "intensity_dropouts_cutoff": "intensity_dropouts_cutoff",
        "library_rt_shift_cutoff": "library_rt_shift_cutoff",
        "in_run_rt_shift_cutoff": "in_run_rt_shift_cutoff",
        "library_mz_shift_cutoff": "library_mz_shift_cutoff",
        "intensity_enabled": "intensity_enabled",
        "library_rt_enabled": "library_rt_enabled",
        "in_run_rt_enabled": "in_run_rt_enabled",
        "library_mz_enabled": "library_mz_enabled",
    }

    count = 0
    for row in rows:
        data = dict(zip(cols, row))
        config_id = data.get("id") or data.get("name", "default")
        thresholds = {
            new_col: data[old_col]
            for old_col, new_col in _col_map.items()
            if old_col in data
        }
        upsert_qc_configuration(session, config_id, **thresholds)
        count += 1

    return count
