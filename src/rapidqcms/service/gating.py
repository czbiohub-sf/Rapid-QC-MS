"""Gate file writer/reader for event-driven pipeline triggering.

The gating contract between this service and downstream processes
(FragPipe launcher, CZB-MAP trigger) is a pair of sidecar files:

  sample_001.raw        ← original raw file
  sample_001.qc_pass    ← written on PASS; downstream proceeds
  sample_001.qc_fail    ← written on WARN or FAIL; downstream blocked

Gate files are JSON so downstream scripts can read the QC outcome and
metrics without querying the database.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from ..qc.base import QCResult, QCStatus


def write_gate_file(raw_path: Path, result: QCResult, stage: str) -> Path:
    """Write a .qc_pass or .qc_fail sidecar next to the raw data file.

    Args:
        raw_path: Path to the raw instrument file.
        result:   QCResult from the module that ran.
        stage:    QC stage identifier embedded in the gate file payload
                  (e.g. "pre_search").

    Returns:
        Path to the written gate file.
    """
    suffix = ".qc_pass" if result.status == QCStatus.PASS else ".qc_fail"
    gate_path = raw_path.with_suffix(suffix)
    gate_path.write_text(
        json.dumps(
            {
                "status": result.status.value,
                "stage": stage,
                "module": result.module,
                "metrics": result.metrics,
                "message": result.message,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
        )
    )
    return gate_path


def read_gate_file(gate_path: Path) -> dict:
    """Parse and return the contents of a gate file."""
    return json.loads(gate_path.read_text())


def get_gate_status(raw_path: Path, stage: str) -> QCStatus | None:
    """Return the QC status from an existing gate file, or None if absent.

    Checks for .qc_pass first, then .qc_fail, matching the given stage.
    """
    for suffix, status in ((".qc_pass", QCStatus.PASS), (".qc_fail", None)):
        gate_path = raw_path.with_suffix(suffix)
        if gate_path.exists():
            data = read_gate_file(gate_path)
            if data.get("stage") == stage:
                if status is QCStatus.PASS:
                    return QCStatus.PASS
                return QCStatus(data["status"])
    return None
