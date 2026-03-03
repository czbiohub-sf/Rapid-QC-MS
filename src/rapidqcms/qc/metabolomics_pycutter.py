"""Metabolomics QC from MS-DIAL + PyCutter alignment exports.

Reads a *PyCutterStep1_Export.txt file (one per polarity per run) and
produces one QCResult per QC sample column.

Checks (all derived from the export file — no DB lookups needed):
    1. IS RT shift   — |deltaRT| per IS vs thresholds (alignment average)
    2. IS m/z shift  — |deltaMZ| per IS vs thresholds (alignment average)
    3. IS Pool %CV   — coefficient of variation across QC pools per IS
    4. IS detection  — fraction of IS detected (non-zero) in this QC sample

IS rows are identified by a "1_" prefix on the Metabolite name column.
QC sample columns are identified by a "QC_" prefix.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .base import QCResult, QCStatus

log = logging.getLogger(__name__)

_DEFAULTS = {
    "rt_shift_warn":  0.3,
    "rt_shift_fail":  0.5,
    "mz_shift_warn":  0.005,
    "mz_shift_fail":  0.010,
    "cv_warn":        30.0,
    "cv_fail":        50.0,
    "fill_warn":      0.95,
    "fill_fail":      0.80,
}


def run_pycutter_qc(
    export_path: Path,
    polarity: str,
    thresholds: dict | None = None,
) -> list[tuple[str, QCResult]]:
    """Run QC on a single PyCutter export file.

    Args:
        export_path: Path to *PyCutterStep1_Export.txt
        polarity:    "Pos" or "Neg"
        thresholds:  Optional dict overriding default thresholds.

    Returns:
        List of (sample_id, QCResult), one per QC sample column.
        Empty list if no QC columns or no IS rows are found.
    """
    t = {**_DEFAULTS, **(thresholds or {})}

    df = pd.read_csv(export_path, sep="\t", skiprows=4, low_memory=False)

    is_rows = df[df["Metabolite name"].str.startswith("1_", na=False)].copy()
    qc_cols = [c for c in df.columns if c.startswith("QC_")]

    if is_rows.empty:
        log.warning("No IS rows (1_ prefix) found in %s", export_path.name)
        return []
    if not qc_cols:
        log.warning("No QC_ columns found in %s", export_path.name)
        return []

    # ── Run-level IS checks (alignment averages — same value for all samples) ──

    def _names(mask) -> list[str]:
        return is_rows.loc[mask, "Metabolite name"].tolist()

    rt = is_rows["deltaRT"].abs()
    rt_warn_names = _names(rt > t["rt_shift_warn"])
    rt_fail_names = _names(rt > t["rt_shift_fail"])

    mz = is_rows["deltaMZ"].abs()
    mz_warn_names = _names(mz > t["mz_shift_warn"])
    mz_fail_names = _names(mz > t["mz_shift_fail"])

    cv = pd.to_numeric(is_rows["Pool %CV"], errors="coerce")
    cv_warn_names = _names(cv > t["cv_warn"])
    cv_fail_names = _names(cv > t["cv_fail"])

    # ── Per-sample checks ──────────────────────────────────────────────────────

    results: list[tuple[str, QCResult]] = []

    for col in qc_cols:
        sample_vals = pd.to_numeric(is_rows[col], errors="coerce")
        n_detected = int((sample_vals > 0).sum())
        fill_fraction = n_detected / len(is_rows)

        fails: list[str] = []
        warnings: list[str] = []
        status = QCStatus.PASS

        # RT shift
        if rt_fail_names:
            status = QCStatus.FAIL
            fails.append(f"IS RT shift >={t['rt_shift_fail']} min: {rt_fail_names}")
        elif rt_warn_names:
            status = QCStatus.WARN
            warnings.append(f"IS RT shift >={t['rt_shift_warn']} min: {rt_warn_names}")

        # m/z shift
        if mz_fail_names:
            status = QCStatus.FAIL
            fails.append(f"IS m/z shift >={t['mz_shift_fail']} Da: {mz_fail_names}")
        elif mz_warn_names and status != QCStatus.FAIL:
            status = QCStatus.WARN
            warnings.append(f"IS m/z shift >={t['mz_shift_warn']} Da: {mz_warn_names}")

        # Pool CV
        if cv_fail_names:
            status = QCStatus.FAIL
            fails.append(f"IS Pool CV >={t['cv_fail']}%: {cv_fail_names}")
        elif cv_warn_names and status != QCStatus.FAIL:
            status = QCStatus.WARN
            warnings.append(f"IS Pool CV >={t['cv_warn']}%: {cv_warn_names}")

        # Per-sample IS detection
        if fill_fraction < t["fill_fail"]:
            status = QCStatus.FAIL
            fails.append(
                f"IS detection {fill_fraction:.1%} < {t['fill_fail']:.0%}"
            )
        elif fill_fraction < t["fill_warn"] and status != QCStatus.FAIL:
            status = QCStatus.WARN
            warnings.append(
                f"IS detection {fill_fraction:.1%} < {t['fill_warn']:.0%}"
            )

        grades: dict = {
            "is_rt_shift": (
                {"status": "Fail", "message": f"IS RT shift ≥{t['rt_shift_fail']} min: {rt_fail_names}"}
                if rt_fail_names else
                {"status": "Warn", "message": f"IS RT shift ≥{t['rt_shift_warn']} min: {rt_warn_names}"}
                if rt_warn_names else
                {"status": "Pass", "message": None}
            ),
            "is_mz_shift": (
                {"status": "Fail", "message": f"IS m/z shift ≥{t['mz_shift_fail']} Da: {mz_fail_names}"}
                if mz_fail_names else
                {"status": "Warn", "message": f"IS m/z shift ≥{t['mz_shift_warn']} Da: {mz_warn_names}"}
                if mz_warn_names else
                {"status": "Pass", "message": None}
            ),
            "is_pool_cv": (
                {"status": "Fail", "message": f"IS Pool CV ≥{t['cv_fail']}%: {cv_fail_names}"}
                if cv_fail_names else
                {"status": "Warn", "message": f"IS Pool CV ≥{t['cv_warn']}%: {cv_warn_names}"}
                if cv_warn_names else
                {"status": "Pass", "message": None}
            ),
            "is_fill_fraction": (
                {"status": "Fail", "message": f"IS detection {fill_fraction:.1%} < {t['fill_fail']:.0%} fail threshold"}
                if fill_fraction < t["fill_fail"] else
                {"status": "Warn", "message": f"IS detection {fill_fraction:.1%} < {t['fill_warn']:.0%} warn threshold"}
                if fill_fraction < t["fill_warn"] else
                {"status": "Pass", "message": None}
            ),
        }

        results.append((
            col,
            QCResult(
                status=status,
                module="metabolomics_pycutter",
                metrics={
                    "polarity":         polarity,
                    "n_is":             len(is_rows),
                    "n_is_detected":    n_detected,
                    "fill_fraction":    round(fill_fraction, 3),
                    "rt_warn_is":       rt_warn_names,
                    "rt_fail_is":       rt_fail_names,
                    "mz_warn_is":       mz_warn_names,
                    "mz_fail_is":       mz_fail_names,
                    "cv_warn_is":       cv_warn_names,
                    "cv_fail_is":       cv_fail_names,
                    "fails":            fails,
                    "warnings":         warnings,
                },
                grades=grades,
            ),
        ))

    return results


def extract_instrument_id(export_path: Path) -> str | None:
    """Infer instrument ID from sample column names in the export.

    Sample columns follow the pattern: QC_Neg_019702_TLG1025_QE2
    The instrument ID is the last _-delimited token.
    Returns None if no suitable column is found.
    """
    try:
        df = pd.read_csv(
            export_path, sep="\t", skiprows=4, nrows=1, low_memory=False
        )
        for col in df.columns:
            if col.startswith("QC_") or col.startswith("BK_"):
                return col.rsplit("_", 1)[-1]
    except Exception:
        log.warning("Could not read instrument ID from %s", export_path.name)
    return None
