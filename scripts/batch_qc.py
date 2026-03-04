#!/usr/bin/env python3
"""Batch proteomics pre-search QC on a directory of mzML files.

Processes every *.mzML found recursively under --dir, writes results to
the local SQLite database, prints a summary table, and writes a CSV report.

Usage (from repo root):
    python scripts/batch_qc.py
    python scripts/batch_qc.py --dir data/mzml --output data/qc_report.csv
"""

import argparse
import csv
import sys
from pathlib import Path

# Allow running from the repo root without `pip install -e .`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from rapidqcms.db.connection import get_engine, get_session, init_db
from rapidqcms.db.models import Instrument, Run
from rapidqcms.db.results import write_qc_result
from rapidqcms.qc.proteomics_pre import ProteomicsPreSearchQCModule

INSTRUMENT_ID = "FL2"
RUN_ID = "AAGI001"
EXPERIMENT_TYPE = "proteomics"
STAGE = "pre_search"

MODULE_CONFIG = {
    "thresholds": {
        "min_ms1_scans": 100,
        "min_ms2_scans": 500,
        "min_ms2_ms1_ratio": 2.0,
    },
    "checks": {
        "ms1_enabled": True,
        "ms2_enabled": True,
        "ratio_enabled": True,
    },
}

STATUS_SYMBOL = {"Pass": "✓", "Warn": "!", "Fail": "✗"}


def _ensure_instrument_and_run(session) -> None:
    if session.get(Instrument, INSTRUMENT_ID) is None:
        session.add(Instrument(id=INSTRUMENT_ID, name="FL2", experiment_type=EXPERIMENT_TYPE))
    if session.get(Run, RUN_ID) is None:
        session.add(Run(id=RUN_ID, instrument_id=INSTRUMENT_ID, experiment_type=EXPERIMENT_TYPE))
    session.flush()


def _infer_type(path: Path) -> str:
    """Classify a file as 'Wash' or 'Sample' based on its path."""
    return "Wash" if "wash" in path.parts[-2].lower() or "wash" in path.stem.lower() else "Sample"


def _print_summary(rows: list[dict]) -> None:
    col_w = max(len(r["sample"]) for r in rows)
    header = (
        f"{'Sample':<{col_w}}  {'Type':<6}  {'':1}  {'Status':<4}"
        f"  {'MS1':>6}  {'MS2':>7}  {'Ratio':>5}  Notes"
    )
    print()
    print(header)
    print("-" * len(header))
    for r in rows:
        sym = STATUS_SYMBOL.get(r["status"], "?")
        notes = r["fails"] or r["warnings"] or ""
        print(
            f"{r['sample']:<{col_w}}  {r['type']:<6}  {sym}  {r['status']:<4}"
            f"  {str(r['ms1_count']):>6}  {str(r['ms2_count']):>7}  {str(r['ms2_ms1_ratio']):>5}  {notes}"
        )
    print()
    statuses = [r["status"] for r in rows]
    n = len(rows)
    print(
        f"Total: {n}   "
        f"Pass: {statuses.count('Pass')}   "
        f"Warn: {statuses.count('Warn')}   "
        f"Fail: {statuses.count('Fail')}"
    )


def run(mzml_dir: Path, output_csv: Path) -> None:
    mzml_files = sorted(mzml_dir.rglob("*.mzML"))
    if not mzml_files:
        print(f"No .mzML files found under {mzml_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(mzml_files)} mzML files — instrument={INSTRUMENT_ID}, run={RUN_ID}")
    print()

    module = ProteomicsPreSearchQCModule.from_config(MODULE_CONFIG)
    rows: list[dict] = []

    with get_session() as session:
        _ensure_instrument_and_run(session)

        for i, path in enumerate(mzml_files, 1):
            sample_id = path.stem
            sample_type = _infer_type(path)
            label = f"[{i:2d}/{len(mzml_files)}] {sample_type:<6} {path.name}"
            print(f"{label} ...", end=" ", flush=True)

            result = module.analyze(
                path,
                context={
                    "instrument_id": INSTRUMENT_ID,
                    "run_id": RUN_ID,
                    "experiment_type": EXPERIMENT_TYPE,
                },
            )

            write_qc_result(
                session=session,
                instrument_id=INSTRUMENT_ID,
                run_id=RUN_ID,
                sample_id=sample_id,
                experiment_type=EXPERIMENT_TYPE,
                qc_stage=STAGE,
                result=result,
            )

            m = result.metrics
            rows.append(
                {
                    "sample": sample_id,
                    "type": sample_type,
                    "status": result.status.value,
                    "ms1_count": m.get("ms1_count", ""),
                    "ms2_count": m.get("ms2_count", ""),
                    "ms2_ms1_ratio": m.get("ms2_ms1_ratio", ""),
                    "fails": "; ".join(m.get("fails", [])),
                    "warnings": "; ".join(m.get("warnings", [])),
                }
            )
            print(f"{STATUS_SYMBOL.get(result.status.value, '?')} {result.status.value}")

    # CSV report
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(output_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    _print_summary(rows)
    print()
    print(f"CSV report → {output_csv}")
    print(f"Results saved to SQLite → {get_engine().url}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--dir",
        default="data/mzml",
        help="Root directory to search for *.mzML files (recursive). Default: data/mzml",
    )
    parser.add_argument(
        "--output",
        default="data/qc_report.csv",
        help="CSV output path. Default: data/qc_report.csv",
    )
    args = parser.parse_args()

    mzml_dir = Path(args.dir)
    if not mzml_dir.exists():
        print(f"Error: directory not found: {mzml_dir}", file=sys.stderr)
        sys.exit(1)

    init_db()
    run(mzml_dir, Path(args.output))


if __name__ == "__main__":
    main()
