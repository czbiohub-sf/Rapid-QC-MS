#!/usr/bin/env python3
"""Quick local test: run pre-search QC on a directory of mzML files.

Bypasses the file-watcher and MD5 stability check so you can test
against pre-existing mzML files without copying them into a watched folder.

Usage (from repo root):
    # Metabolomics positive mode (IS detection QC)
    python scripts/test_mzml_qc.py \\
        --path data/mzml/pos/pos \\
        --instrument QE2 --run TLG1025-pos --polarity Pos

    # Metabolomics negative mode
    python scripts/test_mzml_qc.py \\
        --path data/mzml/neg/neg \\
        --instrument QE2 --run TLG1025-neg --polarity Neg

    # Proteomics (scan count QC)
    python scripts/test_mzml_qc.py \\
        --path data/mzml/pos/pos --experiment-type proteomics

    # Limit to first N files for a quick smoke test
    python scripts/test_mzml_qc.py \\
        --path data/mzml/pos/pos --limit 5
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from rapidqcms.db.connection import get_session, init_db
from rapidqcms.db.models import Instrument, Run
from rapidqcms.db.results import write_qc_result
from rapidqcms.qc.base import QCStatus
from rapidqcms.service.processor import process_sample


def main():
    parser = argparse.ArgumentParser(description="Run pre-search QC on a directory of mzML files")
    parser.add_argument("--path", required=True, help="Directory containing .mzML files")
    parser.add_argument("--instrument", default="QE2", help="Instrument ID")
    parser.add_argument("--run", default="test-run", help="Run / job ID")
    parser.add_argument("--polarity", default="Pos", choices=["Pos", "Neg"])
    parser.add_argument("--experiment-type", default="metabolomics",
                        choices=["metabolomics", "proteomics"])
    parser.add_argument("--chromatography", default="HILIC",
                        help="Chromatography method (default: HILIC)")
    parser.add_argument("--limit", type=int, default=None, help="Max files to process")
    args = parser.parse_args()

    watch_path = Path(args.path).resolve()
    if not watch_path.is_dir():
        print(f"ERROR: {watch_path} is not a directory")
        sys.exit(1)

    mzml_files = sorted(watch_path.glob("*.mzML"))
    if not mzml_files:
        print(f"No .mzML files found in {watch_path}")
        sys.exit(1)

    if args.limit:
        mzml_files = mzml_files[: args.limit]

    print(f"Instrument:      {args.instrument}")
    print(f"Run:             {args.run}")
    print(f"Polarity:        {args.polarity}")
    print(f"Exp type:        {args.experiment_type}")
    print(f"Chromatography:  {args.chromatography}")
    print(f"Files:           {len(mzml_files)}")
    print()

    init_db()

    with get_session() as session:
        # Ensure instrument and run exist
        if not session.get(Instrument, args.instrument):
            session.add(Instrument(id=args.instrument, name=args.instrument,
                                   experiment_type=args.experiment_type))
        if not session.get(Run, args.run):
            session.add(Run(id=args.run, instrument_id=args.instrument,
                            experiment_type=args.experiment_type, status="active"))
        session.commit()

    context = {
        "experiment_type":  args.experiment_type,
        "polarity":         args.polarity,
        "chromatography":   args.chromatography,
        "df_features": None,
        "df_run_retention_times": None,
    }

    work_dir = watch_path / ".rapidqcms_work"
    results_summary = []

    for mzml in mzml_files:
        print(f"  Processing {mzml.name} ...", end=" ", flush=True)
        results = process_sample(
            raw_path=mzml,
            work_dir=work_dir,
            instrument_id=args.instrument,
            run_id=args.run,
            context=context,
            stage="pre_search",
            # msconvert_exe=None, msdial_exe=None — skipped
        )

        if not results:
            print("SKIP (no modules ran)")
            continue

        with get_session() as session:
            for r in results:
                write_qc_result(
                    session,
                    instrument_id=args.instrument,
                    run_id=args.run,
                    sample_id=mzml.stem,
                    experiment_type=args.experiment_type,
                    qc_stage="pre_search",
                    result=r,
                )
            session.commit()

        statuses = [r.status.value for r in results]
        print(" | ".join(f"{r.module}={r.status.value}" for r in results))
        results_summary.append((mzml.stem, statuses))

    print()
    total = len(results_summary)
    passes = sum(1 for _, ss in results_summary if all(s == "Pass" for s in ss))
    warns  = sum(1 for _, ss in results_summary if any(s == "Warn" for s in ss))
    fails  = sum(1 for _, ss in results_summary if any(s == "Fail" for s in ss))
    print(f"Done: {total} samples — Pass={passes}, Warn={warns}, Fail={fails}")
    print(f"\nOpen the dashboard to see results for run '{args.run}'.")


if __name__ == "__main__":
    main()
