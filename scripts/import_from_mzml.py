#!/usr/bin/env python3
"""Import mzML files from data/ into a fresh SQLite DB and run metabolomics_pre QC.

Scans data/<STUDY>/processed_raw_data/{pos,neg}/ for every study directory found
under data/, creates Instrument + Run records as needed, and writes QCResult rows.

Usage (from repo root):
    python scripts/import_from_mzml.py [--data-dir data] [--study TLG1025] [--instrument QE2] [--dry-run]
"""

import argparse
import logging
import sys
from pathlib import Path
import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from rapidqcms.config.library import chromatography_from_filename
from rapidqcms.db.connection import get_session, init_db
from rapidqcms.db.models import Instrument, Run
from rapidqcms.db.results import update_run_cv, upsert_qc_result
from rapidqcms.qc.metabolomics_pre import MetabolomicsPreSearchQCModule

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)


def _polarity_from_path(path: Path) -> str:
    """Infer polarity from parent dir name or filename tokens."""
    parent = path.parent.name.lower()
    if parent == "neg" or parent.endswith("neg"):
        return "Neg"
    if parent == "pos" or parent.endswith("pos"):
        return "Pos"
    stem = path.stem.lower()
    for tok in stem.replace("-", "_").split("_"):
        if tok == "neg":
            return "Neg"
        if tok == "pos":
            return "Pos"
    return "Pos"


def _ensure_instrument(session, instrument_id: str, name: str) -> None:
    if session.get(Instrument, instrument_id) is None:
        session.add(Instrument(id=instrument_id, name=name, experiment_type="metabolomics"))
        session.flush()
        log.info("Created instrument %s", instrument_id)


def _ensure_run(session, run_id: str, instrument_id: str, started_at: datetime.datetime) -> None:
    if session.get(Run, run_id) is None:
        session.add(Run(
            id=run_id,
            instrument_id=instrument_id,
            experiment_type="metabolomics",
            chromatography="HILIC",
            started_at=started_at,
        ))
        session.flush()
        log.info("Created run %s (started_at=%s)", run_id, started_at.date())


def _collect_mzml(data_dir: Path, studies: list[str] | None) -> dict[str, list[Path]]:
    """Return {run_id: [mzml_path, ...]} for studies under data_dir.

    If studies is given, only those study directories are scanned.
    """
    result: dict[str, list[Path]] = {}
    candidates = (
        [data_dir / s for s in studies] if studies else sorted(data_dir.iterdir())
    )
    for study_dir in candidates:
        if not study_dir.is_dir():
            log.warning("Study directory not found: %s", study_dir)
            continue
        prd = study_dir / "processed_raw_data"
        if not prd.is_dir():
            continue
        files = sorted(prd.rglob("*.mzML"))
        if files:
            result[study_dir.name] = files
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-dir", default="data", help="Root data directory (default: data)")
    parser.add_argument("--study", action="append", dest="studies",
                        metavar="STUDY", help="Study ID to import (repeatable); default: all")
    parser.add_argument("--instrument", default="QE2", help="Instrument ID (default: QE2)")
    parser.add_argument("--dry-run", action="store_true", help="Run QC but skip DB writes")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.is_dir():
        log.error("data-dir not found: %s", data_dir)
        sys.exit(1)

    # Initialise DB schema
    if not args.dry_run:
        init_db()
        log.info("DB schema initialised")

    studies = _collect_mzml(data_dir, args.studies)
    if not studies:
        log.error("No mzML files found under %s", data_dir)
        sys.exit(1)

    module = MetabolomicsPreSearchQCModule(config={})

    for run_id, mzml_files in studies.items():
        now = datetime.datetime.now(datetime.timezone.utc)
        log.info("=== %s — %d mzML files ===", run_id, len(mzml_files))

        if not args.dry_run:
            with get_session() as session:
                _ensure_instrument(session, args.instrument, args.instrument)
                _ensure_run(session, run_id, args.instrument, now)

        n_ok = n_fail = 0
        for i, mzml_path in enumerate(mzml_files, 1):
            polarity = _polarity_from_path(mzml_path)
            chrom = chromatography_from_filename(mzml_path.stem) or "HILIC"
            sample_id = mzml_path.stem
            acquired_at = datetime.datetime.now(datetime.timezone.utc)

            log.info("[%d/%d] %s (%s %s)", i, len(mzml_files), sample_id, polarity, chrom)

            try:
                result = module.analyze(mzml_path, {"polarity": polarity, "chromatography": chrom})
            except Exception as exc:
                log.error("  QC failed: %s", exc)
                n_fail += 1
                continue

            m = result.metrics or {}
            log.info(
                "  → %s | detected %s/%s IS",
                result.status.value,
                m.get("n_detected", "?"),
                m.get("n_total", "?"),
            )

            if not args.dry_run:
                with get_session() as session:
                    upsert_qc_result(
                        session,
                        instrument_id=args.instrument,
                        run_id=run_id,
                        sample_id=sample_id,
                        experiment_type="metabolomics",
                        qc_stage="pre_search",
                        result=result,
                        acquired_at=acquired_at,
                    )

            n_ok += 1

        if not args.dry_run:
            with get_session() as session:
                update_run_cv(session, run_id)
            log.info("CV updated for run %s", run_id)

        log.info("Run %s done: ok=%d  failed=%d", run_id, n_ok, n_fail)

    log.info("All done.")


if __name__ == "__main__":
    main()
