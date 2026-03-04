#!/usr/bin/env python3
"""Backfill metabolomics_pre QC results with per-IS details.

Re-runs MetabolomicsPreSearchQCModule on existing mzML files and
upserts the results into the DB so the dashboard plots have data.

Usage (from repo root):
    python scripts/reprocess_metabolomics_pre.py [--dry-run]

Resolves mzML paths by looking for <sample_id>.mzML in a list of
known directories.  Skips samples whose mzML file is not found.
"""

import argparse
import logging
import sys
from pathlib import Path

# Ensure src/ is importable when run directly
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rapidqcms.config.library import chromatography_from_filename
from rapidqcms.db.connection import get_session
from rapidqcms.db.models import QCResult as QCResultModel, Run
from rapidqcms.db.results import update_run_cv, upsert_qc_result
from rapidqcms.qc.metabolomics_pre import MetabolomicsPreSearchQCModule

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# All directories to search for mzML files (relative to repo root)
MZML_SEARCH_DIRS = [
    Path("data/data/mzml/MILA024/pos"),
    Path("data/data/mzml/MILA024/neg"),
    Path("data/mzml/pos/pos"),
    Path("data/mzml/neg/neg"),
    Path("data/mzml"),
]


def _find_mzml(sample_id: str) -> Path | None:
    for d in MZML_SEARCH_DIRS:
        p = d / f"{sample_id}.mzML"
        if p.exists():
            return p
    return None


def _polarity_from_stem(stem: str) -> str:
    low = stem.lower()
    if "_pos_" in low or "_pos" in low.rsplit("_", 1)[-1].lower() or "pos" in stem.split("_"):
        return "Pos"
    if "_neg_" in low or "_neg" in low.rsplit("_", 1)[-1].lower() or "neg" in stem.split("_"):
        return "Neg"
    # Fallback: check individual tokens
    for tok in stem.replace("-", "_").split("_"):
        if tok.lower() == "pos":
            return "Pos"
        if tok.lower() == "neg":
            return "Neg"
    return "Pos"


def main():
    parser = argparse.ArgumentParser(description="Reprocess metabolomics_pre QC")
    parser.add_argument("--dry-run", action="store_true", help="Parse and run QC but don't write to DB")
    parser.add_argument("--run-id", help="Only reprocess this run_id (default: all metabolomics runs)")
    args = parser.parse_args()

    module = MetabolomicsPreSearchQCModule(config={})

    with get_session() as session:
        q = session.query(QCResultModel).join(Run, QCResultModel.run_id == Run.id)
        q = q.filter(Run.experiment_type == "metabolomics")
        q = q.filter(QCResultModel.qc_stage == "pre_search")
        if args.run_id:
            q = q.filter(QCResultModel.run_id == args.run_id)

        # Collect all fields while session is open
        job_list = [
            {
                "instrument_id": r.instrument_id,
                "run_id": r.run_id,
                "sample_id": r.sample_id,
                "experiment_type": r.experiment_type,
                "qc_stage": r.qc_stage,
                "acquired_at": r.acquired_at,
            }
            for r in q.order_by(QCResultModel.run_id, QCResultModel.sample_id).all()
        ]

    log.info("Found %d pre_search QC results to reprocess", len(job_list))

    n_ok = n_skip = n_fail = 0

    for row in job_list:
        sample_id = row["sample_id"]
        mzml_path = _find_mzml(sample_id)

        if mzml_path is None:
            log.warning("mzML not found for %s — skipping", sample_id)
            n_skip += 1
            continue

        chrom = chromatography_from_filename(mzml_path.stem) or "HILIC"
        polarity = _polarity_from_stem(mzml_path.stem)

        context = {
            "polarity": polarity,
            "chromatography": chrom,
        }

        log.info("Processing %s (run=%s, chrom=%s, pol=%s)", sample_id, row["run_id"], chrom, polarity)

        try:
            result = module.analyze(mzml_path, context)
        except Exception as exc:
            log.error("QC failed for %s: %s", sample_id, exc)
            n_fail += 1
            continue

        log.info(
            "  → %s | IS detected %s/%s | details rows: %d",
            result.status.value,
            result.metrics.get("n_detected", "?") if result.metrics else "?",
            result.metrics.get("n_total", "?") if result.metrics else "?",
            len(result.details) if result.details else 0,
        )

        if not args.dry_run:
            with get_session() as session:
                upsert_qc_result(
                    session,
                    instrument_id=row["instrument_id"],
                    run_id=row["run_id"],
                    sample_id=sample_id,
                    experiment_type=row["experiment_type"],
                    qc_stage=row["qc_stage"],
                    result=result,
                    acquired_at=row["acquired_at"],
                )
                session.commit()

        n_ok += 1

    # After all samples are processed, recompute CV per run
    if not args.dry_run:
        processed_runs = {row["run_id"] for row in job_list}
        for run_id in processed_runs:
            with get_session() as session:
                update_run_cv(session, run_id)
                session.commit()
            log.info("CV updated for run %s", run_id)

    log.info("Done. ok=%d  skipped=%d  failed=%d", n_ok, n_skip, n_fail)
    if args.dry_run:
        log.info("(dry-run — no DB changes written)")


if __name__ == "__main__":
    main()
