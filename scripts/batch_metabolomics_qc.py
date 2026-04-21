#!/usr/bin/env python3
"""Batch metabolomics QC from MS-DIAL alignment matrix exports.

Parses the wide Height_*.txt alignment matrices produced by MS-DIAL,
extracts QC (pooled) sample columns, runs intensity-based QC checks,
persists results to the local SQLite database, and prints a report.

Usage (from repo root):
    python scripts/batch_metabolomics_qc.py
    python scripts/batch_metabolomics_qc.py \\
        --neg data/mzml/neg/processed_raw_data/neg/Height_0_2026121820.txt \\
        --pos data/mzml/neg/processed_raw_data/pos/Height_2_2026121821.txt \\
        --instrument QE2 --run TLG1025

QC checks (per QC sample):
    1. Dropout rate  — % of named features with zero/missing intensity
       Fail: > 20%   Warn: > 10%
    2. Intensity CV  — mean CV across all QC injections for named features
       Fail: > 50%   Warn: > 30%
    3. Intensity ratio — per-feature ratio to QC-group median; fraction of
       features within 2× of median
       Fail: < 70%   Warn: < 85%
"""

import argparse
import csv
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from rapidqcms.db.connection import get_session, init_db
from rapidqcms.db.models import Instrument, Run
from rapidqcms.db.results import write_qc_result
from rapidqcms.qc.base import QCResult, QCStatus

INSTRUMENT_ID = "QE2"
RUN_ID = "TLG1025"
EXPERIMENT_TYPE = "metabolomics"
STAGE = "post_search"

# Thresholds
DROPOUT_FAIL = 20.0   # %
DROPOUT_WARN = 10.0   # %
CV_FAIL = 50.0        # %
CV_WARN = 30.0        # %
RATIO_FAIL = 0.70     # fraction within 2× of median
RATIO_WARN = 0.85

STATUS_SYMBOL = {"Pass": "✓", "Warn": "!", "Fail": "✗"}


def _ensure_instrument_and_run(session) -> None:
    if session.get(Instrument, INSTRUMENT_ID) is None:
        session.add(Instrument(id=INSTRUMENT_ID, name="QE2", experiment_type=EXPERIMENT_TYPE))
    if session.get(Run, RUN_ID) is None:
        session.add(Run(id=RUN_ID, instrument_id=INSTRUMENT_ID, experiment_type=EXPERIMENT_TYPE))
    session.flush()


def _load_alignment(path: Path) -> tuple[pd.DataFrame, list[str]]:
    """Read MS-DIAL alignment matrix; return (df, qc_col_names)."""
    df = pd.read_csv(path, sep="\t", header=4, low_memory=False)
    qc_cols = [c for c in df.columns if str(c).startswith("QC_")]
    return df, qc_cols


def _named_features(df: pd.DataFrame) -> pd.DataFrame:
    """Return rows with a non-Unknown, non-blank metabolite annotation."""
    mask = (
        df["Metabolite name"].notna()
        & (df["Metabolite name"].str.strip() != "")
        & (~df["Metabolite name"].str.startswith("Unknown"))
    )
    return df[mask]


def _qc_check(df_named: pd.DataFrame, qc_cols: list[str], sample_col: str) -> QCResult:
    """Run intensity QC checks for a single QC sample column."""
    intensities = df_named[qc_cols].apply(pd.to_numeric, errors="coerce")
    sample_vals = pd.to_numeric(df_named[sample_col], errors="coerce")

    n_total = len(df_named)

    # ── 1. Dropout ───────────────────────────────────────────────────────────
    n_dropout = int((sample_vals.isna() | (sample_vals == 0)).sum())
    dropout_pct = round(n_dropout / n_total * 100, 1) if n_total else 0.0

    # ── 2. CV across QC samples (per feature, then mean) ────────────────────
    qc_mean = intensities.mean(axis=1).replace(0, np.nan)
    qc_std = intensities.std(axis=1)
    per_feature_cv = (qc_std / qc_mean * 100).replace([np.inf, -np.inf], np.nan)
    mean_cv = round(float(per_feature_cv.dropna().mean()), 1)

    # ── 3. Ratio to QC-group median ─────────────────────────────────────────
    qc_median = intensities.median(axis=1).replace(0, np.nan)
    ratio = (sample_vals / qc_median).replace([np.inf, -np.inf], np.nan).dropna()
    in_range = float(((ratio >= 0.5) & (ratio <= 2.0)).sum()) / len(ratio) if len(ratio) else 1.0

    # ── Status ───────────────────────────────────────────────────────────────
    status = QCStatus.PASS
    fails, warnings = [], []

    if dropout_pct > DROPOUT_FAIL:
        status = QCStatus.FAIL
        fails.append(f"Dropout {dropout_pct}% > {DROPOUT_FAIL}%")
    elif dropout_pct > DROPOUT_WARN:
        status = QCStatus.WARN
        warnings.append(f"Dropout {dropout_pct}% > {DROPOUT_WARN}%")

    if mean_cv > CV_FAIL:
        status = QCStatus.FAIL
        fails.append(f"Mean CV {mean_cv}% > {CV_FAIL}%")
    elif mean_cv > CV_WARN and status != QCStatus.FAIL:
        status = QCStatus.WARN
        warnings.append(f"Mean CV {mean_cv}% > {CV_WARN}%")

    in_range_pct = round(in_range * 100, 1)
    if in_range < RATIO_FAIL:
        status = QCStatus.FAIL
        fails.append(f"Only {in_range_pct}% features within 2× median")
    elif in_range < RATIO_WARN and status != QCStatus.FAIL:
        status = QCStatus.WARN
        warnings.append(f"Only {in_range_pct}% features within 2× median")

    return QCResult(
        status=status,
        module="metabolomics",
        metrics={
            "n_named_features": n_total,
            "dropout_count": n_dropout,
            "dropout_pct": dropout_pct,
            "mean_cv_pct": mean_cv,
            "in_range_pct": in_range_pct,
            "fails": fails,
            "warnings": warnings,
        },
    )


def _process_alignment(path: Path, polarity: str, session, rows: list[dict]) -> int:
    df, qc_cols = _load_alignment(path)
    df_named = _named_features(df)
    n = len(qc_cols)
    print(f"\n{polarity.upper()} — {path.name}")
    print(f"  {len(df)} features total, {len(df_named)} named  |  {n} QC samples")

    for i, col in enumerate(qc_cols, 1):
        sample_id = col
        print(f"  [{i:2d}/{n}] {col} ...", end=" ", flush=True)

        result = _qc_check(df_named, qc_cols, col)

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
        rows.append({
            "sample":        sample_id,
            "polarity":      polarity,
            "status":        result.status.value,
            "n_features":    m["n_named_features"],
            "dropout_pct":   m["dropout_pct"],
            "mean_cv_pct":   m["mean_cv_pct"],
            "in_range_pct":  m["in_range_pct"],
            "fails":         "; ".join(m["fails"]),
            "warnings":      "; ".join(m["warnings"]),
        })
        sym = STATUS_SYMBOL.get(result.status.value, "?")
        print(f"{sym} {result.status.value}  "
              f"(dropout={m['dropout_pct']}%  cv={m['mean_cv_pct']}%  "
              f"in_range={m['in_range_pct']}%)")

    return n


def _print_summary(rows: list[dict]) -> None:
    col_w = max(len(r["sample"]) for r in rows)
    hdr = (f"{'Sample':<{col_w}}  {'Pol':<4}  {'':1}  {'Status':<4}"
           f"  {'N feat':>6}  {'Drop%':>6}  {'CV%':>6}  {'InRng%':>7}  Notes")
    print()
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        sym = STATUS_SYMBOL.get(r["status"], "?")
        notes = r["fails"] or r["warnings"] or ""
        print(f"{r['sample']:<{col_w}}  {r['polarity']:<4}  {sym}  {r['status']:<4}"
              f"  {r['n_features']:>6}  {r['dropout_pct']:>6}  {r['mean_cv_pct']:>6}"
              f"  {r['in_range_pct']:>7}  {notes}")
    print()
    statuses = [r["status"] for r in rows]
    print(f"Total: {len(rows)}   Pass: {statuses.count('Pass')}   "
          f"Warn: {statuses.count('Warn')}   Fail: {statuses.count('Fail')}")


def run(neg_path: Path | None, pos_path: Path | None, output_csv: Path) -> None:
    rows: list[dict] = []

    with get_session() as session:
        _ensure_instrument_and_run(session)

        if neg_path:
            _process_alignment(neg_path, "Neg", session, rows)
        if pos_path:
            _process_alignment(pos_path, "Pos", session, rows)

    if not rows:
        print("No QC samples processed.", file=sys.stderr)
        sys.exit(1)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(output_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    _print_summary(rows)
    print()
    print(f"CSV report → {output_csv}")


def main() -> None:
    base = Path("data/mzml/neg/processed_raw_data")
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--neg", default=str(base / "neg/Height_0_2026121820.txt"),
                        help="Neg alignment matrix txt")
    parser.add_argument("--pos", default=str(base / "pos/Height_2_2026121821.txt"),
                        help="Pos alignment matrix txt")
    parser.add_argument("--output", default="data/metabolomics_qc_report.csv")
    args = parser.parse_args()

    neg_path = Path(args.neg) if args.neg and Path(args.neg).exists() else None
    pos_path = Path(args.pos) if args.pos and Path(args.pos).exists() else None

    if not neg_path and not pos_path:
        print("Error: neither neg nor pos alignment file found.", file=sys.stderr)
        sys.exit(1)

    init_db()
    run(neg_path, pos_path, Path(args.output))


if __name__ == "__main__":
    main()
