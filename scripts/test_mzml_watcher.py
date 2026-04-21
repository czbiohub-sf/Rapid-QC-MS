"""Integration test: mzML watcher with two real HILIC metabolomics files.

Starts the watcher against a fresh run directory, copies in two renamed
HILIC Neg mzML files, then waits for .qc_pass / .qc_fail gate files and
a DB record to appear.

Usage:
    python scripts/test_mzml_watcher.py
"""

import json
import logging
import shutil
import sys
import threading
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging — show INFO from the watcher so we can follow along
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("test_mzml_watcher")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR  = REPO_ROOT / "data"

# Source HILIC Neg files — pick two distinct ones
SOURCES = [
    DATA_DIR / "mzml" / "neg" / "neg" / "BK_Neg_019700_TLG1025_QE2.mzML",
    DATA_DIR / "mzml" / "neg" / "neg" / "BK_Neg_019720_TLG1025_QE2.mzML",
]

# The watch directory becomes the run_id via resolve_run_id()
WATCH_DIR = DATA_DIR / "mzml" / "TLG1025-neg-bk"

# Destination filenames — must contain "HILIC" and "Metabolome" to pass the filter
DEST_NAMES = [
    "BK_HILIC_Metabolome_Neg_S01.mzML",
    "BK_HILIC_Metabolome_Neg_S02.mzML",
]

# Main dashboard DB — results will be visible in the UI immediately
DB_URL = f"sqlite:///{DATA_DIR}/rapidqcms.db"

TIMEOUT_S = 180  # max seconds to wait for both gate files


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # --- Preflight ---
    missing = [p for p in SOURCES if not p.exists()]
    if missing:
        print("ERROR: source files not found:")
        for p in missing:
            print(f"  {p}")
        sys.exit(1)

    # Clean slate for this run
    WATCH_DIR.mkdir(parents=True, exist_ok=True)
    for name in DEST_NAMES:
        dest = WATCH_DIR / name
        if dest.exists():
            dest.unlink()
        for suffix in (".qc_pass", ".qc_fail"):
            sidecar = dest.with_suffix(suffix)
            if sidecar.exists():
                sidecar.unlink()

    db_path = DATA_DIR / "test_run_watcher.db"
    if db_path.exists():
        db_path.unlink()

    # --- Set up DB ---
    from sqlalchemy import create_engine
    from rapidqcms.db.models import Base

    engine = create_engine(DB_URL)
    Base.metadata.create_all(engine)
    log.info("DB: %s", DB_URL)

    # --- Configure watcher ---
    from rapidqcms.service.mzml_watcher import MzmlWatcherConfig, start_mzml_watcher

    cfg = MzmlWatcherConfig(
        watch_path=WATCH_DIR,
        filename_filters=["HILIC", "Metabolome"],
        instrument_id="QE2",
        polarity="Neg",
        chromatography="HILIC",
        md5_check_interval=1,   # short for testing
    )
    log.info("Run ID will be: %s", cfg.resolve_run_id())
    log.info("Watching: %s", cfg.watch_path)
    log.info("Filters: %s", list(cfg.filename_filters))

    # --- Start watcher in daemon thread ---
    watcher_thread = threading.Thread(
        target=start_mzml_watcher,
        kwargs={"config": cfg, "db_engine": engine},
        daemon=True,
        name="mzml-watcher",
    )
    watcher_thread.start()
    time.sleep(1.0)  # let the observer settle before dropping files

    # --- Copy files in ---
    for src, name in zip(SOURCES, DEST_NAMES):
        dest = WATCH_DIR / name
        log.info("Copying %s → %s", src.name, dest.name)
        shutil.copy2(src, dest)

    log.info("Both files copied. Waiting for gate files (timeout=%ds)...", TIMEOUT_S)

    # --- Poll for gate files ---
    dest_paths = [WATCH_DIR / name for name in DEST_NAMES]
    deadline = time.monotonic() + TIMEOUT_S
    done: dict[Path, Path] = {}   # dest_path → gate_path

    while time.monotonic() < deadline:
        for p in dest_paths:
            if p in done:
                continue
            for suffix in (".qc_pass", ".qc_fail"):
                gate = p.with_suffix(suffix)
                if gate.exists():
                    done[p] = gate
                    break
        if len(done) == len(dest_paths):
            break
        time.sleep(1)

    # --- Report ---
    print()
    print("=" * 60)
    print("RESULTS")
    print("=" * 60)

    # Pull DB records now for rich display alongside gate file outcome
    from sqlalchemy.orm import sessionmaker
    from rapidqcms.db.models import QCResult, Run

    run_id = cfg.resolve_run_id()
    db_by_sample: dict[str, QCResult] = {}
    with sessionmaker(engine)() as session:
        for r in session.query(QCResult).filter_by(run_id=run_id).all():
            db_by_sample[r.sample_id] = r

    all_ok = True
    for p in dest_paths:
        gate = done.get(p)
        if gate is None:
            print(f"\n  [TIMEOUT] {p.name}")
            all_ok = False
            continue

        payload = json.loads(gate.read_text())
        status  = payload.get("status", "?")

        print(f"\n  {p.name}")
        print(f"    Gate   : {gate.name}")
        print(f"    Status : {status}")

        # Richer detail from DB record (gate file only carries pipeline-level metrics)
        db_rec = db_by_sample.get(p.stem)
        if db_rec:
            m = db_rec.metrics or {}
            fill  = m.get("fill_fraction")
            n_det = m.get("n_detected")
            n_tot = m.get("n_total")
            if fill is not None:
                print(f"    IS fill: {n_det}/{n_tot}  ({fill:.1%})")
            missing_is = m.get("missing_is", [])
            if missing_is:
                print(f"    Missing IS: {', '.join(missing_is)}")
            rt_devs = m.get("rt_deviations", {})
            if rt_devs:
                worst = max(rt_devs, key=lambda k: abs(rt_devs[k]))
                print(f"    Worst RT dev: {worst} = {rt_devs[worst]:+.3f} min")
            grades = db_rec.grades or {}
            if grades:
                parts = []
                for check, entry in grades.items():
                    short = check[3:] if check.startswith("is_") else check
                    parts.append(f"{short}: {entry.get('status', '?')}")
                print(f"    Per-check: {' | '.join(parts)}")
        else:
            print("    (no DB record found)")
            all_ok = False

    # --- DB summary ---
    print()
    print("-" * 60)
    print("DB summary")
    print("-" * 60)
    with sessionmaker(engine)() as session:
        run = session.get(Run, run_id)
        if run:
            print(f"  Run   : {run.id}  instrument={run.instrument_id}  chrom={run.chromatography}")
        else:
            print("  Run   : NOT FOUND in DB")
            all_ok = False

        results = (
            session.query(QCResult)
            .filter_by(run_id=run_id)
            .order_by(QCResult.acquired_at)
            .all()
        )
        print(f"  QCResult rows: {len(results)}")
        for r in results:
            fill = (r.metrics or {}).get("fill_fraction", "n/a")
            print(f"    sample={r.sample_id}  status={r.status}  fill={fill}")

    print()
    if all_ok and len(done) == len(dest_paths):
        print("PASS — both files processed, gate files written, DB records present.")
    else:
        print("FAIL — see above.")

    # watcher_thread is daemon; exits with us


if __name__ == "__main__":
    main()
