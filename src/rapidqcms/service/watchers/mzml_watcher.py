"""Watchdog-based watcher that triggers pre-search QC on mzML files.

Experiment type is determined automatically from the filename:
  - "HILIC" in stem  → metabolomics  (chromatography = HILIC)
  - "Lipid" in stem  → lipidomics    (chromatography derived from filename)
  - anything else    → proteomics

All .mzML files in the watch directory are picked up; the filename alone
decides which QC module runs.

Watch path resolution (first match wins):
  1. Argument passed to ``start_mzml_watcher()``
  2. ``RAPIDQCMS_MZML_WATCH_PATH`` environment variable
  3. ``./data/mzml``  (local dev default)

HPC / SLURM notes
-----------------
On network filesystems (NFS, Lustre, GPFS) inotify events are unreliable.
Set ``RAPIDQCMS_USE_POLLING=1`` (or pass ``use_polling=True``) to switch to a
stat-based ``PollingObserver`` instead of the OS-native one.

SLURM sends ``SIGTERM`` before ``SIGKILL`` — the watcher catches it and shuts
down cleanly, finishing any in-flight QC job before exiting.

See ``scripts/slurm_mzml_watch.sh`` for a ready-made SLURM batch template.

Usage
-----
# CLI (see __main__.py):
    rapidqcms mzml-watch --path ./data/mzml

# Programmatic:
    from rapidqcms.service.watchers.mzml_watcher import MzmlWatcherConfig, start_mzml_watcher
    cfg = MzmlWatcherConfig.from_env(watch_path=Path("./data/mzml"))
    start_mzml_watcher(cfg)
"""

from __future__ import annotations

import hashlib
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)

_DEFAULT_WATCH_PATH = Path("data/mzml")


# ---------------------------------------------------------------------------
# Experiment-type classification
# ---------------------------------------------------------------------------


def _classify_experiment(stem: str) -> tuple[str, str | None]:
    """Derive (experiment_type, chromatography) from an mzML filename stem.

    Rules (first match wins):
      - "HILIC" in stem  → metabolomics, chromatography=HILIC
      - "Lipid" in stem  → lipidomics,   chromatography=None
      - otherwise        → proteomics,   chromatography=None
    """
    if "HILIC" in stem:
        return "metabolomics", "HILIC"
    if "Lipid" in stem:
        return "lipidomics", None
    return "proteomics", None


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class MzmlWatcherConfig:
    """All configuration for one mzML watcher session."""

    watch_path: Path
    """Directory to watch for new .mzML files."""

    instrument_id: str = "unknown"
    """Written to the DB and used as a storage-key prefix."""

    run_id: str = "auto"
    """
    Run identifier written to the DB.
    ``"auto"`` → derive from ``SLURM_JOB_ID`` env var, then from the
    watch_path parent directory name.
    """

    polarity: str = "Pos"
    """Ion polarity passed to the QC module (``"Pos"`` or ``"Neg"``)."""

    use_polling: bool = False
    """
    Force a stat-based ``PollingObserver`` instead of the OS-native observer.
    Required on NFS / Lustre / GPFS where inotify events are not delivered.
    """

    md5_check_interval: int = 30
    """Seconds between consecutive MD5 checks while waiting for file stability."""

    @classmethod
    def from_env(cls, watch_path: Path | None = None) -> "MzmlWatcherConfig":
        """Build config from environment variables, with optional path override."""
        path = watch_path or Path(
            os.getenv("RAPIDQCMS_MZML_WATCH_PATH", str(_DEFAULT_WATCH_PATH))
        )

        return cls(
            watch_path=path,
            instrument_id=os.getenv("RAPIDQCMS_INSTRUMENT_ID", "unknown"),
            run_id=os.getenv("RAPIDQCMS_RUN_ID", "auto"),
            polarity=os.getenv("RAPIDQCMS_POLARITY", "Pos"),
            use_polling=os.getenv("RAPIDQCMS_USE_POLLING", "0") == "1",
            md5_check_interval=int(os.getenv("RAPIDQCMS_MD5_INTERVAL", "30")),
        )

    def resolve_run_id(self) -> str:
        """Return a concrete run_id string (never ``"auto"``)."""
        if self.run_id != "auto":
            return self.run_id
        # Prefer the SLURM job ID when running on HPC
        slurm_id = os.getenv("SLURM_JOB_ID")
        if slurm_id:
            return f"slurm_{slurm_id}"
        # Fall back to the name of the directory being watched
        return self.watch_path.resolve().name


# ---------------------------------------------------------------------------
# Event handler
# ---------------------------------------------------------------------------


class MzmlEventHandler:
    """Watchdog event handler that routes mzML files to the appropriate QC module.

    Handles both ``FileCreatedEvent`` and ``FileMovedEvent`` so that files
    written via an atomic temp-then-rename pattern are also caught.
    """

    def __init__(self, config: MzmlWatcherConfig, session_factory=None):
        self._cfg = config
        self._session_factory = session_factory
        self._active: set[str] = set()
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Watchdog callbacks
    # ------------------------------------------------------------------

    def dispatch(self, event) -> None:
        """Called by watchdog for every filesystem event."""
        from watchdog.events import FileCreatedEvent, FileMovedEvent

        if event.is_directory:
            return
        if isinstance(event, FileCreatedEvent):
            self._consider(Path(event.src_path))
        elif isinstance(event, FileMovedEvent):
            # Atomic rename: the destination name is the final filename
            self._consider(Path(event.dest_path))

    # ------------------------------------------------------------------
    # Filter
    # ------------------------------------------------------------------

    def _matches(self, path: Path) -> bool:
        """Return True if the file should trigger QC (any .mzML file)."""
        return path.suffix.lower() == ".mzml"

    # ------------------------------------------------------------------
    # Dispatch to worker thread
    # ------------------------------------------------------------------

    def _consider(self, path: Path) -> None:
        if not self._matches(path):
            return
        experiment_type, _ = _classify_experiment(path.stem)
        log.info("Matched: %s (%s) — spawning QC worker", path.name, experiment_type)
        t = threading.Thread(target=self._handle_file, args=(path,), daemon=True)
        t.start()

    # ------------------------------------------------------------------
    # Per-file processing
    # ------------------------------------------------------------------

    def _handle_file(self, path: Path) -> None:
        key = str(path)
        with self._lock:
            if key in self._active:
                log.debug("Already processing %s — skipping duplicate event", path.name)
                return
            self._active.add(key)

        try:
            if not self._wait_for_stable(path):
                return
            self._run_qc(path)
        except Exception:
            log.exception("Unhandled error processing %s", path)
        finally:
            with self._lock:
                self._active.discard(key)

    def _wait_for_stable(self, path: Path) -> bool:
        """Block until the file's MD5 is unchanged across two consecutive checks."""
        prev_md5: str | None = None
        while True:
            if not path.exists():
                log.warning("File disappeared while waiting for stability: %s", path)
                return False
            current_md5 = _md5(path)
            if current_md5 == prev_md5:
                log.info("File stable: %s", path.name)
                return True
            prev_md5 = current_md5
            log.debug(
                "File not yet stable, rechecking in %d s: %s",
                self._cfg.md5_check_interval,
                path.name,
            )
            time.sleep(self._cfg.md5_check_interval)

    def _derive_run_id(self, mzml_path: Path) -> str:
        """Return run_id from the file's path.

        When the watch directory has a ``<run_id>/...`` layout (e.g. after
        rsync), the first subdirectory component is used as the run_id.
        Falls back to ``config.resolve_run_id()`` for flat directories or
        when an explicit run_id was configured.
        """
        if self._cfg.run_id != "auto":
            return self._cfg.resolve_run_id()
        try:
            rel = mzml_path.relative_to(self._cfg.watch_path)
            if len(rel.parts) >= 2:
                return rel.parts[0]
        except ValueError:
            pass
        return self._cfg.resolve_run_id()

    def _run_qc(self, mzml_path: Path) -> None:
        from ..pipeline import run_qc

        cfg = self._cfg
        run_id = self._derive_run_id(mzml_path)

        experiment_type, chromatography = _classify_experiment(mzml_path.stem)

        context: dict = {
            "polarity": cfg.polarity,
            "experiment_type": experiment_type,
        }

        if chromatography is not None:
            from ...config.library import get_internal_standards
            context["chromatography"] = chromatography
            is_entries = get_internal_standards(chromatography, cfg.polarity)
            context["internal_standards"] = [
                {"name": e["name"], "precursor_mz": e["mz"], "retention_time": e["rt"]}
                for e in is_entries
            ]

        is_count = len(context.get("internal_standards", []))
        log.info(
            "Running %s pre-search QC on %s  [run=%s%s]",
            experiment_type,
            mzml_path.name,
            run_id,
            f"  IS={is_count}" if chromatography else "",
        )

        # run_qc also writes the gate file (.qc_pass / .qc_fail)
        results = run_qc(
            input_path=mzml_path,
            context=context,
            stage="pre_search",
        )

        if not results:
            log.warning("No QC results produced for %s", mzml_path.name)
            return

        for r in results:
            log.info("  [%s] %s — %s", r.module, r.status.value, r.message or "")

        worst = max(
            results,
            key=lambda r: {"FAIL": 2, "WARN": 1, "PASS": 0}.get(r.status.value, 0),
        )
        log.info("Overall QC outcome for %s: %s", mzml_path.name, worst.status.value)

        if self._session_factory is not None:
            self._persist_results(mzml_path, run_id, experiment_type, chromatography, results)

    def _persist_results(
        self,
        mzml_path: Path,
        run_id: str,
        experiment_type: str,
        chromatography: str | None,
        results,
    ) -> None:
        from ...db.results import write_qc_result
        from ...db.settings import upsert_instrument, get_run, create_run

        cfg = self._cfg
        try:
            with self._session_factory() as session:
                # Ensure the Instrument row exists
                upsert_instrument(session, cfg.instrument_id, cfg.instrument_id)

                # Ensure the Run row exists
                if get_run(session, run_id) is None:
                    create_run(
                        session,
                        run_id=run_id,
                        instrument_id=cfg.instrument_id,
                        experiment_type=experiment_type,
                        chromatography=chromatography,
                    )
                    log.info("Created run record: %s / %s", cfg.instrument_id, run_id)

                for r in results:
                    write_qc_result(
                        session,
                        instrument_id=cfg.instrument_id,
                        run_id=run_id,
                        sample_id=mzml_path.stem,
                        experiment_type=experiment_type,
                        qc_stage="pre_search",
                        result=r,
                    )
                session.commit()
        except Exception:
            log.exception("Failed to write QC results to DB for %s", mzml_path.name)


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------


_BACKFILL_WORKERS = 4  # concurrent QC jobs during backfill


def _backfill(handler: MzmlEventHandler, watch_path: Path) -> None:
    """Process any existing .mzML files that have not yet been QC'd.

    A file is considered already processed if a ``.qc_pass`` or ``.qc_fail``
    sidecar exists next to it.  Files are processed in batches using a bounded
    thread pool (``_BACKFILL_WORKERS``) so the machine isn't overwhelmed.
    The pool runs in a background daemon thread so the watcher starts
    immediately and live events are handled while backfill is in progress.
    """
    from ..events.gating import get_gate_status
    from concurrent.futures import ThreadPoolExecutor
    import threading

    candidates = sorted(watch_path.rglob("*.mzML"))
    if not candidates:
        return

    pending = [p for p in candidates if get_gate_status(p, "pre_search") is None]
    if not pending:
        log.info("Backfill: all %d existing mzML file(s) already processed.", len(candidates))
        return

    log.info(
        "Backfill: %d of %d existing mzML file(s) need QC — starting (workers=%d).",
        len(pending),
        len(candidates),
        _BACKFILL_WORKERS,
    )
    print(
        f"[rapidqcms] Backfilling {len(pending)} unprocessed mzML file(s) "
        f"({_BACKFILL_WORKERS} at a time)…"
    )

    def _run_stable(path: Path) -> None:
        """Like _handle_file but skips the stability wait (file already exists)."""
        key = str(path)
        with handler._lock:
            if key in handler._active:
                return
            handler._active.add(key)
        try:
            handler._run_qc(path)
        except Exception:
            log.exception("Backfill: unhandled error processing %s", path)
        finally:
            with handler._lock:
                handler._active.discard(key)

    def _run_pool() -> None:
        done = 0
        with ThreadPoolExecutor(max_workers=_BACKFILL_WORKERS) as pool:
            for future in pool.map(_run_stable, pending):
                done += 1
                if done % 10 == 0:
                    log.info("Backfill progress: %d / %d", done, len(pending))
        log.info("Backfill complete: %d file(s) processed.", len(pending))
        print(f"[rapidqcms] Backfill complete: {len(pending)} file(s) processed.")

    threading.Thread(target=_run_pool, daemon=True, name="backfill-pool").start()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def start_mzml_watcher(
    config: MzmlWatcherConfig,
    db_engine=None,
) -> None:
    """Start the mzML watcher and block until interrupted.

    Args:
        config:     ``MzmlWatcherConfig`` (typically from ``from_env()``).
        db_engine:  Optional SQLAlchemy engine for persisting QC results.
                    Pass ``None`` (the default) to run gate-file-only mode.
    """
    # Ensure the watch directory exists
    config.watch_path.mkdir(parents=True, exist_ok=True)

    session_factory = None
    if db_engine is not None:
        from ...db.models import Base
        from sqlalchemy.orm import sessionmaker

        Base.metadata.create_all(db_engine)
        session_factory = sessionmaker(db_engine)

    handler = MzmlEventHandler(config, session_factory=session_factory)

    # Backfill: process any .mzML files already present that have no gate file
    _backfill(handler, config.watch_path)

    # Choose observer type
    if config.use_polling:
        from watchdog.observers.polling import PollingObserver as ObserverClass

        log.info("Using PollingObserver (NFS/Lustre/GPFS mode)")
    else:
        from watchdog.observers import Observer as ObserverClass  # type: ignore[assignment]

    observer = ObserverClass()
    observer.schedule(handler, str(config.watch_path), recursive=True)
    observer.start()

    run_id_display = config.resolve_run_id()
    log.info(
        "Watching %s for *.mzML  [instrument=%s  run=%s  polling=%s]",
        config.watch_path,
        config.instrument_id,
        run_id_display,
        config.use_polling,
    )
    print(
        f"[rapidqcms] Watching {config.watch_path} for *.mzML  "
        f"(HILIC→metabolomics, Lipid→lipidomics, other→proteomics, "
        f"polling={config.use_polling})"
    )

    # Graceful shutdown on SIGTERM (systemd / SLURM scancel).
    # signal.signal only works on the main thread; skip silently otherwise
    # (e.g. when start_mzml_watcher is called from a test thread).
    def _sigterm(signum, frame):
        raise KeyboardInterrupt

    try:
        signal.signal(signal.SIGTERM, _sigterm)
    except ValueError:
        log.debug("SIGTERM handler not installed (not main thread)")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutdown signal received — stopping watcher.")
        print("[rapidqcms] Shutting down cleanly.")
    finally:
        observer.stop()
        observer.join()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()
