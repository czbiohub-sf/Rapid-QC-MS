"""Watchdog-based watcher that triggers metabolomics pre-search QC on mzML files.

Filters on:
  - Extension  : .mzML  (case-insensitive)
  - Filename   : must contain every token in ``filename_filters``
                 Default: ``["HILIC", "Metabolome"]``

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
    from rapidqcms.service.mzml_watcher import MzmlWatcherConfig, start_mzml_watcher
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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

log = logging.getLogger(__name__)

_DEFAULT_WATCH_PATH = Path("data/mzml")
_DEFAULT_FILTERS: tuple[str, ...] = ("HILIC", "Metabolome")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class MzmlWatcherConfig:
    """All configuration for one mzML watcher session."""

    watch_path: Path
    """Directory to watch for new .mzML files."""

    filename_filters: Sequence[str] = field(
        default_factory=lambda: list(_DEFAULT_FILTERS)
    )
    """Every token must be present in the filename stem to trigger QC."""

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

    chromatography: str = "HILIC"
    """Chromatography method label passed to the QC context."""

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

        filters_raw = os.getenv("RAPIDQCMS_FILENAME_FILTERS", "")
        filters = (
            [t.strip() for t in filters_raw.split(",") if t.strip()]
            or list(_DEFAULT_FILTERS)
        )

        return cls(
            watch_path=path,
            filename_filters=filters,
            instrument_id=os.getenv("RAPIDQCMS_INSTRUMENT_ID", "unknown"),
            run_id=os.getenv("RAPIDQCMS_RUN_ID", "auto"),
            polarity=os.getenv("RAPIDQCMS_POLARITY", "Pos"),
            chromatography=os.getenv("RAPIDQCMS_CHROMATOGRAPHY", "HILIC"),
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
    """Watchdog event handler that filters mzML files and routes them to QC.

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
        """Return True if the file should trigger QC."""
        if path.suffix.lower() != ".mzml":
            return False
        stem = path.stem
        for token in self._cfg.filename_filters:
            if token not in stem:
                log.debug("Skipping %s — missing filter token %r", path.name, token)
                return False
        return True

    # ------------------------------------------------------------------
    # Dispatch to worker thread
    # ------------------------------------------------------------------

    def _consider(self, path: Path) -> None:
        if not self._matches(path):
            return
        log.info("Matched: %s — spawning QC worker", path.name)
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

    def _run_qc(self, mzml_path: Path) -> None:
        from ..qc.base import QCStatus
        from ..service.pipeline import run_qc
        from ..config.library import get_internal_standards

        cfg = self._cfg
        run_id = cfg.resolve_run_id()

        # Load IS for this polarity/chromatography
        is_entries = get_internal_standards(cfg.chromatography, cfg.polarity)
        internal_standards = [
            {"name": e["name"], "precursor_mz": e["mz"], "retention_time": e["rt"]}
            for e in is_entries
        ]

        context = {
            "polarity": cfg.polarity,
            "chromatography": cfg.chromatography,
            "experiment_type": "metabolomics",
            "internal_standards": internal_standards,
        }

        log.info(
            "Running metabolomics pre-search QC on %s  [run=%s  IS=%d]",
            mzml_path.name,
            run_id,
            len(internal_standards),
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

        # Summarise to log
        for r in results:
            log.info("  [%s] %s — %s", r.module, r.status.value, r.message or "")

        worst = max(
            results,
            key=lambda r: {
                "FAIL": 2,
                "WARN": 1,
                "PASS": 0,
            }.get(r.status.value, 0),
        )
        log.info("Overall QC outcome for %s: %s", mzml_path.name, worst.status.value)

        # Persist to DB if a session factory was provided
        if self._session_factory is not None:
            self._persist_results(mzml_path, run_id, results)

    def _persist_results(self, mzml_path: Path, run_id: str, results) -> None:
        from ..db.results import write_qc_result
        from ..db.settings import upsert_instrument, get_run, create_run

        cfg = self._cfg
        try:
            with self._session_factory() as session:
                # Ensure the Instrument row exists
                upsert_instrument(session, cfg.instrument_id, cfg.instrument_id)

                # Ensure the Run row exists — same directory → same run_id → same run
                if get_run(session, run_id) is None:
                    create_run(
                        session,
                        run_id=run_id,
                        instrument_id=cfg.instrument_id,
                        experiment_type="metabolomics",
                        chromatography=cfg.chromatography,
                    )
                    log.info("Created run record: %s / %s", cfg.instrument_id, run_id)

                for r in results:
                    write_qc_result(
                        session,
                        instrument_id=cfg.instrument_id,
                        run_id=run_id,
                        sample_id=mzml_path.stem,
                        experiment_type="metabolomics",
                        qc_stage="pre_search",
                        result=r,
                    )
                session.commit()
        except Exception:
            log.exception("Failed to write QC results to DB for %s", mzml_path.name)


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
        from ..db.models import Base
        from sqlalchemy.orm import sessionmaker

        Base.metadata.create_all(db_engine)
        session_factory = sessionmaker(db_engine)

    handler = MzmlEventHandler(config, session_factory=session_factory)

    # Choose observer type
    if config.use_polling:
        from watchdog.observers.polling import PollingObserver as ObserverClass

        log.info("Using PollingObserver (NFS/Lustre/GPFS mode)")
    else:
        from watchdog.observers import Observer as ObserverClass  # type: ignore[assignment]

    observer = ObserverClass()
    observer.schedule(handler, str(config.watch_path), recursive=False)
    observer.start()

    run_id_display = config.resolve_run_id()
    log.info(
        "Watching %s for *HILIC*Metabolome*.mzML  "
        "[instrument=%s  run=%s  polling=%s]",
        config.watch_path,
        config.instrument_id,
        run_id_display,
        config.use_polling,
    )
    print(
        f"[rapidqcms] Watching {config.watch_path} "
        f"(filters: {list(config.filename_filters)}, polling={config.use_polling})"
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
