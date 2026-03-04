"""New watchdog-based acquisition listener.

Uses only the Phase 1/2 stack — no imports from DatabaseFunctions,
AutoQCProcessing, or the old AcquisitionListener.

Usage
-----
  rapidqcms listen --instrument INST01 --run-id RUN001 --path /data/acq

  # Or programmatically:
  from rapidqcms.service.listener import ListenerConfig, start_listener
  cfg = ListenerConfig(instrument_id="INST01", run_id="RUN001", ...)
  start_listener(cfg, db_engine=engine, storage=backend)
"""

from __future__ import annotations

import hashlib
import logging
import signal
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from watchdog.events import FileCreatedEvent, FileSystemEventHandler
from watchdog.observers import Observer

from ..config.library import (
    chromatography_from_filename,
    get_internal_standards_df,
    get_registered_chromatographies,
)
from ..db.features import get_in_run_rt_history
from ..db.results import update_run_cv, write_qc_result
from ..qc.base import QCStatus
from .gating import write_gate_file
from .processor import process_sample
from .pipeline import _worst_status

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration dataclass
# ---------------------------------------------------------------------------


@dataclass
class ListenerConfig:
    """All configuration for one acquisition-listener session.

    The three run-specific fields (instrument_id, run_id, watch_path) come
    from CLI arguments.  All other fields are deployment-stable and sourced
    from environment variables via ``config.Settings``.
    """

    instrument_id: str
    run_id: str
    watch_path: Path

    extension: str = ".raw"          # file extension to filter on
    experiment_type: str = "metabolomics"
    chromatography: str = "HILIC"
    polarity: str = "Pos"
    stage: str = "pre_search"

    msconvert_exe: Path | None = None
    msdial_exe: Path | None = None
    msdial_params: Path | None = None
    qc_config_id: str | None = None  # None → use qc_modules.toml defaults

    md5_check_interval: int = 180    # seconds between stability checks


# ---------------------------------------------------------------------------
# File-system event handler
# ---------------------------------------------------------------------------


class AcquisitionEventHandler(FileSystemEventHandler):
    """Watchdog event handler that routes completed raw files into the QC pipeline."""

    def __init__(self, config: ListenerConfig, session_factory, storage=None):
        super().__init__()
        self._cfg = config
        self._session_factory = session_factory
        self._storage = storage
        self._active: set[str] = set()
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Watchdog callbacks
    # ------------------------------------------------------------------

    def on_created(self, event: FileCreatedEvent) -> None:
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix.lower() != self._cfg.extension.lower():
            return
        log.info("New file detected: %s", path)
        t = threading.Thread(target=self._handle_file, args=(path,), daemon=True)
        t.start()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _wait_for_stable(self, path: Path) -> bool:
        """Block until the file's MD5 stops changing between consecutive checks.

        Returns True when stable, False if the file disappears.
        """
        prev_md5 = None
        while True:
            if not path.exists():
                log.warning("File vanished while waiting for stability: %s", path)
                return False
            current_md5 = _md5(path)
            if current_md5 == prev_md5:
                log.info("File stable: %s", path)
                return True
            prev_md5 = current_md5
            log.debug("File not yet stable, retrying in %d s: %s", self._cfg.md5_check_interval, path)
            time.sleep(self._cfg.md5_check_interval)

    def _handle_file(self, path: Path) -> None:
        """Full processing pipeline for a single raw file."""
        key = str(path)
        with self._lock:
            if key in self._active:
                return
            self._active.add(key)

        try:
            if not self._wait_for_stable(path):
                return

            # Validate chromatography from filename before doing any work
            file_chrom = chromatography_from_filename(path.stem)
            registered = get_registered_chromatographies()
            if file_chrom is None:
                log.error(
                    "Cannot determine chromatography from filename '%s' — "
                    "no registered method name found (registered: %s). Skipping.",
                    path.name,
                    ", ".join(sorted(registered)) or "none",
                )
                return
            if file_chrom not in registered:
                log.error(
                    "Chromatography '%s' in filename '%s' is not a registered method "
                    "(registered: %s). Skipping.",
                    file_chrom,
                    path.name,
                    ", ".join(sorted(registered)) or "none",
                )
                return
            if file_chrom != self._cfg.chromatography:
                log.warning(
                    "Filename chromatography '%s' differs from configured '%s' for %s. "
                    "Using chromatography from filename.",
                    file_chrom,
                    self._cfg.chromatography,
                    path.name,
                )

            with self._session_factory() as session:
                cfg = self._cfg

                # Build context from DB (use chromatography inferred from filename)
                df_features = get_internal_standards_df(file_chrom, cfg.polarity)
                df_run_rt = get_in_run_rt_history(session, cfg.run_id, cfg.instrument_id)

                context = {
                    "df_features": df_features,
                    "df_run_retention_times": df_run_rt,
                    "polarity": cfg.polarity,
                    "experiment_type": cfg.experiment_type,
                }

                work_dir = path.parent / ".rapidqcms_work"
                results = process_sample(
                    raw_path=path,
                    work_dir=work_dir,
                    instrument_id=cfg.instrument_id,
                    run_id=cfg.run_id,
                    context=context,
                    stage=cfg.stage,
                    msconvert_exe=cfg.msconvert_exe,
                    msdial_exe=cfg.msdial_exe,
                    msdial_params=cfg.msdial_params,
                    storage=self._storage,
                )

                if not results:
                    log.error("No QC results produced for %s", path)
                    return

                # Persist each result
                for r in results:
                    write_qc_result(
                        session,
                        instrument_id=cfg.instrument_id,
                        run_id=cfg.run_id,
                        sample_id=path.stem,
                        experiment_type=cfg.experiment_type,
                        qc_stage=cfg.stage,
                        result=r,
                    )
                session.commit()

                # Update running CV for the run now that a new sample is in
                update_run_cv(session, cfg.run_id, qc_stage=cfg.stage)
                session.commit()

                # Write gate file for the worst outcome
                worst = _worst_status(results)
                from ..qc.base import QCResult
                gate_result = QCResult(
                    status=worst,
                    module="pipeline",
                    metrics={"per_module": {r.module: r.status.value for r in results}},
                )
                gate_path = write_gate_file(path, gate_result, cfg.stage)
                log.info("Gate file written: %s (status=%s)", gate_path, worst.value)

        except Exception:
            log.exception("Unhandled error processing %s", path)
        finally:
            with self._lock:
                self._active.discard(key)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def start_listener(
    config: ListenerConfig,
    db_engine=None,
    storage=None,
) -> None:
    """Start the watchdog observer and block until interrupted.

    Args:
        config:     ListenerConfig built from CLI args + Settings.
        db_engine:  SQLAlchemy Engine.  If None a default engine is created
                    from ``RAPIDQCMS_DB_URL`` (falls back to SQLite).
        storage:    StorageBackend instance, or None to skip uploads.
    """
    if db_engine is None:
        from ..config import get_settings
        s = get_settings()
        db_engine = create_engine(s.db_url)

    from ..db.models import Base
    Base.metadata.create_all(db_engine)

    session_factory = sessionmaker(db_engine)
    handler = AcquisitionEventHandler(config, session_factory, storage=storage)

    observer = Observer()
    observer.schedule(handler, str(config.watch_path), recursive=False)
    observer.start()
    log.info(
        "Listening for *%s files in %s  [instrument=%s  run=%s]",
        config.extension,
        config.watch_path,
        config.instrument_id,
        config.run_id,
    )

    # Graceful shutdown on SIGTERM (e.g. systemd stop)
    def _sigterm_handler(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _sigterm_handler)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down listener.")
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
