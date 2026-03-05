"""Always-on root directory watcher.

Watches a single root path (e.g. /hpc/projects/mass_spec/projects) for new
.raw files. On startup it reads lab_config.toml, syncs instruments to the DB,
then enters the watch loop.

Trigger rules:
    proteomics    — *.raw  (one file per sample; instrument ID from filename stem)

Run ID     = name of the immediate subdirectory under the root.
Instrument = trailing _SUFFIX token of the filename stem.

Unknown instruments cause a hard error logged at ERROR level — register
them in lab_config.toml before running the watcher.

Usage:
    rapidqcms watch --path /hpc/projects/mass_spec/projects \\
                    --config lab_config.toml
"""

from __future__ import annotations

import hashlib
import logging
import signal
import threading
import time
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from watchdog.events import FileCreatedEvent, FileSystemEventHandler
from watchdog.observers import Observer

from ...config.lab_config import load_lab_config, sync_to_db
from ...db.models import Instrument
from ...db.results import write_qc_result
from ...db.settings import create_run, get_run

log = logging.getLogger(__name__)

_STABILITY_INTERVAL = 30   # seconds between MD5 checks for file stability


# ---------------------------------------------------------------------------
# Event handler
# ---------------------------------------------------------------------------


class RootEventHandler(FileSystemEventHandler):
    """Handles file creation events anywhere under the root watch path."""

    def __init__(self, root_path: Path, session_factory, config: dict):
        super().__init__()
        self._root = root_path
        self._session_factory = session_factory
        self._config = config
        self._active: set[str] = set()
        self._lock = threading.Lock()

    # ------------------------------------------------------------------

    def on_created(self, event: FileCreatedEvent) -> None:
        if event.is_directory:
            return
        path = Path(event.src_path)

        if path.suffix.lower() == ".raw":
            self._dispatch(path, self._handle_raw)

    def _dispatch(self, path: Path, handler) -> None:
        key = str(path)
        with self._lock:
            if key in self._active:
                return
            self._active.add(key)
        t = threading.Thread(target=self._run_handler, args=(path, handler), daemon=True)
        t.start()

    def _run_handler(self, path: Path, handler) -> None:
        try:
            handler(path)
        except Exception:
            log.exception("Unhandled error processing %s", path)
        finally:
            with self._lock:
                self._active.discard(str(path))

    # ------------------------------------------------------------------
    # Proteomics (.raw file, pre-search)
    # ------------------------------------------------------------------

    def _handle_raw(self, path: Path) -> None:
        run_id = self._run_id_from_path(path)
        if run_id is None:
            log.error("Cannot determine run ID for %s — file not in a run subdir", path)
            return

        instrument_id = path.stem.rsplit("_", 1)[-1]

        with self._session_factory() as session:
            inst = self._require_instrument(session, instrument_id, path)
            if inst is None:
                return
            self._ensure_run(session, run_id, instrument_id, "proteomics")
            session.commit()

        if not self._wait_for_stable(path):
            return

        # Import here to avoid circular imports at module level
        from ..processor import process_sample
        from ...config import get_settings

        s = get_settings()
        work_dir = path.parent / ".rapidqcms_work"
        results = process_sample(
            raw_path=path,
            work_dir=work_dir,
            instrument_id=instrument_id,
            run_id=run_id,
            context={"experiment_type": "proteomics"},
            stage="pre_search",
            msconvert_exe=s.msconvert_exe,
            msdial_exe=None,
            msdial_params=None,
        )

        if not results:
            log.error("No QC results produced for %s", path.name)
            return

        with self._session_factory() as session:
            for result in results:
                write_qc_result(
                    session,
                    instrument_id=instrument_id,
                    run_id=run_id,
                    sample_id=path.stem,
                    experiment_type="proteomics",
                    qc_stage="pre_search",
                    result=result,
                )
            session.commit()

        log.info("Proteomics QC complete: run=%s file=%s", run_id, path.name)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _run_id_from_path(self, path: Path) -> str | None:
        """Return subdir name if path is exactly one level below root."""
        try:
            rel = path.relative_to(self._root)
        except ValueError:
            return None
        if len(rel.parts) == 2:  # subdir/filename
            return rel.parts[0]
        return None

    def _require_instrument(self, session, instrument_id: str, path: Path) -> Instrument | None:
        inst = session.get(Instrument, instrument_id)
        if inst is None:
            log.error(
                "Unknown instrument '%s' (from %s). "
                "Add it to lab_config.toml and restart the watcher.",
                instrument_id,
                path.name,
            )
        return inst

    def _ensure_run(self, session, run_id: str, instrument_id: str, experiment_type: str):
        if get_run(session, run_id) is None:
            create_run(session, run_id, instrument_id, experiment_type)
            log.info("Created run: %s (instrument=%s)", run_id, instrument_id)

    def _wait_for_stable(self, path: Path) -> bool:
        """Block until file MD5 is unchanged between two consecutive checks."""
        prev = None
        while True:
            if not path.exists():
                log.warning("File disappeared while waiting for stability: %s", path)
                return False
            current = _md5(path)
            if current == prev:
                log.info("File stable: %s", path.name)
                return True
            prev = current
            log.debug("File not yet stable, rechecking in %ds: %s", _STABILITY_INTERVAL, path.name)
            time.sleep(_STABILITY_INTERVAL)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def start_watcher(
    root_path: Path,
    config: dict,
    db_engine=None,
) -> None:
    """Load config, sync DB, then watch root_path until interrupted.

    Args:
        root_path:  Root directory to watch (e.g. /hpc/projects/mass_spec/projects).
        config:     Parsed lab_config dict (from load_lab_config()).
        db_engine:  SQLAlchemy Engine. Defaults to RAPIDQCMS_DB_URL env var.
    """
    if db_engine is None:
        from ...config import get_settings
        db_engine = create_engine(get_settings().db_url)

    from ...db.models import Base
    Base.metadata.create_all(db_engine)

    session_factory = sessionmaker(db_engine)

    # Sync config to DB before watching
    with session_factory() as session:
        sync_to_db(config, session)
        session.commit()

    handler = RootEventHandler(root_path, session_factory, config)
    observer = Observer()
    observer.schedule(handler, str(root_path), recursive=True)
    observer.start()

    log.info("Watching %s for new runs", root_path)

    def _sigterm(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _sigterm)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down watcher.")
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
