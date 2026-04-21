"""Lab configuration: TOML loader and DB sync.

Reads lab_config.toml and upserts instruments into the database.
Called by the watcher on startup before entering the watch loop.
"""

from __future__ import annotations

import logging
import tomllib
from pathlib import Path

from sqlalchemy.orm import Session

from rapidqcms.db.models import Instrument

log = logging.getLogger(__name__)


def load_lab_config(path: Path) -> dict:
    """Read and parse a lab_config.toml file."""
    with open(path, "rb") as f:
        return tomllib.load(f)


def sync_to_db(config: dict, session: Session) -> None:
    """Upsert instruments from config into the database.

    Instruments not in the config are left untouched.
    Unknown instrument IDs encountered by the watcher at runtime will
    cause a hard error — they must be registered here first.
    """
    for instrument_id, attrs in config.get("instruments", {}).items():
        inst = session.get(Instrument, instrument_id)
        if inst is None:
            inst = Instrument(
                id=instrument_id,
                name=attrs.get("name", instrument_id),
                vendor=attrs.get("vendor"),
                experiment_type=attrs.get("experiment_type"),
            )
            session.add(inst)
            log.info("Registered new instrument: %s (%s)", instrument_id, attrs.get("name"))
        else:
            inst.name = attrs.get("name", instrument_id)
            if "vendor" in attrs:
                inst.vendor = attrs["vendor"]
            if "experiment_type" in attrs:
                inst.experiment_type = attrs["experiment_type"]
            log.debug("Updated instrument: %s", instrument_id)
