"""Settings CRUD for the central database.

This module will grow to replace the settings-related functions currently
spread across DatabaseFunctions.py (instruments, chromatography methods,
internal standards, QC parameters, MS-DIAL configs, notification settings).

Phase 1 provides the skeleton. Full migration happens in Phase 3.
"""

from sqlalchemy.orm import Session

from .models import Instrument, QCResult, Run


# ---------------------------------------------------------------------------
# Instruments
# ---------------------------------------------------------------------------

def get_instrument(session: Session, instrument_id: str) -> Instrument | None:
    return session.get(Instrument, instrument_id)


def upsert_instrument(
    session: Session,
    instrument_id: str,
    name: str,
    vendor: str | None = None,
    experiment_type: str | None = None,
) -> Instrument:
    instrument = session.get(Instrument, instrument_id)
    if instrument is None:
        instrument = Instrument(
            id=instrument_id,
            name=name,
            vendor=vendor,
            experiment_type=experiment_type,
        )
        session.add(instrument)
    else:
        instrument.name = name
        if vendor is not None:
            instrument.vendor = vendor
        if experiment_type is not None:
            instrument.experiment_type = experiment_type
    return instrument


def list_instruments(session: Session) -> list[Instrument]:
    return session.query(Instrument).order_by(Instrument.name).all()


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------

def get_run(session: Session, run_id: str) -> Run | None:
    return session.get(Run, run_id)


def create_run(
    session: Session,
    run_id: str,
    instrument_id: str,
    experiment_type: str,
) -> Run:
    run = Run(
        id=run_id,
        instrument_id=instrument_id,
        experiment_type=experiment_type,
        status="active",
    )
    session.add(run)
    return run


def complete_run(session: Session, run_id: str) -> Run | None:
    import datetime
    run = session.get(Run, run_id)
    if run:
        run.status = "completed"
        run.completed_at = datetime.datetime.now(datetime.timezone.utc)
    return run


def list_runs_for_instrument(
    session: Session, instrument_id: str
) -> list[Run]:
    return (
        session.query(Run)
        .filter_by(instrument_id=instrument_id)
        .order_by(Run.started_at.desc())
        .all()
    )


def list_all_runs(
    session: Session,
    instrument_ids: list[str] | None = None,
    experiment_type: str | None = None,
    status: str | None = None,
    since=None,
    limit: int = 500,
) -> list[Run]:
    """Return runs across all instruments, newest first, with optional filters."""
    q = session.query(Run)
    if instrument_ids:
        q = q.filter(Run.instrument_id.in_(instrument_ids))
    if experiment_type:
        q = q.filter(Run.experiment_type == experiment_type)
    if status:
        q = q.filter(Run.status == status)
    if since:
        q = q.filter(Run.started_at >= since)
    return q.order_by(Run.started_at.desc()).limit(limit).all()


def delete_run(session: Session, run_id: str) -> None:
    """Delete a Run and cascade-delete its QCResult children."""
    session.query(QCResult).filter_by(run_id=run_id).delete()
    run = session.get(Run, run_id)
    if run is not None:
        session.delete(run)
