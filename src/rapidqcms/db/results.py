import datetime

from sqlalchemy.orm import Session

from .models import QCResult as QCResultModel
from ..qc.base import QCResult, QCStatus


def write_qc_result(
    session: Session,
    instrument_id: str,
    run_id: str,
    sample_id: str,
    experiment_type: str,
    qc_stage: str,
    result: QCResult,
    acquired_at: datetime.datetime | None = None,
) -> QCResultModel:
    """Persist a QCResult dataclass to the database and return the ORM record."""
    record = QCResultModel(
        instrument_id=instrument_id,
        run_id=run_id,
        sample_id=sample_id,
        experiment_type=experiment_type,
        qc_stage=qc_stage,
        qc_module=result.module,
        status=result.status.value,
        acquired_at=acquired_at or datetime.datetime.now(datetime.timezone.utc),
        metrics=result.metrics,
        details=result.details,
        message=result.message or None,
        grades=result.grades or None,
    )
    session.add(record)
    return record


def get_results_for_run(
    session: Session, instrument_id: str, run_id: str
) -> list[QCResultModel]:
    """All QC results for a run, ordered by acquisition time ascending."""
    return (
        session.query(QCResultModel)
        .filter_by(instrument_id=instrument_id, run_id=run_id)
        .order_by(QCResultModel.acquired_at)
        .all()
    )


def get_results_for_instrument(
    session: Session, instrument_id: str, limit: int = 500
) -> list[QCResultModel]:
    """Recent QC results for an instrument, newest first."""
    return (
        session.query(QCResultModel)
        .filter_by(instrument_id=instrument_id)
        .order_by(QCResultModel.acquired_at.desc())
        .limit(limit)
        .all()
    )


def get_results_by_status(
    session: Session,
    status: QCStatus,
    experiment_type: str | None = None,
    since: datetime.datetime | None = None,
    limit: int = 500,
) -> list[QCResultModel]:
    """Filter results by outcome, optionally scoped to an experiment type and time range."""
    q = session.query(QCResultModel).filter(QCResultModel.status == status.value)
    if experiment_type:
        q = q.filter(QCResultModel.experiment_type == experiment_type)
    if since:
        q = q.filter(QCResultModel.acquired_at >= since)
    return q.order_by(QCResultModel.acquired_at.desc()).limit(limit).all()
