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


def upsert_qc_result(
    session: Session,
    instrument_id: str,
    run_id: str,
    sample_id: str,
    experiment_type: str,
    qc_stage: str,
    result: QCResult,
    acquired_at: datetime.datetime | None = None,
) -> QCResultModel:
    """Insert or update a QC result matched by (instrument_id, run_id, sample_id, qc_stage)."""
    existing = (
        session.query(QCResultModel)
        .filter_by(
            instrument_id=instrument_id,
            run_id=run_id,
            sample_id=sample_id,
            qc_stage=qc_stage,
        )
        .first()
    )
    if existing:
        existing.qc_module = result.module
        existing.status = result.status.value
        existing.metrics = result.metrics
        existing.details = result.details
        existing.grades = result.grades or None
        existing.message = result.message or None
        return existing
    return write_qc_result(
        session, instrument_id, run_id, sample_id,
        experiment_type, qc_stage, result, acquired_at,
    )


def update_run_cv(
    session: Session,
    run_id: str,
    qc_stage: str = "pre_search",
    cv_warn_pct: float = 25.0,
    cv_fail_pct: float = 40.0,
) -> None:
    """Compute per-IS CV across all processed samples and write to Run.summary_metrics.

    Called after each sample is committed so the run always reflects the
    most up-to-date CV.  Requires at least 2 samples to produce a CV value.
    """
    from collections import defaultdict
    from .models import Run as RunModel

    rows = (
        session.query(QCResultModel)
        .filter_by(run_id=run_id, qc_stage=qc_stage)
        .order_by(QCResultModel.acquired_at)
        .all()
    )

    heights: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        if not row.details or not isinstance(row.details, list):
            continue
        for entry in row.details:
            nm = entry.get("Name")
            h = entry.get("Height")
            if nm and h is not None and float(h) > 0:
                heights[nm].append(float(h))

    cv_per_is: dict[str, float] = {}
    for nm, hs in heights.items():
        if len(hs) < 2:
            continue
        mean = sum(hs) / len(hs)
        if mean == 0:
            continue
        variance = sum((h - mean) ** 2 for h in hs) / (len(hs) - 1)
        cv_per_is[nm] = round(variance ** 0.5 / mean * 100, 2)

    cv_warn_is = sorted(nm for nm, cv in cv_per_is.items() if cv > cv_warn_pct)
    cv_fail_is = sorted(nm for nm, cv in cv_per_is.items() if cv > cv_fail_pct)

    run = session.get(RunModel, run_id)
    if run is None:
        return

    existing = dict(run.summary_metrics or {})
    existing.update({
        "cv_per_is":        cv_per_is,
        "cv_warn_is":       cv_warn_is,
        "cv_fail_is":       cv_fail_is,
        "n_samples_for_cv": len(rows),
    })
    run.summary_metrics = existing


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
