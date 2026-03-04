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
    chromatography: str | None = None,
) -> Run:
    run = Run(
        id=run_id,
        instrument_id=instrument_id,
        experiment_type=experiment_type,
        chromatography=chromatography,
    )
    session.add(run)
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
    chromatographies: list[str] | None = None,
    since=None,
    limit: int = 500,
) -> list[Run]:
    """Return runs across all instruments, newest first, with optional filters.

    chromatographies: if given, only return runs whose chromatography is in
    this list (e.g. ["HILIC"]).  Runs with chromatography=None are excluded.
    """
    q = session.query(Run)
    if instrument_ids:
        q = q.filter(Run.instrument_id.in_(instrument_ids))
    if experiment_type:
        q = q.filter(Run.experiment_type == experiment_type)
    if chromatographies:
        q = q.filter(Run.chromatography.in_(chromatographies))
    if since:
        q = q.filter(Run.started_at >= since)
    return q.order_by(Run.started_at.desc()).limit(limit).all()


def delete_run(session: Session, run_id: str) -> None:
    """Delete a Run and cascade-delete its QCResult children."""
    session.query(QCResult).filter_by(run_id=run_id).delete()
    run = session.get(Run, run_id)
    if run is not None:
        session.delete(run)


def get_instrument_run_qc_summary(
    session: Session,
    instrument_ids: list[str] | None = None,
    experiment_type: str | None = None,
    since=None,
    limit: int = 500,
) -> list[dict]:
    """Return run-level QC aggregates for performance trend charts.

    Each entry covers one (instrument_id, run_id) pair with aggregated
    Pass/Warn/Fail counts and experiment-type-specific metric averages.
    Results are ordered by started_at ascending (chronological for charting).
    """
    import datetime
    from collections import defaultdict

    q = (
        session.query(QCResult, Run.started_at, Run.experiment_type, Run.summary_metrics)
        .join(Run, QCResult.run_id == Run.id)
    )
    if instrument_ids:
        q = q.filter(QCResult.instrument_id.in_(instrument_ids))
    if experiment_type:
        q = q.filter(Run.experiment_type == experiment_type)
    if since:
        q = q.filter(Run.started_at >= since)

    rows = q.order_by(Run.started_at.asc()).limit(limit).all()

    groups: dict = defaultdict(list)
    meta: dict = {}
    for qc, started_at, exp_type, run_summary in rows:
        key = (qc.instrument_id, qc.run_id)
        groups[key].append(qc)
        meta[key] = (started_at, exp_type, run_summary or {})

    result = []
    for (instrument_id, run_id), qcs in groups.items():
        started_at, exp_type, run_summary = meta[(instrument_id, run_id)]
        n_pass  = sum(1 for q in qcs if q.status == "Pass")
        n_warn  = sum(1 for q in qcs if q.status == "Warn")
        n_fail  = sum(1 for q in qcs if q.status == "Fail")
        n_total = len(qcs)
        m_list  = [q.metrics or {} for q in qcs]

        def _avg(key):
            vals = [m[key] for m in m_list if key in m and m[key] is not None]
            return round(sum(vals) / len(vals), 3) if vals else None

        def _avg_len(key):
            vals = [len(m[key]) for m in m_list if key in m]
            return round(sum(vals) / len(vals), 2) if vals else None

        result.append({
            "instrument_id":     instrument_id,
            "run_id":            run_id,
            "started_at":        started_at,
            "experiment_type":   exp_type,
            "n_pass":            n_pass,
            "n_warn":            n_warn,
            "n_fail":            n_fail,
            "n_total":           n_total,
            "pass_rate":         round(n_pass / n_total, 3) if n_total else None,
            # metabolomics
            "avg_fill_fraction": _avg("fill_fraction"),
            "avg_rt_warn":       _avg_len("rt_warn_is"),
            "avg_rt_fail":       _avg_len("rt_fail_is"),
            "avg_mz_warn":       _avg_len("mz_warn_is"),
            "avg_mz_fail":       _avg_len("mz_fail_is"),
            # CV comes from run-level summary_metrics (computed across all samples)
            "avg_cv_warn":       len(run_summary.get("cv_warn_is") or []),
            "avg_cv_fail":       len(run_summary.get("cv_fail_is") or []),
            # proteomics
            "avg_ms1":           _avg("ms1_count"),
            "avg_ms2":           _avg("ms2_count"),
            "avg_ratio":         _avg("ms2_ms1_ratio"),
        })

    return sorted(result, key=lambda x: x["started_at"] or datetime.datetime.min)
