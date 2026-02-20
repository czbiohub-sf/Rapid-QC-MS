"""Feature-layer query functions that build the context dicts consumed by QC modules.

These functions are the bridge between the database and the QC pipeline:
  - get_internal_standards   → df_features for MetabolomicsQCModule
  - get_in_run_rt_history    → df_run_retention_times for in-run RT checks
  - QCConfiguration CRUD     → threshold / enabled-flag overrides
"""

from __future__ import annotations

import json

import pandas as pd
from sqlalchemy.orm import Session

from .models import InternalStandard, QCConfiguration, QCResult as QCResultModel


# ---------------------------------------------------------------------------
# Internal standards
# ---------------------------------------------------------------------------


def get_internal_standards(
    session: Session,
    chromatography: str,
    polarity: str,
) -> pd.DataFrame:
    """Return the internal standard library for a given chromatography / polarity.

    Returns a DataFrame with columns:
        name, precursor_mz, retention_time, ms2_spectrum, inchikey,
        chromatography, polarity
    """
    rows = (
        session.query(InternalStandard)
        .filter_by(chromatography=chromatography, polarity=polarity)
        .order_by(InternalStandard.name)
        .all()
    )
    return pd.DataFrame(
        [
            {
                "name": r.name,
                "precursor_mz": r.precursor_mz,
                "retention_time": r.retention_time,
                "ms2_spectrum": r.ms2_spectrum,
                "inchikey": r.inchikey,
                "chromatography": r.chromatography,
                "polarity": r.polarity,
            }
            for r in rows
        ]
    )


def upsert_internal_standard(
    session: Session,
    name: str,
    chromatography: str,
    polarity: str,
    precursor_mz: float,
    retention_time: float,
    ms2_spectrum: str | None = None,
    inchikey: str | None = None,
) -> InternalStandard:
    """Insert or update an internal standard record (matched on name + chromatography + polarity)."""
    record = (
        session.query(InternalStandard)
        .filter_by(name=name, chromatography=chromatography, polarity=polarity)
        .one_or_none()
    )
    if record is None:
        record = InternalStandard(
            name=name,
            chromatography=chromatography,
            polarity=polarity,
        )
        session.add(record)

    record.precursor_mz = precursor_mz
    record.retention_time = retention_time
    record.ms2_spectrum = ms2_spectrum
    record.inchikey = inchikey
    return record


def list_internal_standards(
    session: Session,
    chromatography: str | None = None,
) -> list[InternalStandard]:
    """Return all internal standards, optionally filtered by chromatography."""
    q = session.query(InternalStandard)
    if chromatography is not None:
        q = q.filter_by(chromatography=chromatography)
    return q.order_by(InternalStandard.name).all()


# ---------------------------------------------------------------------------
# QC configurations
# ---------------------------------------------------------------------------


def get_qc_configuration(session: Session, config_id: str) -> QCConfiguration | None:
    """Return a QCConfiguration by ID, or None if not found."""
    return session.get(QCConfiguration, config_id)


def upsert_qc_configuration(
    session: Session,
    config_id: str,
    **thresholds,
) -> QCConfiguration:
    """Insert or update a QCConfiguration record.

    Keyword arguments match the column names:
        intensity_dropouts_cutoff, library_rt_shift_cutoff,
        in_run_rt_shift_cutoff, library_mz_shift_cutoff,
        intensity_enabled, library_rt_enabled, in_run_rt_enabled,
        library_mz_enabled
    """
    record = session.get(QCConfiguration, config_id)
    if record is None:
        record = QCConfiguration(id=config_id)
        session.add(record)

    for key, value in thresholds.items():
        setattr(record, key, value)

    return record


# ---------------------------------------------------------------------------
# In-run RT history
# ---------------------------------------------------------------------------


def get_in_run_rt_history(
    session: Session,
    run_id: str,
    instrument_id: str,
) -> pd.DataFrame | None:
    """Reconstruct the in-run RT history DataFrame from stored QCResult details.

    Each QCResult.details is a list of per-standard dicts.  Those dicts must
    contain at least "Name" and "RT (min)" (the observed RT) — which
    MetabolomicsQCModule._check() now writes into qc_df.

    Returns a DataFrame with:
        "Specimen" column + one column per internal standard name,
        each cell containing the observed RT (float) for that sample.
    Returns None when no metabolomics QC results exist for this run yet.
    """
    rows = (
        session.query(QCResultModel)
        .filter_by(
            run_id=run_id,
            instrument_id=instrument_id,
            qc_module="metabolomics",
        )
        .order_by(QCResultModel.acquired_at)
        .all()
    )

    if not rows:
        return None

    records = []
    for row in rows:
        details = row.details
        if not details:
            continue
        # details is a list of dicts with Name, RT (min), ...
        rt_map: dict[str, object] = {"Specimen": row.sample_id}
        for entry in details:
            name = entry.get("Name")
            rt = entry.get("RT (min)")
            if name and rt != "" and rt is not None:
                try:
                    rt_map[name] = float(rt)
                except (TypeError, ValueError):
                    pass
        records.append(rt_map)

    if not records:
        return None

    return pd.DataFrame(records)
