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

from .models import (
    BioStandard,
    EmailNotification,
    MsDialConfiguration,
    QCConfiguration,
    QCResult as QCResultModel,
)


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
# QC configuration list / delete
# ---------------------------------------------------------------------------


def list_qc_configurations(session: Session) -> list[QCConfiguration]:
    """Return all QC configurations ordered by ID."""
    return session.query(QCConfiguration).order_by(QCConfiguration.id).all()


def delete_qc_configuration(session: Session, config_id: str) -> None:
    """Delete a QCConfiguration by ID (no-op if not found)."""
    record = session.get(QCConfiguration, config_id)
    if record is not None:
        session.delete(record)


# ---------------------------------------------------------------------------
# Biological standards
# ---------------------------------------------------------------------------


def list_bio_standards(
    session: Session, chromatography: str | None = None
) -> list[BioStandard]:
    """Return all biological standards, optionally filtered by chromatography."""
    q = session.query(BioStandard)
    if chromatography is not None:
        q = q.filter_by(chromatography=chromatography)
    return q.order_by(BioStandard.name).all()


def upsert_bio_standard(
    session: Session,
    name: str,
    chromatography: str,
    msdial_config_id: str | None = None,
) -> BioStandard:
    """Insert or update a BioStandard (matched on name + chromatography)."""
    record = (
        session.query(BioStandard)
        .filter_by(name=name, chromatography=chromatography)
        .one_or_none()
    )
    if record is None:
        record = BioStandard(name=name, chromatography=chromatography)
        session.add(record)
    record.msdial_config_id = msdial_config_id
    return record


def delete_bio_standard(session: Session, name: str, chromatography: str) -> None:
    """Delete a BioStandard by name + chromatography (no-op if not found)."""
    record = (
        session.query(BioStandard)
        .filter_by(name=name, chromatography=chromatography)
        .one_or_none()
    )
    if record is not None:
        session.delete(record)


# ---------------------------------------------------------------------------
# MS-DIAL configurations
# ---------------------------------------------------------------------------


def list_msdial_configurations(session: Session) -> list[MsDialConfiguration]:
    """Return all MS-DIAL configurations ordered by ID."""
    return session.query(MsDialConfiguration).order_by(MsDialConfiguration.id).all()


def upsert_msdial_configuration(
    session: Session,
    config_id: str,
    parameter_file_path: str,
    msdial_exe_path: str | None = None,
) -> MsDialConfiguration:
    """Insert or update an MsDialConfiguration by ID."""
    record = session.get(MsDialConfiguration, config_id)
    if record is None:
        record = MsDialConfiguration(id=config_id)
        session.add(record)
    record.parameter_file_path = parameter_file_path
    record.msdial_exe_path = msdial_exe_path
    return record


def get_msdial_configuration(
    session: Session, config_id: str
) -> MsDialConfiguration | None:
    """Return an MsDialConfiguration by ID, or None if not found."""
    return session.get(MsDialConfiguration, config_id)


def delete_msdial_configuration(session: Session, config_id: str) -> None:
    """Delete an MsDialConfiguration by ID (no-op if not found)."""
    record = session.get(MsDialConfiguration, config_id)
    if record is not None:
        session.delete(record)


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
