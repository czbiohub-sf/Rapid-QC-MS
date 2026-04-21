"""Tests for src/rapidqcms/db/features.py"""

import datetime

import pytest

from rapidqcms.db.features import (
    get_in_run_rt_history,
    get_qc_configuration,
    upsert_qc_configuration,
)
from rapidqcms.db.results import write_qc_result
from rapidqcms.qc.base import QCResult, QCStatus


# ---------------------------------------------------------------------------
# QCConfiguration CRUD
# ---------------------------------------------------------------------------


def test_upsert_qc_configuration(db_session):
    """Round-trip CRUD for QCConfiguration."""
    upsert_qc_configuration(
        db_session,
        "default",
        intensity_dropouts_cutoff=6,
        library_rt_shift_cutoff=0.5,
        in_run_rt_shift_cutoff=0.15,
        library_mz_shift_cutoff=0.01,
        intensity_enabled=True,
        library_rt_enabled=True,
        in_run_rt_enabled=False,
        library_mz_enabled=True,
    )
    db_session.flush()

    cfg = get_qc_configuration(db_session, "default")
    assert cfg is not None
    assert cfg.intensity_dropouts_cutoff == 6
    assert cfg.library_rt_shift_cutoff == pytest.approx(0.5)
    assert cfg.in_run_rt_enabled is False

    # Upsert update
    upsert_qc_configuration(db_session, "default", intensity_dropouts_cutoff=8)
    db_session.flush()
    cfg2 = get_qc_configuration(db_session, "default")
    assert cfg2.intensity_dropouts_cutoff == 8
    assert cfg2.library_rt_shift_cutoff == pytest.approx(0.5)  # unchanged


def test_get_qc_configuration_returns_none_for_unknown(db_session):
    assert get_qc_configuration(db_session, "nonexistent") is None


# ---------------------------------------------------------------------------
# In-run RT history
# ---------------------------------------------------------------------------


def test_get_in_run_rt_history_returns_none_when_no_results(db_session):
    result = get_in_run_rt_history(db_session, run_id="RUN001", instrument_id="INST01")
    assert result is None


def test_get_in_run_rt_history_reconstructs_from_details(db_session):
    """Write synthetic QCResult records and verify DataFrame shape."""
    from rapidqcms.db.models import Instrument, Run

    # Minimal Instrument + Run rows required by FK
    db_session.add(Instrument(id="INST01", name="Test Instrument"))
    db_session.add(Run(id="RUN001", instrument_id="INST01", experiment_type="metabolomics"))
    db_session.flush()

    samples = ["sample_001", "sample_002", "sample_003"]
    standards = ["IS1", "IS2"]

    for idx, sample in enumerate(samples):
        details = [
            {"Name": "IS1", "RT (min)": 1.0 + idx * 0.01, "Delta RT": 0.0, "Fails": "", "Warnings": ""},
            {"Name": "IS2", "RT (min)": 2.0 + idx * 0.02, "Delta RT": 0.0, "Fails": "", "Warnings": ""},
        ]
        qc_result = QCResult(
            status=QCStatus.PASS,
            module="metabolomics",
            details=details,
        )
        write_qc_result(
            db_session,
            instrument_id="INST01",
            run_id="RUN001",
            sample_id=sample,
            experiment_type="metabolomics",
            qc_stage="pre_search",
            result=qc_result,
            acquired_at=datetime.datetime(2024, 1, 1, 12, idx, tzinfo=datetime.timezone.utc),
        )
    db_session.flush()

    df = get_in_run_rt_history(db_session, run_id="RUN001", instrument_id="INST01")

    assert df is not None
    assert "Specimen" in df.columns
    assert "IS1" in df.columns
    assert "IS2" in df.columns
    assert len(df) == 3  # three samples
    assert list(df["Specimen"]) == samples
    assert df["IS1"].iloc[0] == pytest.approx(1.0)
    assert df["IS2"].iloc[2] == pytest.approx(2.04)
