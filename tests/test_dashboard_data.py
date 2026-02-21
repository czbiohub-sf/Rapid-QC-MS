"""Tests for the dashboard data layer (data.py).

Uses the in-memory SQLite db_session fixture from conftest.py.
"""

import datetime
import json

import io

import pandas as pd
import pytest

from rapidqcms.db.models import Instrument, InternalStandard, QCResult, Run
from rapidqcms.dashboard.data import get_run_dataframes, get_sample_table


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_instrument(session, instrument_id="INST01"):
    inst = Instrument(id=instrument_id, name="Test Instrument")
    session.add(inst)
    run = Run(id="RUN001", instrument_id=instrument_id, experiment_type="HILIC")
    session.add(run)
    return inst, run


def _seed_is(session, chromatography="HILIC"):
    """Seed two Pos internal standards."""
    session.add(InternalStandard(
        name="CarnitineD3",
        chromatography=chromatography,
        polarity="Pos",
        precursor_mz=165.123,
        retention_time=0.85,
    ))
    session.add(InternalStandard(
        name="AcetylcarnitineD3",
        chromatography=chromatography,
        polarity="Pos",
        precursor_mz=207.149,
        retention_time=1.10,
    ))


def _make_details(rt1, rt2):
    return [
        {
            "Name": "CarnitineD3",
            "RT (min)": rt1,
            "Height": 100_000.0,
            "Delta m/z": 0.001,
            "Delta RT": rt1 - 0.85,
            "In-run delta RT": 0.0,
            "Intensity dropout": 0,
            "Warnings": "",
            "Fails": "",
        },
        {
            "Name": "AcetylcarnitineD3",
            "RT (min)": rt2,
            "Height": 80_000.0,
            "Delta m/z": 0.002,
            "Delta RT": rt2 - 1.10,
            "In-run delta RT": 0.0,
            "Intensity dropout": 0,
            "Warnings": "",
            "Fails": "",
        },
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_get_run_dataframes_returns_empty_dicts_for_no_results(db_session):
    """When there are no QCResult rows the returned dict has None pivot values."""
    _seed_instrument(db_session)
    db_session.commit()

    result = get_run_dataframes(db_session, "INST01", "RUN001", chromatography="HILIC")

    assert isinstance(result, dict)
    assert result["df_rt_pos"] is None
    assert result["df_rt_neg"] is None
    assert result["df_intensity_pos"] is None
    pos_is = json.loads(result["pos_internal_standards"])
    assert pos_is == []


def test_get_run_dataframes_pivots_rt_correctly(db_session):
    """Two samples with Pos IS should produce df_rt_pos with 2 rows and IS columns."""
    _seed_instrument(db_session)
    _seed_is(db_session)

    now = datetime.datetime.now(datetime.UTC)
    for i, (rt1, rt2) in enumerate([(0.84, 1.09), (0.86, 1.11)]):
        db_session.add(QCResult(
            instrument_id="INST01",
            run_id="RUN001",
            sample_id=f"Sample_{i + 1}",
            experiment_type="HILIC",
            qc_stage="pre_search",
            qc_module="metabolomics",
            status="Pass",
            acquired_at=now + datetime.timedelta(minutes=i),
            details=_make_details(rt1, rt2),
        ))
    db_session.commit()

    result = get_run_dataframes(db_session, "INST01", "RUN001", chromatography="HILIC")

    assert result["df_rt_pos"] is not None
    df = pd.read_json(io.StringIO(result["df_rt_pos"]), orient="records")

    # 2 samples, 3 columns: Specimen + 2 IS
    assert df.shape == (2, 3)
    assert "Specimen" in df.columns
    assert "CarnitineD3" in df.columns
    assert "AcetylcarnitineD3" in df.columns

    # RT values should match what we seeded
    carnitine_rts = set(df["CarnitineD3"].tolist())
    assert carnitine_rts == {0.84, 0.86}


def test_get_run_dataframes_pos_internal_standards_sorted(db_session):
    """pos_internal_standards key should be a sorted JSON list."""
    _seed_instrument(db_session)
    _seed_is(db_session)

    db_session.add(QCResult(
        instrument_id="INST01",
        run_id="RUN001",
        sample_id="Sample_1",
        experiment_type="HILIC",
        qc_stage="pre_search",
        qc_module="metabolomics",
        status="Pass",
        acquired_at=datetime.datetime.now(datetime.UTC),
        details=_make_details(0.85, 1.10),
    ))
    db_session.commit()

    result = get_run_dataframes(db_session, "INST01", "RUN001", chromatography="HILIC")
    pos_is = json.loads(result["pos_internal_standards"])
    assert pos_is == sorted(pos_is)
    assert "CarnitineD3" in pos_is
    assert "AcetylcarnitineD3" in pos_is


def test_get_sample_table_shape(db_session):
    """get_sample_table returns a DataFrame with expected columns."""
    _seed_instrument(db_session)

    for i in range(3):
        db_session.add(QCResult(
            instrument_id="INST01",
            run_id="RUN001",
            sample_id=f"Sample_{i + 1}",
            experiment_type="HILIC",
            qc_stage="pre_search",
            qc_module="metabolomics",
            status="Pass",
            acquired_at=datetime.datetime.now(datetime.UTC),
        ))
    db_session.commit()

    df = get_sample_table(db_session, "INST01", "RUN001")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert "Specimen" in df.columns
    assert "QC" in df.columns


def test_dashboard_app_imports():
    """Smoke-test: the dashboard app can be imported without error."""
    from rapidqcms.dashboard.app import app  # noqa: F401

    assert app is not None
    assert app.title == "Rapid-QC-MS"
