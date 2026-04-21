"""Tests for the dashboard data layer (data.py).

Uses the in-memory SQLite db_session fixture from conftest.py.
"""

import datetime
import json

import io

import pandas as pd
import pytest

from rapidqcms.db.models import BioStandard, Instrument, QCResult, Run
from rapidqcms.dashboard.data import (
    _identify_bio_standards,
    get_bio_standard_dataframes,
    get_run_dataframes,
    get_sample_table,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_instrument(session, instrument_id="INST01"):
    inst = Instrument(id=instrument_id, name="Test Instrument")
    session.add(inst)
    run = Run(id="RUN001", instrument_id=instrument_id, experiment_type="HILIC")
    session.add(run)
    return inst, run



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
    """get_sample_table returns a DataFrame with Specimen, Status, QC columns."""
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
    assert "Status" in df.columns
    assert "QC" in df.columns


def test_get_sample_table_qc_from_grades(db_session):
    """QC column shows per-check breakdown from grades; Status holds overall status."""
    _seed_instrument(db_session)

    db_session.add(QCResult(
        instrument_id="INST01",
        run_id="RUN001",
        sample_id="Sample_warn",
        experiment_type="HILIC",
        qc_stage="pre_search",
        qc_module="metabolomics_pre",
        status="Warn",
        acquired_at=datetime.datetime.now(datetime.UTC),
        grades={
            "is_fill_fraction": {"status": "Warn", "message": "IS detection 18/21 (85.7%) < 90% warn threshold"},
            "is_rt_deviation": {"status": "Pass", "message": None},
        },
    ))
    db_session.add(QCResult(
        instrument_id="INST01",
        run_id="RUN001",
        sample_id="Sample_pass",
        experiment_type="HILIC",
        qc_stage="pre_search",
        qc_module="metabolomics_pre",
        status="Pass",
        acquired_at=datetime.datetime.now(datetime.UTC),
        grades={
            "is_fill_fraction": {"status": "Pass", "message": None},
            "is_rt_deviation": {"status": "Pass", "message": None},
        },
    ))
    db_session.commit()

    df = get_sample_table(db_session, "INST01", "RUN001")
    warn_row = df.loc[df["Specimen"] == "Sample_warn"].iloc[0]
    pass_row = df.loc[df["Specimen"] == "Sample_pass"].iloc[0]

    assert warn_row["Status"] == "Warn"
    assert "fill:Warn" in warn_row["QC"]
    assert "RT" not in warn_row["QC"]   # Pass checks are not shown
    assert pass_row["Status"] == "Pass"
    assert pass_row["QC"] == ""          # All-pass rows show nothing


def test_get_sample_table_qc_from_metrics_fallback(db_session):
    """QC column derives per-check breakdown from metrics when grades is absent."""
    _seed_instrument(db_session)

    db_session.add(QCResult(
        instrument_id="INST01",
        run_id="RUN001",
        sample_id="Sample_legacy",
        experiment_type="HILIC",
        qc_stage="pre_search",
        qc_module="metabolomics",
        status="Warn",
        acquired_at=datetime.datetime.now(datetime.UTC),
        grades=None,
        metrics={
            "dropout_pct": 2.5,
            "mean_cv_pct": 38.3,
            "in_range_pct": 83.1,
            "warnings": ["Mean CV 38.3% > 30.0%", "Only 83.1% features within 2× median"],
            "fails": [],
        },
    ))
    db_session.commit()

    df = get_sample_table(db_session, "INST01", "RUN001")
    row = df.iloc[0]

    assert row["Status"] == "Warn"
    assert "CV:Warn" in row["QC"]
    assert "in_range:Warn" in row["QC"]
    assert "dropout" not in row["QC"]   # Pass checks are not shown


def test_dashboard_app_imports():
    """Smoke-test: the dashboard app can be imported without error."""
    from rapidqcms.dashboard.app import app  # noqa: F401

    assert app is not None
    assert app.title == "Rapid-QC-MS"


# ---------------------------------------------------------------------------
# Bio standard tests
# ---------------------------------------------------------------------------


def test_identify_bio_standards_prefix_matching():
    """Pure unit test — no DB needed."""
    bio_names = ["HeLa", "K562"]
    sample_ids = ["HeLa_01", "HeLa-rep2", "HeLa", "K562_A", "Sample_1"]
    result = _identify_bio_standards(sample_ids, bio_names)

    assert result["HeLa_01"] == "HeLa"
    assert result["HeLa-rep2"] == "HeLa"
    assert result["HeLa"] == "HeLa"
    assert result["K562_A"] == "K562"
    assert "Sample_1" not in result


def test_get_bio_standard_dataframes_empty_when_no_bio_standards(db_session):
    """Returns empty names list when no bio standards in DB."""
    _seed_instrument(db_session)
    db_session.commit()

    result = get_bio_standard_dataframes(db_session, "INST01", "RUN001", "HILIC")
    assert result["bio_standard_names"] == []


def test_get_bio_standard_dataframes_identifies_hela_samples(db_session):
    """Bio standard samples are identified and pivoted into DataFrames."""
    _seed_instrument(db_session)

    db_session.add(BioStandard(name="HeLa", chromatography="HILIC"))

    now = datetime.datetime.now(datetime.UTC)
    db_session.add(QCResult(
        instrument_id="INST01",
        run_id="RUN001",
        sample_id="HeLa_01",
        experiment_type="HILIC",
        qc_stage="pre_search",
        qc_module="metabolomics",
        status="Pass",
        acquired_at=now,
        details=_make_details(0.85, 1.10),
    ))
    db_session.commit()

    result = get_bio_standard_dataframes(db_session, "INST01", "RUN001", "HILIC")

    assert result["bio_standard_names"] == ["HeLa"]
    assert "HeLa" in result["bio_rt_pos"]

    df = pd.read_json(io.StringIO(result["bio_rt_pos"]["HeLa"]), orient="records")
    assert len(df) == 1
    assert "Name" in df.columns
    assert df["Name"].iloc[0] == "HeLa_01"
    assert "CarnitineD3" in df.columns
    assert "AcetylcarnitineD3" in df.columns
