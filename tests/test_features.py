"""Tests for src/rapidqcms/db/features.py"""

import datetime

import pytest

from rapidqcms.db.features import (
    get_in_run_rt_history,
    get_internal_standards,
    get_qc_configuration,
    list_internal_standards,
    upsert_internal_standard,
    upsert_qc_configuration,
)
from rapidqcms.db.results import write_qc_result
from rapidqcms.qc.base import QCResult, QCStatus


# ---------------------------------------------------------------------------
# Internal-standard CRUD
# ---------------------------------------------------------------------------


def test_upsert_and_get_internal_standards(db_session):
    """Round-trip: insert, retrieve, update."""
    upsert_internal_standard(
        db_session,
        name="IS1",
        chromatography="HILIC",
        polarity="Pos",
        precursor_mz=123.456,
        retention_time=1.23,
        inchikey="AAAAAAAA",
    )
    db_session.flush()

    # Retrieve
    df = get_internal_standards(db_session, "HILIC", "Pos")
    assert len(df) == 1
    assert df.iloc[0]["name"] == "IS1"
    assert df.iloc[0]["precursor_mz"] == pytest.approx(123.456)
    assert df.iloc[0]["retention_time"] == pytest.approx(1.23)
    assert df.iloc[0]["inchikey"] == "AAAAAAAA"

    # Upsert updates the existing record
    upsert_internal_standard(
        db_session,
        name="IS1",
        chromatography="HILIC",
        polarity="Pos",
        precursor_mz=124.0,
        retention_time=1.30,
    )
    db_session.flush()
    df2 = get_internal_standards(db_session, "HILIC", "Pos")
    assert len(df2) == 1
    assert df2.iloc[0]["precursor_mz"] == pytest.approx(124.0)


def test_get_internal_standards_returns_dataframe(db_session):
    """DataFrame has the correct columns and values."""
    upsert_internal_standard(
        db_session,
        name="CarnitineD3",
        chromatography="HILIC",
        polarity="Pos",
        precursor_mz=165.123,
        retention_time=0.85,
        ms2_spectrum="85.0 0.9",
        inchikey="BBBBB",
    )
    db_session.flush()

    df = get_internal_standards(db_session, "HILIC", "Pos")
    expected_cols = {
        "name", "precursor_mz", "retention_time", "ms2_spectrum",
        "inchikey", "chromatography", "polarity",
    }
    assert expected_cols <= set(df.columns)
    row = df.iloc[0]
    assert row["name"] == "CarnitineD3"
    assert row["ms2_spectrum"] == "85.0 0.9"
    assert row["chromatography"] == "HILIC"
    assert row["polarity"] == "Pos"


def test_get_internal_standards_filters_by_chromatography_and_polarity(db_session):
    """Only matching chromatography + polarity rows are returned."""
    for name, chrom, pol in [
        ("IS-HILIC-Pos", "HILIC", "Pos"),
        ("IS-HILIC-Neg", "HILIC", "Neg"),
        ("IS-C18-Pos",   "C18",   "Pos"),
    ]:
        upsert_internal_standard(
            db_session, name=name, chromatography=chrom, polarity=pol,
            precursor_mz=100.0, retention_time=1.0,
        )
    db_session.flush()

    df_pos = get_internal_standards(db_session, "HILIC", "Pos")
    assert list(df_pos["name"]) == ["IS-HILIC-Pos"]

    df_neg = get_internal_standards(db_session, "HILIC", "Neg")
    assert list(df_neg["name"]) == ["IS-HILIC-Neg"]

    df_c18 = get_internal_standards(db_session, "C18", "Pos")
    assert list(df_c18["name"]) == ["IS-C18-Pos"]

    df_none = get_internal_standards(db_session, "C18", "Neg")
    assert len(df_none) == 0


def test_list_internal_standards(db_session):
    """list_internal_standards returns ORM objects, optionally filtered."""
    for chrom in ("HILIC", "C18"):
        upsert_internal_standard(
            db_session, name=f"IS-{chrom}", chromatography=chrom,
            polarity="Pos", precursor_mz=100.0, retention_time=1.0,
        )
    db_session.flush()

    all_std = list_internal_standards(db_session)
    assert len(all_std) == 2

    hilic_only = list_internal_standards(db_session, chromatography="HILIC")
    assert len(hilic_only) == 1
    assert hilic_only[0].name == "IS-HILIC"


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
