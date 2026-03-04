"""Tests for the db layer (models, results CRUD, settings CRUD)."""

import datetime

import pytest

from rapidqcms.db.results import (
    get_results_by_status,
    get_results_for_instrument,
    get_results_for_run,
    write_qc_result,
)
from rapidqcms.db.settings import (
    create_run,
    get_run,
    list_instruments,
    list_runs_for_instrument,
    upsert_instrument,
)
from rapidqcms.qc.base import QCResult, QCStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_result(status: QCStatus = QCStatus.PASS, module: str = "metabolomics") -> QCResult:
    return QCResult(
        status=status,
        module=module,
        metrics={"intensity_dropouts": 0, "rt_shifts": 2},
        message="",
    )


def _seed_instrument_and_run(session) -> tuple[str, str]:
    upsert_instrument(session, "INSTR_001", "Orbitrap Exploris 480", "Thermo Fisher", "metabolomics")
    create_run(session, "RUN_2026_001", "INSTR_001", "metabolomics")
    session.commit()
    return "INSTR_001", "RUN_2026_001"


# ---------------------------------------------------------------------------
# QC Results
# ---------------------------------------------------------------------------

class TestWriteQCResult:
    def test_writes_pass_result(self, db_session):
        instrument_id, run_id = _seed_instrument_and_run(db_session)
        result = _make_result(QCStatus.PASS)

        write_qc_result(
            db_session, instrument_id, run_id, "SAMPLE_001",
            "metabolomics", "pre_search", result,
            acquired_at=datetime.datetime(2026, 2, 20, 10, 0, 0),
        )
        db_session.commit()

        records = get_results_for_run(db_session, instrument_id, run_id)
        assert len(records) == 1
        assert records[0].status == "Pass"
        assert records[0].metrics["intensity_dropouts"] == 0

    def test_writes_fail_result_with_message(self, db_session):
        instrument_id, run_id = _seed_instrument_and_run(db_session)
        result = QCResult(
            status=QCStatus.FAIL,
            module="metabolomics",
            message="Too many intensity dropouts",
        )

        write_qc_result(
            db_session, instrument_id, run_id, "SAMPLE_002",
            "metabolomics", "pre_search", result,
        )
        db_session.commit()

        records = get_results_for_run(db_session, instrument_id, run_id)
        assert records[0].status == "Fail"
        assert records[0].message == "Too many intensity dropouts"

    def test_writes_grades_and_round_trips(self, db_session):
        instrument_id, run_id = _seed_instrument_and_run(db_session)
        grades = {
            "is_fill_fraction": {"status": "Warn", "message": "IS detection 18/21 (85.7%) < 90% warn threshold"},
            "is_rt_deviation": {"status": "Pass", "message": None},
        }
        result = QCResult(
            status=QCStatus.WARN,
            module="metabolomics_pre",
            grades=grades,
        )

        write_qc_result(
            db_session, instrument_id, run_id, "SAMPLE_003",
            "metabolomics", "pre_search", result,
        )
        db_session.commit()

        records = get_results_for_run(db_session, instrument_id, run_id)
        assert records[0].grades is not None
        assert records[0].grades["is_fill_fraction"]["status"] == "Warn"
        assert "85.7%" in records[0].grades["is_fill_fraction"]["message"]
        assert records[0].grades["is_rt_deviation"]["status"] == "Pass"


class TestGetResultsForRun:
    def test_ordered_by_acquired_at(self, db_session):
        instrument_id, run_id = _seed_instrument_and_run(db_session)
        statuses = [QCStatus.PASS, QCStatus.WARN, QCStatus.FAIL]

        for i, status in enumerate(statuses):
            write_qc_result(
                db_session, instrument_id, run_id, f"SAMPLE_{i:03d}",
                "metabolomics", "pre_search", _make_result(status),
                acquired_at=datetime.datetime(2026, 2, 20, 10, i, 0),
            )
        db_session.commit()

        records = get_results_for_run(db_session, instrument_id, run_id)
        assert [r.status for r in records] == ["Pass", "Warn", "Fail"]

    def test_empty_for_unknown_run(self, db_session):
        assert get_results_for_run(db_session, "UNKNOWN", "UNKNOWN") == []


class TestGetResultsByStatus:
    def test_filters_by_status(self, db_session):
        instrument_id, run_id = _seed_instrument_and_run(db_session)

        for status in [QCStatus.PASS, QCStatus.PASS, QCStatus.FAIL]:
            write_qc_result(
                db_session, instrument_id, run_id, "SAMPLE_X",
                "metabolomics", "pre_search", _make_result(status),
            )
        db_session.commit()

        fails = get_results_by_status(db_session, QCStatus.FAIL)
        assert len(fails) == 1
        passes = get_results_by_status(db_session, QCStatus.PASS)
        assert len(passes) == 2

    def test_filters_by_experiment_type(self, db_session):
        instrument_id, run_id = _seed_instrument_and_run(db_session)

        write_qc_result(
            db_session, instrument_id, run_id, "S1",
            "metabolomics", "pre_search", _make_result(QCStatus.PASS),
        )
        write_qc_result(
            db_session, instrument_id, run_id, "S2",
            "proteomics_dda", "pre_search", _make_result(QCStatus.PASS),
        )
        db_session.commit()

        results = get_results_by_status(
            db_session, QCStatus.PASS, experiment_type="proteomics_dda"
        )
        assert len(results) == 1
        assert results[0].experiment_type == "proteomics_dda"


# ---------------------------------------------------------------------------
# Settings (instruments + runs)
# ---------------------------------------------------------------------------

class TestInstrumentCRUD:
    def test_upsert_creates_instrument(self, db_session):
        upsert_instrument(db_session, "INSTR_001", "Exploris 480", "Thermo Fisher")
        db_session.commit()

        instruments = list_instruments(db_session)
        assert len(instruments) == 1
        assert instruments[0].id == "INSTR_001"

    def test_upsert_updates_existing(self, db_session):
        upsert_instrument(db_session, "INSTR_001", "Old Name")
        db_session.commit()
        upsert_instrument(db_session, "INSTR_001", "New Name")
        db_session.commit()

        instruments = list_instruments(db_session)
        assert len(instruments) == 1
        assert instruments[0].name == "New Name"


class TestRunCRUD:
    def test_create_run(self, db_session):
        upsert_instrument(db_session, "INSTR_001", "Test")
        create_run(db_session, "RUN_001", "INSTR_001", "metabolomics")
        db_session.commit()

        run = get_run(db_session, "RUN_001")
        assert run is not None
        assert run.id == "RUN_001"
        assert run.instrument_id == "INSTR_001"
        assert run.experiment_type == "metabolomics"

    def test_list_runs_for_instrument(self, db_session):
        upsert_instrument(db_session, "INSTR_001", "Test")
        create_run(db_session, "RUN_001", "INSTR_001", "metabolomics")
        create_run(db_session, "RUN_002", "INSTR_001", "metabolomics")
        db_session.commit()

        runs = list_runs_for_instrument(db_session, "INSTR_001")
        assert len(runs) == 2
