"""Tests for src/rapidqcms/service/listener.py"""

import datetime
import hashlib
import threading
import time
from pathlib import Path
from unittest import mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from rapidqcms.db.models import Base, Instrument, Run
from rapidqcms.qc.base import QCResult, QCStatus
from rapidqcms.service.listener import AcquisitionEventHandler, ListenerConfig, _md5


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _make_session_factory(tmp_path):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    # Pre-populate required FK parents
    Session = sessionmaker(engine)
    with Session() as s:
        s.add(Instrument(id="INST01", name="Test"))
        s.add(Run(id="RUN001", instrument_id="INST01", experiment_type="metabolomics"))
        s.commit()
    return Session


def _make_config(tmp_path) -> ListenerConfig:
    return ListenerConfig(
        instrument_id="INST01",
        run_id="RUN001",
        watch_path=tmp_path,
        extension=".raw",
        experiment_type="metabolomics",
        chromatography="HILIC",
        polarity="Pos",
        stage="pre_search",
        md5_check_interval=0,  # no wait in tests
    )


# ---------------------------------------------------------------------------
# MD5 stability helper
# ---------------------------------------------------------------------------


def test_stable_file_detected_on_matching_checksums(tmp_path):
    """_wait_for_stable returns True when MD5 is unchanged on second check."""
    raw = tmp_path / "sample.raw"
    raw.write_bytes(b"stable content")

    cfg = _make_config(tmp_path)
    sf = sessionmaker(create_engine("sqlite:///:memory:"))
    handler = AcquisitionEventHandler(cfg, sf)

    # With md5_check_interval=0 and identical file, should resolve quickly
    result = handler._wait_for_stable(raw)
    assert result is True


def test_unstable_file_retried(tmp_path):
    """_wait_for_stable keeps retrying while file content changes."""
    raw = tmp_path / "sample.raw"
    raw.write_bytes(b"initial content")

    cfg = _make_config(tmp_path)
    sf = sessionmaker(create_engine("sqlite:///:memory:"))
    handler = AcquisitionEventHandler(cfg, sf)

    call_count = 0

    original_md5 = _md5

    def changing_md5(path):
        nonlocal call_count
        call_count += 1
        # Return different values for first two calls, then stabilise
        return str(call_count) if call_count < 3 else "stable"

    with mock.patch("rapidqcms.service.listener._md5", side_effect=changing_md5):
        result = handler._wait_for_stable(raw)

    assert result is True
    assert call_count >= 3  # at least two unstable + one stable


# ---------------------------------------------------------------------------
# _handle_file: end-to-end with mocked processor
# ---------------------------------------------------------------------------


def test_handle_file_persists_qc_result(tmp_path):
    """_handle_file calls write_qc_result and write_gate_file after processing."""
    raw = tmp_path / "sample.raw"
    raw.write_bytes(b"data")

    cfg = _make_config(tmp_path)
    session_factory = _make_session_factory(tmp_path)

    fake_result = QCResult(
        status=QCStatus.PASS,
        module="metabolomics",
        metrics={"total_standards": 5, "detected_standards": 5},
        details=[{"Name": "IS1", "RT (min)": 1.23}],
    )

    with (
        mock.patch("rapidqcms.service.listener._md5", return_value="fixed"),
        mock.patch(
            "rapidqcms.service.listener.process_sample",
            return_value=[fake_result],
        ),
        mock.patch(
            "rapidqcms.service.listener.get_internal_standards_df",
            return_value=mock.MagicMock(),
        ),
        mock.patch(
            "rapidqcms.service.listener.get_in_run_rt_history",
            return_value=None,
        ),
        mock.patch("rapidqcms.service.listener.write_qc_result") as mock_write_qc,
        mock.patch("rapidqcms.service.listener.write_gate_file") as mock_write_gate,
    ):
        handler = AcquisitionEventHandler(cfg, session_factory)
        handler._handle_file(raw)

    mock_write_qc.assert_called_once()
    mock_write_gate.assert_called_once()

    # Verify gate file was called with PASS status and correct stage
    gate_call_args = mock_write_gate.call_args
    assert gate_call_args.args[0] == raw          # raw_path
    assert gate_call_args.args[2] == "pre_search"  # stage
    gate_result_arg = gate_call_args.args[1]
    assert gate_result_arg.status == QCStatus.PASS
