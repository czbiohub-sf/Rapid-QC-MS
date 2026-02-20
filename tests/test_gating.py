"""Tests for the gate file writer/reader."""

import json

from rapidqcms.qc.base import QCResult, QCStatus
from rapidqcms.service.gating import get_gate_status, read_gate_file, write_gate_file


def _raw(tmp_path, name="sample_001.raw"):
    p = tmp_path / name
    p.touch()
    return p


class TestWriteGateFile:
    def test_pass_writes_qc_pass_suffix(self, tmp_path):
        result = QCResult(status=QCStatus.PASS, module="metabolomics")
        gate = write_gate_file(_raw(tmp_path), result, stage="pre_search")
        assert gate.suffix == ".qc_pass"

    def test_warn_writes_qc_fail_suffix(self, tmp_path):
        result = QCResult(status=QCStatus.WARN, module="metabolomics")
        gate = write_gate_file(_raw(tmp_path), result, stage="pre_search")
        assert gate.suffix == ".qc_fail"

    def test_fail_writes_qc_fail_suffix(self, tmp_path):
        result = QCResult(status=QCStatus.FAIL, module="metabolomics")
        gate = write_gate_file(_raw(tmp_path), result, stage="pre_search")
        assert gate.suffix == ".qc_fail"

    def test_gate_file_is_valid_json(self, tmp_path):
        result = QCResult(
            status=QCStatus.PASS,
            module="metabolomics",
            metrics={"intensity_dropouts": 0},
            message="all checks passed",
        )
        gate = write_gate_file(_raw(tmp_path), result, stage="pre_search")
        data = json.loads(gate.read_text())

        assert data["status"] == "Pass"
        assert data["stage"] == "pre_search"
        assert data["module"] == "metabolomics"
        assert data["metrics"]["intensity_dropouts"] == 0
        assert "timestamp" in data

    def test_gate_file_includes_stage(self, tmp_path):
        result = QCResult(status=QCStatus.PASS, module="proteomics_post")
        gate = write_gate_file(_raw(tmp_path), result, stage="post_search")
        data = read_gate_file(gate)
        assert data["stage"] == "post_search"


class TestGetGateStatus:
    def test_returns_pass_when_qc_pass_exists(self, tmp_path):
        raw = _raw(tmp_path)
        write_gate_file(raw, QCResult(status=QCStatus.PASS, module="m"), stage="pre_search")
        assert get_gate_status(raw, "pre_search") == QCStatus.PASS

    def test_returns_fail_when_qc_fail_exists(self, tmp_path):
        raw = _raw(tmp_path)
        write_gate_file(raw, QCResult(status=QCStatus.FAIL, module="m"), stage="pre_search")
        assert get_gate_status(raw, "pre_search") == QCStatus.FAIL

    def test_returns_warn_when_qc_fail_contains_warn(self, tmp_path):
        raw = _raw(tmp_path)
        write_gate_file(raw, QCResult(status=QCStatus.WARN, module="m"), stage="pre_search")
        assert get_gate_status(raw, "pre_search") == QCStatus.WARN

    def test_returns_none_when_no_gate_exists(self, tmp_path):
        raw = _raw(tmp_path)
        assert get_gate_status(raw, "pre_search") is None

    def test_stage_mismatch_returns_none(self, tmp_path):
        raw = _raw(tmp_path)
        write_gate_file(raw, QCResult(status=QCStatus.PASS, module="m"), stage="pre_search")
        assert get_gate_status(raw, "post_search") is None
