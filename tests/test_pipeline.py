"""Tests for the pipeline orchestrator (service/pipeline.py)."""

from pathlib import Path

import pytest

from rapidqcms.qc.base import QCResult, QCStatus
from rapidqcms.service.pipeline import _worst_status, run_qc


# ---------------------------------------------------------------------------
# _worst_status helper
# ---------------------------------------------------------------------------

def _r(status: QCStatus) -> QCResult:
    return QCResult(status=status, module="test")


class TestWorstStatus:
    def test_all_pass(self):
        assert _worst_status([_r(QCStatus.PASS)] * 3) == QCStatus.PASS

    def test_any_warn(self):
        assert _worst_status([_r(QCStatus.PASS), _r(QCStatus.WARN)]) == QCStatus.WARN

    def test_any_fail_overrides_warn(self):
        results = [_r(QCStatus.WARN), _r(QCStatus.FAIL), _r(QCStatus.PASS)]
        assert _worst_status(results) == QCStatus.FAIL

    def test_single_fail(self):
        assert _worst_status([_r(QCStatus.FAIL)]) == QCStatus.FAIL


# ---------------------------------------------------------------------------
# run_qc integration: uses proteomics_pre with injected scan_counts
# (avoids needing a real mzML file)
# ---------------------------------------------------------------------------

def _write_toml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "qc_modules.toml"
    p.write_text(content)
    return p


_PROTEOMICS_PRE_TOML = """
[modules.proteomics_pre]
class           = "rapidqcms.qc.proteomics_pre.ProteomicsPreSearchQCModule"
enabled         = true
stage           = "pre_search"
experiment_type = "proteomics"

[modules.proteomics_pre.thresholds]
min_ms1_scans     = 100
min_ms2_scans     = 500
min_ms2_ms1_ratio = 2.0

[modules.proteomics_pre.checks]
ms1_enabled   = true
ms2_enabled   = true
ratio_enabled = true
"""


class TestRunQC:
    def test_returns_list_of_results(self, tmp_path):
        toml_path = _write_toml(tmp_path, _PROTEOMICS_PRE_TOML)
        fake_raw = tmp_path / "sample.raw"
        fake_raw.touch()

        results = run_qc(
            input_path=fake_raw,
            context={"scan_counts": {"ms1": 200, "ms2": 1000}},
            stage="pre_search",
            config_path=toml_path,
        )

        assert len(results) == 1
        assert results[0].module == "proteomics_pre"
        assert results[0].status == QCStatus.PASS

    def test_gate_file_written_on_pass(self, tmp_path):
        toml_path = _write_toml(tmp_path, _PROTEOMICS_PRE_TOML)
        fake_raw = tmp_path / "sample.raw"
        fake_raw.touch()

        run_qc(
            input_path=fake_raw,
            context={"scan_counts": {"ms1": 200, "ms2": 1000}},
            stage="pre_search",
            config_path=toml_path,
        )

        assert (tmp_path / "sample.qc_pass").exists()
        assert not (tmp_path / "sample.qc_fail").exists()

    def test_gate_file_written_on_fail(self, tmp_path):
        toml_path = _write_toml(tmp_path, _PROTEOMICS_PRE_TOML)
        fake_raw = tmp_path / "sample.raw"
        fake_raw.touch()

        # ms2=50 far below min_ms2=500 → hard Fail
        run_qc(
            input_path=fake_raw,
            context={"scan_counts": {"ms1": 200, "ms2": 50}},
            stage="pre_search",
            config_path=toml_path,
        )

        assert (tmp_path / "sample.qc_fail").exists()
        assert not (tmp_path / "sample.qc_pass").exists()

    def test_empty_registry_returns_empty_list(self, tmp_path):
        toml_path = _write_toml(tmp_path, "# no modules\n")
        fake_raw = tmp_path / "sample.raw"
        fake_raw.touch()

        results = run_qc(
            input_path=fake_raw,
            context={},
            stage="pre_search",
            config_path=toml_path,
        )
        assert results == []
