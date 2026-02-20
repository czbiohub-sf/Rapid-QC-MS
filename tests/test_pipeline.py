"""Tests for the pipeline orchestrator (service/pipeline.py)."""

from pathlib import Path

import pandas as pd
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
# run_qc integration: uses a custom toml with the metabolomics module
# ---------------------------------------------------------------------------

def _write_peak_list(tmp_path: Path, names: list[str]) -> Path:
    """Write a minimal .msdial TSV where all ISs are found at library values."""
    rows = [{"Title": n, "Precursor m/z": 500.0, "RT (min)": 1.0,
             "Height": 1e6, "MSMS spectrum": None} for n in names]
    df = pd.DataFrame(rows)
    p = tmp_path / "sample.msdial"
    df.to_csv(p, sep="\t", index=False)
    return p


def _write_toml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "qc_modules.toml"
    p.write_text(content)
    return p


class TestRunQC:
    def test_returns_list_of_results(self, tmp_path):
        toml_path = _write_toml(tmp_path, """
[modules.metabolomics]
class   = "rapidqcms.qc.metabolomics.MetabolomicsQCModule"
enabled = true

[modules.metabolomics.thresholds]
intensity_dropouts_cutoff = 4
library_rt_shift_cutoff   = 0.3
in_run_rt_shift_cutoff    = 0.1
library_mz_shift_cutoff   = 0.005

[modules.metabolomics.checks]
intensity_enabled  = true
library_rt_enabled = true
in_run_rt_enabled  = true
library_mz_enabled = true
""")
        features = pd.DataFrame({
            "name": ["IS1", "IS2"],
            "precursor_mz": [500.0, 500.0],
            "retention_time": [1.0, 1.0],
            "ms2_spectrum": [None, None],
            "inchikey": ["IK1", "IK2"],
            "chromatography": ["HILIC", "HILIC"],
            "polarity": ["Pos", "Pos"],
        })
        peak_file = _write_peak_list(tmp_path, ["IS1", "IS2"])

        results = run_qc(
            input_path=peak_file,
            context={"df_features": features},
            stage="pre_search",
            config_path=toml_path,
        )

        assert len(results) == 1
        assert results[0].module == "metabolomics"
        assert results[0].status == QCStatus.PASS

    def test_gate_file_written_on_pass(self, tmp_path):
        toml_path = _write_toml(tmp_path, """
[modules.metabolomics]
class   = "rapidqcms.qc.metabolomics.MetabolomicsQCModule"
enabled = true

[modules.metabolomics.thresholds]
intensity_dropouts_cutoff = 4
library_rt_shift_cutoff   = 0.3
in_run_rt_shift_cutoff    = 0.1
library_mz_shift_cutoff   = 0.005

[modules.metabolomics.checks]
intensity_enabled  = true
library_rt_enabled = true
in_run_rt_enabled  = true
library_mz_enabled = true
""")
        features = pd.DataFrame({
            "name": ["IS1"],
            "precursor_mz": [500.0],
            "retention_time": [1.0],
            "ms2_spectrum": [None],
            "inchikey": ["IK1"],
            "chromatography": ["HILIC"],
            "polarity": ["Pos"],
        })
        peak_file = _write_peak_list(tmp_path, ["IS1"])

        run_qc(
            input_path=peak_file,
            context={"df_features": features},
            stage="pre_search",
            config_path=toml_path,
        )

        assert (tmp_path / "sample.qc_pass").exists()
        assert not (tmp_path / "sample.qc_fail").exists()

    def test_gate_file_written_on_fail(self, tmp_path):
        toml_path = _write_toml(tmp_path, """
[modules.metabolomics]
class   = "rapidqcms.qc.metabolomics.MetabolomicsQCModule"
enabled = true

[modules.metabolomics.thresholds]
intensity_dropouts_cutoff = 1
library_rt_shift_cutoff   = 0.3
in_run_rt_shift_cutoff    = 0.1
library_mz_shift_cutoff   = 0.005

[modules.metabolomics.checks]
intensity_enabled  = true
library_rt_enabled = true
in_run_rt_enabled  = true
library_mz_enabled = true
""")
        features = pd.DataFrame({
            "name": ["IS1", "IS2"],
            "precursor_mz": [500.0, 500.0],
            "retention_time": [1.0, 1.0],
            "ms2_spectrum": [None, None],
            "inchikey": ["IK1", "IK2"],
            "chromatography": ["HILIC", "HILIC"],
            "polarity": ["Pos", "Pos"],
        })
        # Only IS1 found, IS2 missing → 1 dropout ≥ cutoff(1) → FAIL
        peak_file = _write_peak_list(tmp_path, ["IS1"])

        run_qc(
            input_path=peak_file,
            context={"df_features": features},
            stage="pre_search",
            config_path=toml_path,
        )

        assert (tmp_path / "sample.qc_fail").exists()
        assert not (tmp_path / "sample.qc_pass").exists()

    def test_empty_registry_returns_empty_list(self, tmp_path):
        toml_path = _write_toml(tmp_path, "# no modules\n")
        peak_file = _write_peak_list(tmp_path, [])

        results = run_qc(
            input_path=peak_file,
            context={},
            stage="pre_search",
            config_path=toml_path,
        )
        assert results == []
