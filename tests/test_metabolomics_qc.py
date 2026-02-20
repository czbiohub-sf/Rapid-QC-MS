"""Tests for MetabolomicsQCModule.

All tests use synthetic DataFrames written to tmp_path so no real MS-DIAL
files or database connections are needed.
"""

import textwrap
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from rapidqcms.qc.base import QCStatus
from rapidqcms.qc.metabolomics import MetabolomicsQCModule


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LIB_MZ = 500.0
_LIB_RT = 1.0


def _default_config(**overrides):
    cfg = {
        "thresholds": {
            "intensity_dropouts_cutoff": 4,
            "library_rt_shift_cutoff": 0.3,
            "in_run_rt_shift_cutoff": 0.1,
            "library_mz_shift_cutoff": 0.005,
        },
        "checks": {
            "intensity_enabled": True,
            "library_rt_enabled": True,
            "in_run_rt_enabled": True,
            "library_mz_enabled": True,
        },
    }
    cfg.update(overrides)
    return cfg


def _make_features(names, mz=_LIB_MZ, rt=_LIB_RT):
    """Build a minimal df_features DataFrame.

    All features share the same library mz and rt by default, simplifying
    tests that only care about threshold behaviour, not per-feature values.
    """
    n = len(names)
    return pd.DataFrame({
        "name": names,
        "precursor_mz": [mz] * n,
        "retention_time": [rt] * n,
        "ms2_spectrum": [None] * n,
        "inchikey": [f"INCHIKEY{i}" for i in range(n)],
        "chromatography": ["HILIC"] * n,
        "polarity": ["Pos"] * n,
    })


def _write_peak_list(tmp_path: Path, rows: list[dict]) -> Path:
    """Write a minimal .msdial TSV peak list to a temp file."""
    cols = ["Title", "Precursor m/z", "RT (min)", "Height", "MSMS spectrum"]
    df = pd.DataFrame(rows, columns=cols)
    p = tmp_path / "peaks.msdial"
    df.to_csv(p, sep="\t", index=False)
    return p


def _peak_row(name, mz=_LIB_MZ, rt=_LIB_RT, height=1e6, msms=None):
    """Peak row with defaults matching the shared library values."""
    return {"Title": name, "Precursor m/z": mz, "RT (min)": rt,
            "Height": height, "MSMS spectrum": msms}


# ---------------------------------------------------------------------------
# Happy path — all standards found, no shifts
# ---------------------------------------------------------------------------

class TestPassScenario:
    def test_all_found_no_shift_is_pass(self, tmp_path):
        features = _make_features(["IS1", "IS2", "IS3"])
        rows = [_peak_row("IS1"), _peak_row("IS2"), _peak_row("IS3")]
        peak_file = _write_peak_list(tmp_path, rows)
        mod = MetabolomicsQCModule(_default_config())
        result = mod.analyze(peak_file, {"df_features": features})
        assert result.status == QCStatus.PASS
        assert result.metrics["intensity_dropouts"] == 0
        assert result.metrics["detected_standards"] == 3

    def test_biological_standard_always_pass(self, tmp_path):
        features = _make_features(["IS1"])
        peak_file = _write_peak_list(tmp_path, [])  # empty — doesn't matter
        mod = MetabolomicsQCModule(_default_config())
        result = mod.analyze(peak_file, {
            "df_features": features,
            "is_bio_standard": True,
        })
        assert result.status == QCStatus.PASS


# ---------------------------------------------------------------------------
# Intensity dropout checks
# ---------------------------------------------------------------------------

class TestIntensityDropouts:
    def _make_ten_features(self):
        return _make_features([f"IS{i}" for i in range(10)])

    def test_below_cutoff_is_pass(self, tmp_path):
        features = _make_features(["IS1", "IS2", "IS3", "IS4", "IS5", "IS6"])
        # 3 detected, 3 missing. cutoff=4; 3 < 4 → no fail
        # warn threshold: 3 < 4/1.33=3.007 → no warn → PASS
        rows = [_peak_row("IS1"), _peak_row("IS2"), _peak_row("IS3")]
        peak_file = _write_peak_list(tmp_path, rows)
        mod = MetabolomicsQCModule(_default_config())
        result = mod.analyze(peak_file, {"df_features": features})
        assert result.status == QCStatus.PASS
        assert result.metrics["intensity_dropouts"] == 3

    def test_warn_threshold_triggers_warn(self, tmp_path):
        """cutoff=6; warn if dropouts > 6/1.33≈4.5; 5 dropouts → Warn."""
        features = self._make_ten_features()
        cfg = _default_config()
        cfg["thresholds"]["intensity_dropouts_cutoff"] = 6
        rows = [_peak_row(f"IS{i}") for i in range(5)]
        peak_file = _write_peak_list(tmp_path, rows)
        mod = MetabolomicsQCModule(cfg)
        result = mod.analyze(peak_file, {"df_features": features})
        assert result.status == QCStatus.WARN
        assert result.metrics["intensity_dropouts"] == 5

    def test_at_cutoff_is_fail(self, tmp_path):
        features = _make_features(["IS1", "IS2", "IS3", "IS4", "IS5", "IS6"])
        # 2 detected, 4 missing (= cutoff)
        rows = [_peak_row("IS1"), _peak_row("IS2")]
        peak_file = _write_peak_list(tmp_path, rows)
        mod = MetabolomicsQCModule(_default_config())
        result = mod.analyze(peak_file, {"df_features": features})
        assert result.status == QCStatus.FAIL
        assert result.metrics["intensity_dropouts"] == 4

    def test_dropout_check_disabled(self, tmp_path):
        features = _make_features(["IS1", "IS2", "IS3", "IS4", "IS5", "IS6"])
        cfg = _default_config()
        cfg["checks"]["intensity_enabled"] = False
        # Nothing detected → would normally Fail, but check disabled
        peak_file = _write_peak_list(tmp_path, [])
        mod = MetabolomicsQCModule(cfg)
        result = mod.analyze(peak_file, {"df_features": features})
        assert result.status == QCStatus.PASS


# ---------------------------------------------------------------------------
# Library RT shift
# ---------------------------------------------------------------------------

class TestLibraryRTShift:
    def _features(self):
        return _make_features(["IS1", "IS2", "IS3", "IS4"])

    def test_within_cutoff_is_pass(self, tmp_path):
        features = self._features()
        rows = [
            _peak_row("IS1", rt=_LIB_RT + 0.10),   # |delta| = 0.10 < 0.3
            _peak_row("IS2", rt=_LIB_RT + 0.05),
            _peak_row("IS3", rt=_LIB_RT - 0.10),
            _peak_row("IS4", rt=_LIB_RT),
        ]
        peak_file = _write_peak_list(tmp_path, rows)
        mod = MetabolomicsQCModule(_default_config())
        result = mod.analyze(peak_file, {"df_features": features})
        assert result.status == QCStatus.PASS

    def test_half_exceed_cutoff_is_fail(self, tmp_path):
        features = self._features()
        rows = [
            _peak_row("IS1", rt=_LIB_RT + 0.5),   # fail
            _peak_row("IS2", rt=_LIB_RT + 0.4),   # fail
            _peak_row("IS3", rt=_LIB_RT),           # pass
            _peak_row("IS4", rt=_LIB_RT),           # pass
        ]
        peak_file = _write_peak_list(tmp_path, rows)
        mod = MetabolomicsQCModule(_default_config())
        result = mod.analyze(peak_file, {"df_features": features})
        assert result.status == QCStatus.FAIL

    def test_individual_warn_does_not_trigger_overall_warn(self, tmp_path):
        """One standard with delta > cutoff/1.5 (=0.2) but < cutoff (=0.3).
        Only 1 of 2 standards warned → 50% which is NOT > 50% → no overall Warn.
        """
        features = _make_features(["IS1", "IS2"])
        rows = [
            _peak_row("IS1", rt=_LIB_RT + 0.25),   # 0.25 > 0.2 (warn), < 0.3 (fail)
            _peak_row("IS2", rt=_LIB_RT),
        ]
        peak_file = _write_peak_list(tmp_path, rows)
        mod = MetabolomicsQCModule(_default_config())
        result = mod.analyze(peak_file, {"df_features": features})
        assert result.status == QCStatus.PASS

    def test_rt_check_disabled(self, tmp_path):
        features = self._features()
        cfg = _default_config()
        cfg["checks"]["library_rt_enabled"] = False
        cfg["checks"]["library_mz_enabled"] = False
        cfg["checks"]["in_run_rt_enabled"] = False
        rows = [_peak_row(f, rt=99.0) for f in ["IS1", "IS2", "IS3", "IS4"]]
        peak_file = _write_peak_list(tmp_path, rows)
        mod = MetabolomicsQCModule(cfg)
        result = mod.analyze(peak_file, {"df_features": features})
        assert result.status == QCStatus.PASS


# ---------------------------------------------------------------------------
# In-run RT shift
# ---------------------------------------------------------------------------

class TestInRunRTShift:
    def _make_run_rt(self, names, avg_rts):
        """Build a df_run_retention_times with one prior sample row."""
        data = {"Specimen": ["sample_001"]}
        for name, rt in zip(names, avg_rts):
            data[name] = [rt]
        return pd.DataFrame(data)

    def test_in_run_rt_fail(self, tmp_path):
        """Both standards exceed the in-run RT cutoff → FAIL."""
        features = _make_features(["IS1", "IS2"])
        # In-run average: both 1.0. Observed: both 1.25 → delta=0.25 > 0.1 (cutoff)
        df_run_rt = self._make_run_rt(["IS1", "IS2"], [1.0, 1.0])
        rows = [_peak_row("IS1", rt=1.25), _peak_row("IS2", rt=1.25)]
        peak_file = _write_peak_list(tmp_path, rows)
        cfg = _default_config()
        cfg["checks"]["library_rt_enabled"] = False  # isolate in-run check
        mod = MetabolomicsQCModule(cfg)
        result = mod.analyze(peak_file, {
            "df_features": features,
            "df_run_retention_times": df_run_rt,
        })
        # 2/2 = 100% ≥ 50% → FAIL
        assert result.status == QCStatus.FAIL

    def test_in_run_rt_skipped_when_none(self, tmp_path):
        """Without df_run_retention_times the in-run check doesn't fire."""
        features = _make_features(["IS1", "IS2"])
        # RT within library cutoff → no library RT fail either
        rows = [_peak_row("IS1"), _peak_row("IS2")]
        peak_file = _write_peak_list(tmp_path, rows)
        mod = MetabolomicsQCModule(_default_config())
        result = mod.analyze(peak_file, {"df_features": features})
        assert result.status == QCStatus.PASS

    def test_in_run_rt_check_disabled(self, tmp_path):
        features = _make_features(["IS1", "IS2"])
        df_run_rt = self._make_run_rt(["IS1", "IS2"], [1.0, 1.0])
        rows = [_peak_row("IS1", rt=1.25), _peak_row("IS2", rt=1.25)]
        peak_file = _write_peak_list(tmp_path, rows)
        cfg = _default_config()
        cfg["checks"]["in_run_rt_enabled"] = False
        cfg["checks"]["library_rt_enabled"] = False
        mod = MetabolomicsQCModule(cfg)
        result = mod.analyze(peak_file, {
            "df_features": features,
            "df_run_retention_times": df_run_rt,
        })
        assert result.status == QCStatus.PASS


# ---------------------------------------------------------------------------
# Library m/z shift
# ---------------------------------------------------------------------------

class TestLibraryMZShift:
    def _features(self):
        return _make_features(["IS1", "IS2", "IS3", "IS4"])

    def test_within_mz_cutoff_is_pass(self, tmp_path):
        features = self._features()  # all lib mz = _LIB_MZ = 500.0
        rows = [
            _peak_row("IS1", mz=500.001),   # delta = 0.001 < 0.005
            _peak_row("IS2", mz=500.003),
            _peak_row("IS3", mz=500.001),
            _peak_row("IS4", mz=500.000),
        ]
        peak_file = _write_peak_list(tmp_path, rows)
        mod = MetabolomicsQCModule(_default_config())
        result = mod.analyze(peak_file, {"df_features": features})
        assert result.status == QCStatus.PASS

    def test_half_exceed_mz_cutoff_is_fail(self, tmp_path):
        features = self._features()  # all lib mz = 500.0
        rows = [
            _peak_row("IS1", mz=500.01),   # delta = 0.01 > 0.005 → fail
            _peak_row("IS2", mz=500.01),   # fail
            _peak_row("IS3", mz=500.00),   # pass
            _peak_row("IS4", mz=500.00),   # pass
        ]
        peak_file = _write_peak_list(tmp_path, rows)
        mod = MetabolomicsQCModule(_default_config())
        result = mod.analyze(peak_file, {"df_features": features})
        assert result.status == QCStatus.FAIL


# ---------------------------------------------------------------------------
# Duplicate resolution
# ---------------------------------------------------------------------------

class TestDuplicateResolution:
    def test_ms2_backed_preferred_over_no_ms2(self, tmp_path):
        """When one annotation has MS2 and one doesn't, the MS2 hit is used."""
        features = _make_features(["IS1"])
        # MS2 hit has a large RT shift (→ FAIL); no-MS2 hit is at perfect RT (→ PASS).
        # The code should pick the MS2 hit.
        tsv = textwrap.dedent("""\
            Title\tPrecursor m/z\tRT (min)\tHeight\tMSMS spectrum
            IS1\t500.0\t1.5\t1000000\t150/0.99 200/0.5
            w/o MS2:IS1\t500.0\t1.0\t2000000\t
        """)
        p = tmp_path / "peaks.msdial"
        p.write_text(tsv)
        mod = MetabolomicsQCModule(_default_config())
        result = mod.analyze(p, {"df_features": features})
        # MS2-backed hit chosen (rt=1.5); |1.5-1.0|=0.5 > 0.3 → FAIL
        assert result.status == QCStatus.FAIL

    def test_no_ms2_tiebreak_by_closest_rt(self, tmp_path):
        """Two w/o MS2 hits for IS2; closest RT wins. IS1 provides MS2 context."""
        # IS1 has real MS2 → ms2_matching=True → all "w/o MS2:" prefixes stripped globally.
        features = _make_features(["IS1", "IS2"])
        tsv = textwrap.dedent("""\
            Title\tPrecursor m/z\tRT (min)\tHeight\tMSMS spectrum
            IS1\t500.0\t1.0\t1000000\t150/0.99 200/0.5
            w/o MS2:IS2\t500.0\t1.05\t1000000\t
            w/o MS2:IS2\t500.0\t5.00\t2000000\t
        """)
        p = tmp_path / "peaks.msdial"
        p.write_text(tsv)
        mod = MetabolomicsQCModule(_default_config())
        result = mod.analyze(p, {"df_features": features})
        # IS2 closest RT: 1.05, delta = 0.05 < 0.3 → PASS
        assert result.status == QCStatus.PASS


# ---------------------------------------------------------------------------
# Result structure
# ---------------------------------------------------------------------------

class TestResultStructure:
    def test_metrics_keys_present(self, tmp_path):
        features = _make_features(["IS1", "IS2"])
        rows = [_peak_row("IS1"), _peak_row("IS2")]
        peak_file = _write_peak_list(tmp_path, rows)
        mod = MetabolomicsQCModule(_default_config())
        result = mod.analyze(peak_file, {"df_features": features})
        for key in ("total_standards", "detected_standards", "intensity_dropouts",
                    "rt_fails", "in_run_rt_fails", "mz_fails",
                    "rt_warnings", "mz_warnings"):
            assert key in result.metrics, f"Missing metrics key: {key}"
        assert result.metrics["total_standards"] == 2

    def test_details_is_list_of_dicts(self, tmp_path):
        features = _make_features(["IS1"])
        rows = [_peak_row("IS1")]
        peak_file = _write_peak_list(tmp_path, rows)
        mod = MetabolomicsQCModule(_default_config())
        result = mod.analyze(peak_file, {"df_features": features})
        assert isinstance(result.details, list)
        assert isinstance(result.details[0], dict)
        assert "Name" in result.details[0]

    def test_module_name(self, tmp_path):
        mod = MetabolomicsQCModule(_default_config())
        assert mod.name == "metabolomics"
