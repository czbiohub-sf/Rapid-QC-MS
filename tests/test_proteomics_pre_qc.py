"""Tests for ProteomicsPreSearchQCModule.

Uses context-injected scan_counts to avoid requiring pyteomics and
real mzML files in unit tests.
"""

from pathlib import Path

import pytest

from rapidqcms.qc.base import QCStatus
from rapidqcms.qc.proteomics_pre import ProteomicsPreSearchQCModule


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _default_config(**threshold_overrides):
    cfg = {
        "thresholds": {
            "min_ms1_scans": 100,
            "min_ms2_scans": 500,
            "min_ms2_ms1_ratio": 2.0,
        },
        "checks": {
            "ms1_enabled": True,
            "ms2_enabled": True,
            "ratio_enabled": True,
        },
    }
    cfg["thresholds"].update(threshold_overrides)
    return cfg


def _analyze(scan_counts, config=None, path=None):
    """Run the module with pre-computed scan counts (no mzML needed)."""
    mod = ProteomicsPreSearchQCModule(config or _default_config())
    p = path or Path("/fake/sample.mzML")
    return mod.analyze(p, {"scan_counts": scan_counts})


# ---------------------------------------------------------------------------
# Pass scenarios
# ---------------------------------------------------------------------------

class TestPassScenario:
    def test_all_above_thresholds_is_pass(self):
        result = _analyze({"ms1": 200, "ms2": 1000})
        assert result.status == QCStatus.PASS
        assert result.metrics["ms1_count"] == 200
        assert result.metrics["ms2_count"] == 1000

    def test_ratio_above_minimum_is_pass(self):
        # ms2/ms1 = 2.5 > 2.0 → no warn
        result = _analyze({"ms1": 200, "ms2": 500})
        assert result.status == QCStatus.PASS

    def test_module_name(self):
        mod = ProteomicsPreSearchQCModule(_default_config())
        assert mod.name == "proteomics_pre"


# ---------------------------------------------------------------------------
# MS1 failures
# ---------------------------------------------------------------------------

class TestMS1Count:
    def test_below_min_ms1_is_fail(self):
        result = _analyze({"ms1": 50, "ms2": 1000})
        assert result.status == QCStatus.FAIL
        assert any("MS1" in f for f in result.metrics["fails"])

    def test_above_min_ms1_is_pass(self):
        result = _analyze({"ms1": 100, "ms2": 1000})
        assert result.status == QCStatus.PASS

    def test_ms1_warn_threshold(self):
        """ms1=85 is in [100/1.33≈75, 100) → Warn (approaching minimum)."""
        result = _analyze({"ms1": 85, "ms2": 1000})
        assert result.status == QCStatus.WARN
        assert any("MS1" in w for w in result.metrics["warnings"])

    def test_ms1_check_disabled(self):
        cfg = _default_config()
        cfg["checks"]["ms1_enabled"] = False
        # ms1=0: ms1 check skipped; ratio=0/ms1... ms1=0 → ratio=0 < 2.0 → Warn
        result = _analyze({"ms1": 0, "ms2": 1000}, config=cfg)
        # ratio fires (ms2/ms1 where ms1=0 → ratio=0 < 2.0); ms1 check disabled
        assert result.status == QCStatus.WARN  # ratio fires; ms1 check disabled

    def test_ms1_check_disabled_all_checks_disabled(self):
        cfg = _default_config()
        cfg["checks"]["ms1_enabled"] = False
        cfg["checks"]["ms2_enabled"] = False
        cfg["checks"]["ratio_enabled"] = False
        result = _analyze({"ms1": 0, "ms2": 0}, config=cfg)
        assert result.status == QCStatus.PASS


# ---------------------------------------------------------------------------
# MS2 failures
# ---------------------------------------------------------------------------

class TestMS2Count:
    def test_below_min_ms2_is_fail(self):
        result = _analyze({"ms1": 200, "ms2": 200})
        assert result.status == QCStatus.FAIL
        assert any("MS2" in f for f in result.metrics["fails"])

    def test_above_min_ms2_is_pass(self):
        result = _analyze({"ms1": 200, "ms2": 500})
        assert result.status == QCStatus.PASS

    def test_ms2_warn_threshold(self):
        """ms2=420 is in [500/1.33≈376, 500) → Warn (approaching minimum)."""
        result = _analyze({"ms1": 200, "ms2": 420})
        assert result.status == QCStatus.WARN
        assert any("MS2" in w for w in result.metrics["warnings"])

    def test_ms2_check_disabled(self):
        cfg = _default_config()
        cfg["checks"]["ms2_enabled"] = False
        result = _analyze({"ms1": 200, "ms2": 0}, config=cfg)
        # ratio = 0/200 = 0 < 2.0 → Warn (ratio check still enabled)
        assert result.status == QCStatus.WARN


# ---------------------------------------------------------------------------
# MS2/MS1 ratio
# ---------------------------------------------------------------------------

class TestRatioCheck:
    def test_low_ratio_is_warn_not_fail(self):
        # ms1=400, ms2=650 → both above min thresholds; ratio=1.625 < 2.0 → Warn only
        result = _analyze({"ms1": 400, "ms2": 650})
        assert result.status == QCStatus.WARN
        assert any("ratio" in w.lower() for w in result.metrics["warnings"])

    def test_ratio_check_disabled(self):
        cfg = _default_config()
        cfg["checks"]["ratio_enabled"] = False
        result = _analyze({"ms1": 200, "ms2": 300}, config=cfg)
        # ms2 >= min_ms2 (300 < 500 → FAIL actually)
        assert result.status == QCStatus.FAIL  # ms2 check fires

    def test_ratio_warn_does_not_override_fail(self):
        """When MS1 count fails, low ratio adds warn but doesn't change FAIL."""
        result = _analyze({"ms1": 50, "ms2": 100})
        assert result.status == QCStatus.FAIL
        # fails should mention MS1
        assert any("MS1" in f for f in result.metrics["fails"])

    def test_zero_ms1_does_not_crash(self):
        """When ms1=0, ratio = 0 and code should not divide by zero."""
        cfg = _default_config()
        cfg["checks"]["ms1_enabled"] = False
        result = _analyze({"ms1": 0, "ms2": 1000}, config=cfg)
        assert result.metrics["ms2_ms1_ratio"] == 0.0


# ---------------------------------------------------------------------------
# Result structure
# ---------------------------------------------------------------------------

class TestResultStructure:
    def test_metrics_keys_present(self):
        result = _analyze({"ms1": 200, "ms2": 1000})
        for key in ("ms1_count", "ms2_count", "ms2_ms1_ratio", "fails", "warnings"):
            assert key in result.metrics, f"Missing metrics key: {key}"

    def test_ratio_rounded_to_three_decimals(self):
        result = _analyze({"ms1": 3, "ms2": 10})
        assert result.metrics["ms2_ms1_ratio"] == round(10 / 3, 3)
