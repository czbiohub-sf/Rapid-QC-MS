"""Proteomics pre-search QC module.

Runs before FragPipe/database search on raw mzML files.
This is the fast gate: if the file fails here, search is skipped entirely.

QC checks (each independently configurable via qc_modules.toml):

    1. MS1 scan count  — too few MS1 scans indicates a bad acquisition
    2. MS2 scan count  — too few MS2 scans indicates poor fragmentation trigger
    3. MS2/MS1 ratio   — low ratio may indicate instrument issues

Overall status rules:
    - Fail : MS1 count < min_ms1_scans, OR MS2 count < min_ms2_scans
    - Warn : counts between warn threshold (cutoff / warn_ratio) and cutoff,
             OR MS2/MS1 ratio < min_ms2_ms1_ratio
    - Pass : none of the above
"""

import logging
from pathlib import Path

from .base import QCModule, QCResult, QCStatus

log = logging.getLogger(__name__)

_HARD_FAIL_RATIO = 1.33  # count < min/1.33 → hard Fail; count in [min/1.33, min) → Warn


class ProteomicsPreSearchQCModule(QCModule):
    """QC module for proteomics pre-search file validation.

    Parses an mzML file to count MS1/MS2 scans and compare them to
    configured thresholds.

    Expected context keys
    ---------------------
    scan_counts : dict, optional
        Pre-computed scan counts: {"ms1": int, "ms2": int}.
        When present, mzML parsing is skipped (intended for unit tests and
        scenarios where scans were counted by an upstream step).
    instrument_id : str, optional
        Used for logging only.
    """

    @property
    def name(self) -> str:
        return "proteomics_pre"

    def analyze(self, input_path: Path, context: dict) -> QCResult:
        # Allow callers to inject pre-computed counts (testing / upstream pipeline)
        if "scan_counts" in context:
            counts = context["scan_counts"]
            log.debug("Using pre-computed scan counts for %s", input_path.name)
        else:
            counts = self._count_scans(input_path)

        ms1 = counts.get("ms1", 0)
        ms2 = counts.get("ms2", 0)
        ratio = (ms2 / ms1) if ms1 > 0 else 0.0

        status, fails, warnings = self._evaluate(ms1, ms2, ratio)

        metrics = {
            "ms1_count": ms1,
            "ms2_count": ms2,
            "ms2_ms1_ratio": round(ratio, 3),
            "fails": fails,
            "warnings": warnings,
        }
        return QCResult(
            status=status,
            module=self.name,
            metrics=metrics,
        )

    # ------------------------------------------------------------------
    # mzML scan counter
    # ------------------------------------------------------------------

    def _count_scans(self, path: Path) -> dict:
        """Count MS1 and MS2 spectra in an mzML file using pyteomics."""
        try:
            from pyteomics import mzml  # lazy import — not needed for metabolomics
        except ImportError as exc:
            raise ImportError(
                "pyteomics is required for proteomics pre-search QC. "
                "Install it with: pip install pyteomics"
            ) from exc

        ms1, ms2 = 0, 0
        with mzml.MzML(str(path)) as reader:
            for spectrum in reader:
                level = spectrum.get("ms level", 0)
                if level == 1:
                    ms1 += 1
                elif level == 2:
                    ms2 += 1

        log.debug("%s: %d MS1 scans, %d MS2 scans", path.name, ms1, ms2)
        return {"ms1": ms1, "ms2": ms2}

    # ------------------------------------------------------------------
    # Threshold evaluation
    # ------------------------------------------------------------------

    def _evaluate(
        self, ms1: int, ms2: int, ratio: float
    ) -> tuple[QCStatus, list[str], list[str]]:
        thresholds = self.config.get("thresholds", {})
        checks = self.config.get("checks", {})

        min_ms1 = int(thresholds.get("min_ms1_scans", 100))
        min_ms2 = int(thresholds.get("min_ms2_scans", 500))
        min_ratio = float(thresholds.get("min_ms2_ms1_ratio", 2.0))

        status = QCStatus.PASS
        fails: list[str] = []
        warnings: list[str] = []

        # ── MS1 count ──────────────────────────────────────────────────
        # Tier 1 (hard Fail): count < min / 1.33  (≈ 75 % of minimum)
        # Tier 2 (Warn):      count in [min/1.33, min)  (approaching minimum)
        if checks.get("ms1_enabled", True):
            if ms1 < min_ms1 / _HARD_FAIL_RATIO:
                status = QCStatus.FAIL
                fails.append(f"MS1 count {ms1} < hard minimum ({min_ms1 / _HARD_FAIL_RATIO:.0f})")
            elif ms1 < min_ms1:
                warnings.append(f"MS1 count {ms1} below recommended minimum {min_ms1}")
                if status != QCStatus.FAIL:
                    status = QCStatus.WARN

        # ── MS2 count ──────────────────────────────────────────────────
        if checks.get("ms2_enabled", True):
            if ms2 < min_ms2 / _HARD_FAIL_RATIO:
                status = QCStatus.FAIL
                fails.append(f"MS2 count {ms2} < hard minimum ({min_ms2 / _HARD_FAIL_RATIO:.0f})")
            elif ms2 < min_ms2:
                warnings.append(f"MS2 count {ms2} below recommended minimum {min_ms2}")
                if status != QCStatus.FAIL:
                    status = QCStatus.WARN

        # ── MS2/MS1 ratio ──────────────────────────────────────────────
        if checks.get("ratio_enabled", True):
            if ratio < min_ratio and status != QCStatus.FAIL:
                status = QCStatus.WARN
                warnings.append(f"MS2/MS1 ratio {ratio:.2f} < minimum {min_ratio}")

        return status, fails, warnings
