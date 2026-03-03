"""Metabolomics pre-search QC module.

Runs on individual mzML files before MS-DIAL alignment.  For each internal
standard (IS) configured in the database, this module extracts a targeted
XIC (extracted ion chromatogram) and checks whether the IS was detected.

QC checks:
    1. IS fill fraction  — fraction of configured IS detected above threshold
    2. RT deviation      — peak apex RT vs expected library RT

Overall status rules:
    Fail : fill_fraction < fail_fill_fraction  (default 0.70)
    Warn : fill_fraction < warn_fill_fraction  (default 0.90)
           OR any detected IS RT deviation > rt_deviation_warn (default 0.5 min)
    Pass : otherwise

If no IS are configured for the given polarity/chromatography the module
returns Pass with an explanatory message so the pipeline isn't blocked.
"""

import logging
from pathlib import Path

import numpy as np

from .base import QCModule, QCResult, QCStatus

log = logging.getLogger(__name__)


class MetabolomicsPreSearchQCModule(QCModule):
    """Pre-search IS detection QC for metabolomics mzML files.

    Expected context keys
    ---------------------
    polarity : str
        "Pos" or "Neg" — used to select IS from DB.
    chromatography : str, optional
        Chromatography method label (default "HILIC").
    internal_standards : list[InternalStandard | dict], optional
        Pre-loaded IS (skips DB lookup).  Each item must have attributes /
        keys: name, precursor_mz, retention_time.
    """

    @property
    def name(self) -> str:
        return "metabolomics_pre"

    def analyze(self, input_path: Path, context: dict) -> QCResult:
        is_list = self._load_is(context)

        if not is_list:
            return QCResult(
                status=QCStatus.PASS,
                module=self.name,
                message="No internal standards configured — skipping IS QC",
            )

        thresholds = self.config.get("thresholds", {})
        mz_ppm       = float(thresholds.get("mz_ppm",            10.0))
        rt_window    = float(thresholds.get("rt_window_min",      0.75))
        min_intensity = float(thresholds.get("min_intensity",     1e4))
        warn_fill    = float(thresholds.get("warn_fill_fraction", 0.90))
        fail_fill    = float(thresholds.get("fail_fill_fraction", 0.70))
        rt_dev_warn  = float(thresholds.get("rt_deviation_warn",  0.50))

        best_intensity, best_rt = self._extract_xics(
            input_path, is_list, mz_ppm=mz_ppm, rt_window=rt_window
        )

        detected, missing, rt_devs = [], [], {}
        for is_entry in is_list:
            nm = is_entry["name"] if isinstance(is_entry, dict) else is_entry.name
            mz_expected = is_entry["precursor_mz"] if isinstance(is_entry, dict) else is_entry.precursor_mz
            rt_expected = is_entry["retention_time"] if isinstance(is_entry, dict) else is_entry.retention_time

            intensity = best_intensity.get(nm, 0.0)
            if intensity >= min_intensity:
                detected.append(nm)
                apex_rt = best_rt.get(nm)
                if apex_rt is not None:
                    rt_devs[nm] = round(apex_rt - rt_expected, 4)
            else:
                missing.append(nm)

        n_total = len(is_list)
        n_detected = len(detected)
        fill_fraction = n_detected / n_total if n_total else 0.0

        # Determine worst RT deviation across detected IS
        max_rt_dev = max(abs(v) for v in rt_devs.values()) if rt_devs else 0.0

        if fill_fraction < fail_fill:
            status = QCStatus.FAIL
        elif fill_fraction < warn_fill or max_rt_dev > rt_dev_warn:
            status = QCStatus.WARN
        else:
            status = QCStatus.PASS

        metrics = {
            "fill_fraction": round(fill_fraction, 4),
            "n_detected":    n_detected,
            "n_total":       n_total,
            "detected_is":   detected,
            "missing_is":    missing,
            "rt_deviations": rt_devs,
            "peak_intensities": {
                nm: round(best_intensity[nm], 1)
                for nm in detected
            },
        }

        msg_parts = [f"IS detected: {n_detected}/{n_total}"]
        if missing:
            msg_parts.append(f"missing: {', '.join(missing[:3])}{'...' if len(missing) > 3 else ''}")
        if max_rt_dev > rt_dev_warn:
            worst_nm = max(rt_devs, key=lambda k: abs(rt_devs[k]))
            msg_parts.append(f"RT dev {worst_nm}={rt_devs[worst_nm]:+.2f} min")

        return QCResult(
            status=status,
            module=self.name,
            metrics=metrics,
            message="; ".join(msg_parts),
        )

    # ------------------------------------------------------------------
    # IS loader
    # ------------------------------------------------------------------

    def _load_is(self, context: dict) -> list:
        """Load IS from context (if pre-loaded) or from the DB."""
        if "internal_standards" in context:
            return context["internal_standards"]

        polarity       = context.get("polarity", "Pos")
        chromatography = context.get("chromatography", "HILIC")

        from rapidqcms.config.library import get_internal_standards
        entries = get_internal_standards(chromatography, polarity)
        return [
            {"name": e["name"], "precursor_mz": e["mz"], "retention_time": e["rt"]}
            for e in entries
        ]

    # ------------------------------------------------------------------
    # XIC extraction
    # ------------------------------------------------------------------

    def _extract_xics(
        self,
        path: Path,
        is_list: list,
        mz_ppm: float,
        rt_window: float,
    ) -> tuple[dict, dict]:
        """Return (best_intensity, best_rt) dicts keyed by IS name.

        Scans all MS1 spectra in the mzML file and records the maximum
        intensity found within the RT window and m/z tolerance for each IS.
        """
        try:
            from pyteomics import mzml as pymzml
        except ImportError as exc:
            raise ImportError(
                "pyteomics is required for metabolomics pre-search QC. "
                "Install it with: pip install pyteomics"
            ) from exc

        # Build lookup lists for fast matching
        names, expected_mzs, expected_rts, mz_tols = [], [], [], []
        for entry in is_list:
            nm  = entry["name"]           if isinstance(entry, dict) else entry.name
            emz = entry["precursor_mz"]   if isinstance(entry, dict) else entry.precursor_mz
            ert = entry["retention_time"] if isinstance(entry, dict) else entry.retention_time
            names.append(nm)
            expected_mzs.append(emz)
            expected_rts.append(ert)
            mz_tols.append(emz * mz_ppm / 1e6)

        best_intensity: dict[str, float] = {nm: 0.0 for nm in names}
        best_rt: dict[str, float | None] = {nm: None for nm in names}

        expected_mzs_arr = np.array(expected_mzs)
        expected_rts_arr = np.array(expected_rts)
        mz_tols_arr      = np.array(mz_tols)
        rt_lo_arr        = expected_rts_arr - rt_window
        rt_hi_arr        = expected_rts_arr + rt_window

        with pymzml.MzML(str(path)) as reader:
            for spec in reader:
                if spec.get("ms level") != 1:
                    continue

                # Extract scan RT (minutes)
                scan_list = spec.get("scanList", {})
                scans = scan_list.get("scan", [{}])
                rt = scans[0].get("scan start time", 0.0) if scans else 0.0
                # Handle seconds-encoded RT (runs are always < 120 min)
                if hasattr(rt, "unit_info") and rt.unit_info == "second":
                    rt = float(rt) / 60.0
                else:
                    rt = float(rt)

                mz_arr  = np.asarray(spec.get("m/z array",        []), dtype=float)
                int_arr = np.asarray(spec.get("intensity array",   []), dtype=float)
                if mz_arr.size == 0:
                    continue

                # Which IS have their RT window covering this scan?
                candidates = np.where((rt >= rt_lo_arr) & (rt <= rt_hi_arr))[0]
                for idx in candidates:
                    lo = expected_mzs_arr[idx] - mz_tols_arr[idx]
                    hi = expected_mzs_arr[idx] + mz_tols_arr[idx]
                    mask = (mz_arr >= lo) & (mz_arr <= hi)
                    if not mask.any():
                        continue
                    local_max = float(int_arr[mask].max())
                    nm = names[idx]
                    if local_max > best_intensity[nm]:
                        best_intensity[nm] = local_max
                        best_rt[nm] = rt

        return best_intensity, best_rt
