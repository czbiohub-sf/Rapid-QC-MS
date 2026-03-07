"""Metabolomics pre-search QC module.

Runs on individual mzML files before MS-DIAL alignment.  For each internal
standard (IS) configured in the database, this module extracts a targeted
XIC (extracted ion chromatogram) and checks whether the IS was detected.

QC checks:
    1. IS fill fraction  — fraction of configured IS detected above threshold
    2. RT deviation      — peak apex RT vs expected library RT
    3. m/z deviation     — measured m/z vs expected library m/z

Overall status rules:
    Fail : any detected IS has RT deviation > rt_deviation_fail (default 1.0 min)
           OR any detected IS has m/z deviation > mz_deviation_fail_ppm (default 8 ppm)
    Warn : fill_fraction < warn_fill_fraction (default 0.90) — IS not detected is a
           warning, not a failure; only detected-but-wrong IS can Fail
           OR any detected IS RT deviation > rt_deviation_warn (default 0.5 min)
           OR any detected IS m/z deviation > mz_deviation_warn_ppm (default 5 ppm)
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
        mz_ppm         = float(thresholds.get("mz_ppm",              10.0))
        rt_window      = float(thresholds.get("rt_window_min",        0.75))
        min_intensity  = float(thresholds.get("min_intensity",        1e4))
        warn_fill      = float(thresholds.get("warn_fill_fraction",   0.90))
        fail_fill      = float(thresholds.get("fail_fill_fraction",   0.70))
        rt_dev_warn    = float(thresholds.get("rt_deviation_warn",    0.50))
        rt_dev_fail    = float(thresholds.get("rt_deviation_fail",    1.00))
        mz_dev_warn    = float(thresholds.get("mz_deviation_warn_ppm", 5.0))
        mz_dev_fail    = float(thresholds.get("mz_deviation_fail_ppm", 8.0))

        best_intensity, best_rt, best_mz = self._extract_xics(
            input_path, is_list, mz_ppm=mz_ppm, rt_window=rt_window
        )

        detected, missing = [], []
        rt_devs: dict[str, float] = {}
        mz_devs_ppm: dict[str, float] = {}
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
                measured_mz = best_mz.get(nm)
                if measured_mz is not None and mz_expected > 0:
                    mz_devs_ppm[nm] = round(
                        (measured_mz - mz_expected) / mz_expected * 1e6, 4
                    )
            else:
                missing.append(nm)

        n_total = len(is_list)
        n_detected = len(detected)
        fill_fraction = n_detected / n_total if n_total else 0.0

        # Classify IS by RT and m/z issue severity
        rt_warn_is = [nm for nm in detected if abs(rt_devs.get(nm, 0.0)) > rt_dev_warn]
        rt_fail_is = [nm for nm in detected if abs(rt_devs.get(nm, 0.0)) > rt_dev_fail]
        mz_warn_is = [nm for nm in detected if abs(mz_devs_ppm.get(nm, 0.0)) > mz_dev_warn]
        mz_fail_is = [nm for nm in detected if abs(mz_devs_ppm.get(nm, 0.0)) > mz_dev_fail]

        # Not detected → at most Warn. Only detected-but-wrong IS can Fail.
        if rt_fail_is or mz_fail_is:
            status = QCStatus.FAIL
        elif fill_fraction < warn_fill or rt_warn_is or mz_warn_is:
            status = QCStatus.WARN
        else:
            status = QCStatus.PASS

        metrics = {
            "fill_fraction":    round(fill_fraction, 4),
            "n_detected":       n_detected,
            "n_total":          n_total,
            "detected_is":      detected,
            "missing_is":       missing,
            "rt_deviations":    rt_devs,
            "mz_deviations_ppm": mz_devs_ppm,
            "rt_warn_is":       rt_warn_is,
            "rt_fail_is":       rt_fail_is,
            "mz_warn_is":       mz_warn_is,
            "mz_fail_is":       mz_fail_is,
            "peak_intensities": {
                nm: round(best_intensity[nm], 1)
                for nm in detected
            },
        }

        # Per-IS detail records used by the plot layer
        details = []
        for is_entry in is_list:
            nm  = is_entry["name"]           if isinstance(is_entry, dict) else is_entry.name
            ert = is_entry["retention_time"] if isinstance(is_entry, dict) else is_entry.retention_time
            intensity = best_intensity.get(nm, 0.0)
            is_detected = intensity >= min_intensity
            apex_rt = best_rt.get(nm) if is_detected else None
            delta_rt = round(apex_rt - ert, 4) if apex_rt is not None else None
            delta_mz = mz_devs_ppm.get(nm) if is_detected else None
            warns = []
            if delta_rt is not None and abs(delta_rt) > rt_dev_warn:
                warns.append(f"RT dev {delta_rt:+.2f} min (>{rt_dev_warn} min)")
            if delta_mz is not None and abs(delta_mz) > mz_dev_warn:
                warns.append(f"m/z dev {delta_mz:+.1f} ppm (>{mz_dev_warn} ppm)")
            details.append({
                "Name":             nm,
                "RT (min)":         round(apex_rt, 4) if apex_rt is not None else None,
                "Height":           round(intensity, 1),
                "Delta RT":         delta_rt,
                "In-run delta RT":  None,
                "Delta m/z":        delta_mz,
                "Warnings":         "; ".join(warns),
                "Fails":            "" if is_detected else f"{nm} not detected",
            })

        # Build per-check grades
        grades: dict = {}

        if fill_fraction < warn_fill:
            grades["is_fill_fraction"] = {
                "status": "Warn",
                "message": f"IS detection {n_detected}/{n_total} ({fill_fraction:.1%}) < {warn_fill:.0%} threshold",
            }
        else:
            grades["is_fill_fraction"] = {"status": "Pass", "message": None}

        if rt_fail_is:
            worst_nm = max(rt_fail_is, key=lambda k: abs(rt_devs[k]))
            grades["is_rt_deviation"] = {
                "status": "Fail",
                "message": f"{worst_nm}: {rt_devs[worst_nm]:+.2f} min (>{rt_dev_fail} min threshold)",
            }
        elif rt_warn_is:
            worst_nm = max(rt_warn_is, key=lambda k: abs(rt_devs[k]))
            grades["is_rt_deviation"] = {
                "status": "Warn",
                "message": f"{worst_nm}: {rt_devs[worst_nm]:+.2f} min (>{rt_dev_warn} min threshold)",
            }
        else:
            grades["is_rt_deviation"] = {"status": "Pass", "message": None}

        if mz_fail_is:
            worst_nm = max(mz_fail_is, key=lambda k: abs(mz_devs_ppm[k]))
            grades["is_mz_shift"] = {
                "status": "Fail",
                "message": f"{worst_nm}: {mz_devs_ppm[worst_nm]:+.1f} ppm (>{mz_dev_fail} ppm threshold)",
            }
        elif mz_warn_is:
            worst_nm = max(mz_warn_is, key=lambda k: abs(mz_devs_ppm[k]))
            grades["is_mz_shift"] = {
                "status": "Warn",
                "message": f"{worst_nm}: {mz_devs_ppm[worst_nm]:+.1f} ppm (>{mz_dev_warn} ppm threshold)",
            }
        else:
            grades["is_mz_shift"] = {"status": "Pass", "message": None}

        msg_parts = [f"IS detected: {n_detected}/{n_total}"]
        if missing:
            msg_parts.append(f"missing: {', '.join(missing[:3])}{'...' if len(missing) > 3 else ''}")
        if rt_fail_is:
            worst_nm = max(rt_fail_is, key=lambda k: abs(rt_devs[k]))
            msg_parts.append(f"RT fail {worst_nm}={rt_devs[worst_nm]:+.2f} min")
        elif rt_warn_is:
            worst_nm = max(rt_warn_is, key=lambda k: abs(rt_devs[k]))
            msg_parts.append(f"RT warn {worst_nm}={rt_devs[worst_nm]:+.2f} min")
        if mz_warn_is:
            worst_nm = max(mz_warn_is, key=lambda k: abs(mz_devs_ppm[k]))
            msg_parts.append(f"m/z warn {worst_nm}={mz_devs_ppm[worst_nm]:+.1f} ppm")

        return QCResult(
            status=status,
            module=self.name,
            metrics=metrics,
            details=details,
            message="; ".join(msg_parts),
            grades=grades,
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
    ) -> tuple[dict, dict, dict]:
        """Return (best_intensity, best_rt, best_mz) dicts keyed by IS name.

        Scans all MS1 spectra in the mzML file and records the maximum
        intensity found within the RT window and m/z tolerance for each IS.
        best_mz holds the measured m/z of the most intense peak at the apex scan.
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
        best_mz: dict[str, float | None] = {nm: None for nm in names}

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
                    masked_ints = int_arr[mask]
                    peak_idx = masked_ints.argmax()
                    local_max = float(masked_ints[peak_idx])
                    nm = names[idx]
                    if local_max > best_intensity[nm]:
                        best_intensity[nm] = local_max
                        best_rt[nm] = rt
                        best_mz[nm] = float(mz_arr[mask][peak_idx])

        return best_intensity, best_rt, best_mz
