"""Metabolomics QC module using MS-DIAL processed data.

Extracted from AutoQCProcessing.qc_sample() and peak_list_to_dataframe().
All database calls have been replaced by context/config arguments so the
module is fully testable without a database connection.

QC checks (each independently configurable via qc_modules.toml):

    1. Intensity dropouts  — how many internal standards are absent
    2. Library RT shift    — observed RT vs. library expected RT
    3. In-run RT shift     — observed RT vs. running average for this acquisition
    4. Library m/z shift   — observed precursor m/z vs. library expected m/z

Overall status rules:
    - Fail   : any individual check exceeds its cutoff threshold
    - Warn   : dropout count > 75 % of cutoff, OR > 50 % of standards trigger warnings
    - Pass   : none of the above
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from .base import QCModule, QCResult, QCStatus

log = logging.getLogger(__name__)

# Tiebreak constants from the original implementation — kept for reproducibility
_WARN_DROPOUT_RATIO = 1.33   # dropout > cutoff/1.33 → Warn
_WARN_RT_RATIO = 1.5         # delta RT > cutoff/1.5  → individual Warn
_WARN_IN_RUN_RT_RATIO = 1.25 # in-run delta > cutoff/1.25 → individual Warn
_WARN_MZ_RATIO = 1.25        # delta m/z > cutoff/1.25 → individual Warn
_FAIL_FRACTION = 0.5         # ≥ 50 % of standards fail a check → overall Fail
_WARN_FRACTION = 0.5         # > 50 % of standards warn → overall Warn


class MetabolomicsQCModule(QCModule):
    """QC module for untargeted metabolomics runs processed through MS-DIAL.

    Expected context keys
    ---------------------
    df_features : pd.DataFrame
        Internal standards table from the database.
        Required columns: name, precursor_mz, retention_time, ms2_spectrum, inchikey,
                          chromatography, polarity.
    polarity : str, optional
        "Pos" or "Neg" — used only for logging. Defaults to "Pos".
    df_run_retention_times : pd.DataFrame or None, optional
        In-run RT history. Columns: "Specimen" plus one column per internal
        standard containing observed RT values for prior samples in this run.
        Pass None (or omit) to skip in-run RT checks.
    is_bio_standard : bool, optional
        True if the sample is a biological standard. These are currently
        recorded as Pass without grading. Defaults to False.
    """

    @property
    def name(self) -> str:
        return "metabolomics"

    def analyze(self, input_path: Path, context: dict) -> QCResult:
        is_bio_standard = context.get("is_bio_standard", False)
        df_features = context["df_features"]

        if is_bio_standard:
            # Biological standard grading will be added in a future phase.
            log.debug("Skipping grading for biological standard: %s", input_path.name)
            return QCResult(
                status=QCStatus.PASS,
                module=self.name,
                message="Biological standard — grading not yet implemented",
            )

        df_peak_list = self._parse_msdial_peak_list(input_path, df_features)
        qc_df, status = self._check(
            df_peak_list,
            df_features,
            df_run_rt=context.get("df_run_retention_times"),
        )
        return self._build_result(status, qc_df, df_features)

    # ------------------------------------------------------------------
    # Peak list parsing (extracted from peak_list_to_dataframe)
    # ------------------------------------------------------------------

    def _parse_msdial_peak_list(
        self, peak_list_path: Path, df_features: pd.DataFrame
    ) -> pd.DataFrame:
        """Parse an MS-DIAL .msdial peak table and filter it to the target feature list.

        Handles duplicate annotations by preferring the hit with the closest
        m/z and RT to the library value. When MS2 spectra are available,
        identifications backed by MS2 are preferred over RT-only hits.

        Returns a DataFrame with columns:
            Name, Precursor m/z, RT (min), Height, MSMS spectrum
        """
        df = pd.read_csv(peak_list_path, sep="\t", engine="python", skip_blank_lines=True)
        df.rename(columns={"Title": "Name"}, inplace=True)
        df = df[["Name", "Precursor m/z", "RT (min)", "Height", "MSMS spectrum"]]

        feature_list = df_features["name"].astype(str).tolist()
        without_ms2_names = ["w/o MS2:" + f for f in feature_list]
        df = df.loc[df["Name"].isin(feature_list) | df["Name"].isin(without_ms2_names)]

        # Cast to object so pandas accepts the "MS2" string assignment on a
        # column that may have been read as float64 (all-NaN case).
        df["MSMS spectrum"] = df["MSMS spectrum"].astype(object)
        with_ms2 = df["MSMS spectrum"].notnull()
        df.loc[with_ms2, "MSMS spectrum"] = "MS2"

        # If any MS2-backed identifications exist, strip the "w/o MS2:" prefix
        # so all annotations share the same name space.
        ms2_matching = len(df[with_ms2]) > 0
        if ms2_matching:
            df.replace(["w/o MS2:"], "", regex=True, inplace=True)

        df_duplicates = df[df.duplicated(subset=["Name"], keep=False)]
        df = df[~df.duplicated(subset=["Name"], keep=False)]

        if len(df_duplicates) > 0:
            df = self._resolve_duplicates(df, df_duplicates, df_features, ms2_matching)

        try:
            df.drop(columns=["Delta m/z", "Delta RT"], inplace=True)
        except KeyError:
            pass
        finally:
            df.reset_index(drop=True, inplace=True)

        return df

    def _resolve_duplicates(
        self,
        df: pd.DataFrame,
        df_duplicates: pd.DataFrame,
        df_features: pd.DataFrame,
        ms2_matching: bool,
    ) -> pd.DataFrame:
        """Pick the best annotation for each duplicated feature name."""
        annotations = (
            df_duplicates[~df_duplicates.duplicated(subset=["Name"])]["Name"].tolist()
        )

        for annotation in annotations:
            df_ann = df_duplicates[df_duplicates["Name"] == annotation].copy()
            # Strip "w/o MS2:" prefix for the feature lookup regardless of
            # whether ms2_matching caused a global rename.
            feature_name = annotation.replace("w/o MS2:", "")
            lib = df_features.loc[df_features["name"] == feature_name]

            df_ann["Delta m/z"] = (
                df_ann["Precursor m/z"].astype(float) - lib["precursor_mz"].astype(float).values[0]
            ).abs()
            df_ann["Delta RT"] = (
                df_ann["RT (min)"].astype(float) - lib["retention_time"].astype(float).values[0]
            ).abs()

            # Prefer MS2-backed hits when available
            if ms2_matching:
                with_ms2 = df_ann.loc[df_ann["MSMS spectrum"].notnull()]
                if len(with_ms2) > 1:
                    df_ann = with_ms2
                elif len(with_ms2) == 1:
                    # Exactly one MS2 hit — use it directly
                    df = pd.concat([df, with_ms2])
                    continue
                elif len(with_ms2) == 0:
                    # Fall back to highest intensity among non-MS2 hits
                    if len(df_ann) > 1:
                        df_ann = df_ann.loc[df_ann["Height"] == df_ann["Height"].max()]
                    if len(df_ann) == 1:
                        df = pd.concat([df, df_ann])
                        continue

            # Tiebreak: lowest delta RT + delta m/z simultaneously
            best = df_ann.loc[
                (df_ann["Delta m/z"] == df_ann["Delta m/z"].min())
                & (df_ann["Delta RT"] == df_ann["Delta RT"].min())
            ]
            if len(best) == 0:
                best = df_ann.loc[df_ann["Delta RT"] == df_ann["Delta RT"].min()]
            if len(best) > 1:
                best = best.loc[best["Delta m/z"] == best["Delta m/z"].min()]
            if len(best) > 1:
                best = best.loc[best["Height"] == best["Height"].max()]
            if len(best) > 1:
                best = best.iloc[:1]

            df = pd.concat([df, best], ignore_index=True)

        return df

    # ------------------------------------------------------------------
    # QC checks (extracted from qc_sample)
    # ------------------------------------------------------------------

    def _check(
        self,
        df_peak_list: pd.DataFrame,
        df_features: pd.DataFrame,
        df_run_rt: pd.DataFrame | None,
    ) -> tuple[pd.DataFrame, QCStatus]:
        """Run all enabled QC checks. Returns (annotated qc DataFrame, overall status)."""
        thresholds = self.config.get("thresholds", {})
        checks = self.config.get("checks", {})
        post_id_rt_tolerance = self.config.get("post_id_rt_tolerance", 0.5)

        # Normalise feature column names to match the merged DataFrame
        df_feat = df_features.rename(
            columns={
                "name": "Name",
                "chromatography": "Chromatography",
                "polarity": "Polarity",
                "precursor_mz": "Library m/z",
                "retention_time": "Library RT",
                "ms2_spectrum": "Library MS2",
                "inchikey": "Library INCHIKEY",
            }
        )

        df_compare = pd.merge(df_feat, df_peak_list, on="Name")
        df_compare["Delta RT"] = (
            df_compare["RT (min)"].astype(float) - df_compare["Library RT"].astype(float)
        )
        df_compare["Delta m/z"] = (
            df_compare["Precursor m/z"].astype(float) - df_compare["Library m/z"].astype(float)
        )
        df_peak_list_copy = df_peak_list.copy()

        # Drop without-MS2 annotations that fall outside the MS-DIAL RT tolerance
        with_ms2 = df_compare["MSMS spectrum"].notnull()
        without_ms2 = df_compare["MSMS spectrum"].isnull()
        annotations_without_ms2 = df_compare[without_ms2]["Name"].astype(str).tolist()

        if len(df_compare[with_ms2]) > 0:
            outside = df_compare["Delta RT"].abs() > post_id_rt_tolerance
            to_drop = df_compare.loc[without_ms2 & outside]
            df_compare.drop(to_drop.index, inplace=True)
            drop_names = to_drop["Name"].astype(str).tolist()
            df_peak_list_copy = df_peak_list_copy[
                ~df_peak_list_copy["Name"].isin(drop_names)
            ]

        # In-run RT averages
        df_compare["In-run RT average"] = np.nan
        if df_run_rt is not None:
            for col in df_run_rt.columns:
                if col == "Specimen":
                    continue
                avg = df_run_rt[col].dropna().astype(float).mean()
                df_compare.loc[df_compare["Name"] == col, "In-run RT average"] = avg
            df_compare["In-run delta RT"] = (
                df_compare["RT (min)"].astype(float)
                - df_compare["In-run RT average"].astype(float)
            )
        else:
            df_compare["In-run delta RT"] = np.nan

        # Build per-standard QC table with dropout rows for missing standards
        qc_df = df_compare[["Name", "RT (min)", "Delta m/z", "Delta RT", "In-run delta RT"]].copy()
        qc_df["Intensity dropout"] = 0
        qc_df["Warnings"] = ""
        qc_df["Fails"] = ""

        detected = df_peak_list_copy["Name"].astype(str).tolist()
        for feature in df_feat["Name"].astype(str).tolist():
            if feature not in detected:
                missing_row = pd.DataFrame.from_records([{
                    "Name": feature,
                    "RT (min)": np.nan,
                    "Delta m/z": np.nan,
                    "Delta RT": np.nan,
                    "In-run delta RT": np.nan,
                    "Intensity dropout": 1,
                    "Warnings": "",
                    "Fails": "",
                }])
                qc_df = pd.concat([qc_df, missing_row], ignore_index=True)

        status = QCStatus.PASS

        # ── Intensity dropouts ───────────────────────────────────────────
        if checks.get("intensity_enabled", True):
            cutoff = int(thresholds.get("intensity_dropouts_cutoff", 4))
            qc_df.loc[qc_df["Intensity dropout"].astype(int) == 1, "Fails"] = "Missing"
            n_dropouts = int(qc_df["Intensity dropout"].astype(int).sum())

            if n_dropouts >= cutoff:
                status = QCStatus.FAIL
            elif n_dropouts > cutoff / _WARN_DROPOUT_RATIO and status != QCStatus.FAIL:
                status = QCStatus.WARN

        # ── Library RT shift ─────────────────────────────────────────────
        if checks.get("library_rt_enabled", True):
            cutoff = float(thresholds.get("library_rt_shift_cutoff", 0.3))
            fails = qc_df["Delta RT"].abs() > cutoff
            warns = (qc_df["Delta RT"].abs() > cutoff / _WARN_RT_RATIO) & ~fails
            qc_df.loc[fails, "Fails"] = "RT"
            qc_df.loc[warns & (qc_df["Fails"] == ""), "Warnings"] = "RT"

            if len(qc_df.loc[fails]) >= len(qc_df) * _FAIL_FRACTION:
                status = QCStatus.FAIL
            elif len(qc_df.loc[warns]) > len(qc_df) * _WARN_FRACTION and status != QCStatus.FAIL:
                status = QCStatus.WARN

        # ── In-run RT shift ──────────────────────────────────────────────
        if checks.get("in_run_rt_enabled", True) and df_run_rt is not None:
            cutoff = float(thresholds.get("in_run_rt_shift_cutoff", 0.1))
            fails = qc_df["In-run delta RT"].abs() > cutoff
            warns = (
                qc_df["In-run delta RT"].abs() > cutoff / _WARN_IN_RUN_RT_RATIO
            ) & ~fails
            qc_df.loc[fails, "Fails"] = "In-Run RT"
            qc_df.loc[warns & (qc_df["Fails"] == ""), "Warnings"] = "In-Run RT"

            if len(qc_df.loc[fails]) >= len(qc_df) * _FAIL_FRACTION:
                status = QCStatus.FAIL
            elif len(qc_df.loc[warns]) > len(qc_df) * _WARN_FRACTION and status != QCStatus.FAIL:
                status = QCStatus.WARN

        # ── Library m/z shift ────────────────────────────────────────────
        if checks.get("library_mz_enabled", True):
            cutoff = float(thresholds.get("library_mz_shift_cutoff", 0.005))
            fails = qc_df["Delta m/z"].abs() > cutoff
            warns = (qc_df["Delta m/z"].abs() > cutoff / _WARN_MZ_RATIO) & ~fails
            qc_df.loc[fails, "Fails"] = "m/z"
            qc_df.loc[warns & (qc_df["Fails"] == ""), "Warnings"] = "m/z"

            if len(qc_df.loc[fails]) >= len(qc_df) * _FAIL_FRACTION:
                status = QCStatus.FAIL
            elif len(qc_df.loc[warns]) > len(qc_df) * _WARN_FRACTION and status != QCStatus.FAIL:
                status = QCStatus.WARN

        # Annotate no-MS2 warnings (informational, doesn't affect overall status)
        qc_df.loc[qc_df["Name"].isin(annotations_without_ms2), "Warnings"] = "No MS2"

        return qc_df, status

    # ------------------------------------------------------------------
    # Result builder
    # ------------------------------------------------------------------

    def _build_result(
        self, status: QCStatus, qc_df: pd.DataFrame, df_features: pd.DataFrame
    ) -> QCResult:
        n_total = len(df_features)
        n_dropouts = int(qc_df["Intensity dropout"].astype(int).sum()) if "Intensity dropout" in qc_df.columns else 0

        metrics = {
            "total_standards": n_total,
            "detected_standards": n_total - n_dropouts,
            "intensity_dropouts": n_dropouts,
            "rt_fails": int((qc_df.get("Fails", pd.Series(dtype=str)) == "RT").sum()),
            "in_run_rt_fails": int((qc_df.get("Fails", pd.Series(dtype=str)) == "In-Run RT").sum()),
            "mz_fails": int((qc_df.get("Fails", pd.Series(dtype=str)) == "m/z").sum()),
            "rt_warnings": int((qc_df.get("Warnings", pd.Series(dtype=str)) == "RT").sum()),
            "mz_warnings": int((qc_df.get("Warnings", pd.Series(dtype=str)) == "m/z").sum()),
        }

        return QCResult(
            status=status,
            module=self.name,
            metrics=metrics,
            details=qc_df.fillna("").to_dict(orient="records"),
        )
