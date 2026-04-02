"""Dashboard data layer.

Replaces PlotGeneration.get_qc_results() / DatabaseFunctions parse_* functions.
Reads QCResult.details from the DB and pivots into the wide DataFrames that the
plot functions expect.
"""

from __future__ import annotations

import io
import json
import logging

import pandas as pd
from sqlalchemy.orm import Session

from rapidqcms.config.library import get_internal_standards_df
from rapidqcms.db.features import list_bio_standards
from rapidqcms.db.models import QCResult as QCResultModel
from rapidqcms.db.settings import get_run

log = logging.getLogger(__name__)


def get_results_for_run(
    session: Session,
    instrument_id: str,
    run_id: str,
    qc_module: str | None = None,
) -> list[QCResultModel]:
    """Return all QCResult rows for a run, ordered by acquisition time.

    If qc_module is given, filter to that module only (e.g. "metabolomics").
    If None (default), return results for all modules so proteomics and
    metabolomics runs both populate the sample table correctly.
    """
    q = (
        session.query(QCResultModel)
        .filter_by(instrument_id=instrument_id, run_id=run_id)
    )
    if qc_module is not None:
        q = q.filter(QCResultModel.qc_module == qc_module)
    return q.order_by(QCResultModel.acquired_at).all()


def _pivot_field(
    rows: list[QCResultModel],
    field: str,
    polarity_map: dict[str, str],
    target_polarity: str,
) -> pd.DataFrame | None:
    """Build a wide pivot DataFrame for a given detail field and polarity.

    Returns a DataFrame with columns ["Specimen"] + IS names, or None if empty.
    """
    records: list[dict] = []
    for row in rows:
        if polarity_map.get(row.sample_id, "Pos") != target_polarity:
            continue
        if not row.details:
            continue
        record: dict = {"Specimen": row.sample_id}
        for entry in row.details:
            name = entry.get("Name")
            if name:
                record[name] = entry.get(field, "")
        records.append(record)

    if not records:
        return None
    return pd.DataFrame(records)


_GRADES_KEYWORD_MAP = {
    # metabolomics batch module: map warning/fail message keywords → check name
    "dropout":  ["dropout", "Dropout"],
    "cv":       ["CV", " cv "],
    "in_range": ["within", "median", "range"],
}


def _grades_from_metrics(qc_module: str, metrics: dict) -> dict:
    """Derive per-check grades from metrics for rows written before the grades field existed.

    For the 'metabolomics' batch module, parses the ``warnings`` and ``fails``
    lists in metrics and maps them back to named checks via keyword matching.
    Returns an empty dict for unknown modules.
    """
    if not metrics:
        return {}

    if qc_module == "metabolomics":
        warns = metrics.get("warnings") or []
        fails = metrics.get("fails") or []
        grades: dict = {}
        for check_name, keywords in _GRADES_KEYWORD_MAP.items():
            if not any(k in metrics for k in ["dropout_pct", "mean_cv_pct", "in_range_pct"]):
                continue
            fail_msg = next(
                (m for m in fails if any(kw in m for kw in keywords)), None
            )
            warn_msg = next(
                (m for m in warns if any(kw in m for kw in keywords)), None
            )
            if fail_msg:
                grades[check_name] = {"status": "Fail", "message": fail_msg}
            elif warn_msg:
                grades[check_name] = {"status": "Warn", "message": warn_msg}
            else:
                grades[check_name] = {"status": "Pass", "message": None}
        return grades

    return {}


_CHECK_ABBREV = {
    "fill_fraction": "fill",
    "rt_deviation":  "RT",
    "mz_shift":      "m/z",
    "dropout":       "dropout",
    "cv":            "CV",
    "in_range":      "in_range",
}


def _format_qc(grades: dict) -> str:
    """Format grades as a compact string showing only non-Pass checks.

    Pass rows return an empty string (row colour already conveys status).
    Warn/Fail checks appear as ``RT:Warn | m/z:Warn``.
    """
    if not grades:
        return ""
    parts = []
    for check, entry in grades.items():
        st = entry.get("status", "Pass")
        if st == "Pass":
            continue
        short = check[3:] if check.startswith("is_") else check
        label = _CHECK_ABBREV.get(short, short)
        parts.append(f"{label}:{st}")
    return " | ".join(parts)


def get_sample_table(
    session: Session,
    instrument_id: str,
    run_id: str,
) -> pd.DataFrame:
    """Return [Specimen, Status, QC] from QCResult rows for the sample table.

    Status is the overall Pass/Warn/Fail used for row colour-coding.
    QC is a per-check modular breakdown string built from the grades field
    (or derived from metrics for legacy rows without grades).
    """
    rows = get_results_for_run(session, instrument_id, run_id)
    records = []
    for row in rows:
        grades = row.grades or _grades_from_metrics(row.qc_module, row.metrics or {})
        records.append({
            "Specimen": row.sample_id,
            "Status":   row.status,
            "QC":       _format_qc(grades),
        })
    return pd.DataFrame(records)


def get_run_dataframes(
    session: Session,
    instrument_id: str,
    run_id: str,
    chromatography: str | None = None,
) -> dict:
    """Reconstruct plot-ready DataFrames from QCResult.details.

    QCResult.details rows contain:
        Name, RT (min), Height, Delta m/z, Delta RT, In-run delta RT,
        Intensity dropout, Warnings, Fails

    Returns a dict with keys (each a JSON-serialised DataFrame or list):
      df_rt_pos / df_rt_neg
      df_intensity_pos / df_intensity_neg
      df_mz_pos / df_mz_neg
      df_delta_rt_pos / df_delta_rt_neg
      df_in_run_delta_rt_pos / df_in_run_delta_rt_neg
      df_delta_mz_pos / df_delta_mz_neg
      df_warnings_pos / df_warnings_neg
      df_fails_pos / df_fails_neg
      df_samples    — JSON records: [Specimen, Position, QC, Polarity]
      resources     — JSON: {instrument, run_id, chromatography, ...}
      pos_internal_standards / neg_internal_standards — JSON lists
    """
    rows = get_results_for_run(session, instrument_id, run_id)

    # Determine chromatography from the run record if not provided
    if chromatography is None:
        run = get_run(session, run_id)
        chromatography = (run.chromatography if run and run.chromatography else None)
        if chromatography is None:
            log.warning(
                "Run %s has no chromatography set; defaulting to HILIC", run_id
            )
            chromatography = "HILIC"

    # Build polarity lookup: IS library has polarity per standard name.
    # We infer sample polarity from which IS library produced results.
    # Strategy: if a sample's details contain standards found in Pos IS lib → Pos, else Neg.
    pos_is_df = get_internal_standards_df(chromatography, "Pos")
    neg_is_df = get_internal_standards_df(chromatography, "Neg")
    pos_names = set(pos_is_df["name"].tolist()) if not pos_is_df.empty else set()
    neg_names = set(neg_is_df["name"].tolist()) if not neg_is_df.empty else set()

    # Build precursor m/z dicts for resources
    precursor_mz_dict: dict[str, float] = {}
    retention_times_dict: dict[str, float] = {}
    for _, r in pos_is_df.iterrows():
        precursor_mz_dict[r["name"]] = r["precursor_mz"]
        retention_times_dict[r["name"]] = r["retention_time"]
    for _, r in neg_is_df.iterrows():
        precursor_mz_dict[r["name"]] = r["precursor_mz"]
        retention_times_dict[r["name"]] = r["retention_time"]

    # Determine polarity for each sample.
    # Primary: look for _Neg_ / _Pos_ tokens in the sample_id (most reliable).
    # Fallback: compare exclusive (non-overlapping) IS names in details.
    _neg_only = neg_names - pos_names
    _pos_only = pos_names - neg_names

    def _polarity_from_id(sample_id: str) -> str | None:
        for tok in sample_id.replace("-", "_").split("_"):
            if tok.lower() == "neg":
                return "Neg"
            if tok.lower() == "pos":
                return "Pos"
        return None

    polarity_map: dict[str, str] = {}
    sample_records: list[dict] = []
    for row in rows:
        polarity = _polarity_from_id(row.sample_id)
        if polarity is None:
            # Fallback: use exclusive IS names in details
            if row.details:
                names_in_details = {e.get("Name") for e in row.details if e.get("Name")}
                if names_in_details & _neg_only and not (names_in_details & _pos_only):
                    polarity = "Neg"
                else:
                    polarity = "Pos"
            else:
                polarity = "Pos"
        polarity_map[row.sample_id] = polarity
        grades = row.grades
        if not grades and row.metrics:
            grades = _grades_from_metrics(row.qc_module or "", row.metrics)
        missing_is = (row.metrics or {}).get("missing_is", [])
        notes = f"missing: {', '.join(missing_is)}" if missing_is else ""
        sample_records.append({
            "Specimen": row.sample_id,
            "Position": "",
            "Status":   row.status,
            "Notes":    notes,
            "QC":       _format_qc(grades or {}),
            "Polarity": polarity,
        })

    def _to_json(df: pd.DataFrame | None) -> str | None:
        if df is None:
            return None
        return df.to_json(orient="records")

    result: dict = {}

    for pol_key, polarity in (("pos", "Pos"), ("neg", "Neg")):
        result[f"df_rt_{pol_key}"] = _to_json(
            _pivot_field(rows, "RT (min)", polarity_map, polarity)
        )
        result[f"df_intensity_{pol_key}"] = _to_json(
            _pivot_field(rows, "Height", polarity_map, polarity)
        )
        result[f"df_delta_rt_{pol_key}"] = _to_json(
            _pivot_field(rows, "Delta RT", polarity_map, polarity)
        )
        result[f"df_in_run_delta_rt_{pol_key}"] = _to_json(
            _pivot_field(rows, "In-run delta RT", polarity_map, polarity)
        )
        result[f"df_delta_mz_{pol_key}"] = _to_json(
            _pivot_field(rows, "Delta m/z", polarity_map, polarity)
        )
        result[f"df_warnings_{pol_key}"] = _to_json(
            _pivot_field(rows, "Warnings", polarity_map, polarity)
        )
        result[f"df_fails_{pol_key}"] = _to_json(
            _pivot_field(rows, "Fails", polarity_map, polarity)
        )
        # m/z column: use library precursor_mz from IS, not observed (not stored per-sample)
        # Build from IS library directly
        mz_df = _build_mz_from_library(
            rows, polarity_map, polarity,
            pos_is_df if polarity == "Pos" else neg_is_df,
        )
        result[f"df_mz_{pol_key}"] = _to_json(mz_df)

    # Derive IS name lists from RT dataframes
    pos_rt = result.get("df_rt_pos")
    neg_rt = result.get("df_rt_neg")
    pos_internal_standards: list[str] = []
    neg_internal_standards: list[str] = []
    if pos_rt:
        cols = pd.read_json(io.StringIO(pos_rt), orient="records").columns.tolist()
        pos_internal_standards = [c for c in cols if c != "Specimen"]
    if neg_rt:
        cols = pd.read_json(io.StringIO(neg_rt), orient="records").columns.tolist()
        neg_internal_standards = [c for c in cols if c != "Specimen"]

    # Bio standard DataFrames
    bio = get_bio_standard_dataframes(session, instrument_id, run_id, chromatography)
    bio_names = bio.get("bio_standard_names") or []

    def _bio_dict_to_json(d: dict | None) -> str | None:
        if not d:
            return None
        return json.dumps(d)

    result["bio_rt_pos"] = _bio_dict_to_json(bio.get("bio_rt_pos"))
    result["bio_rt_neg"] = _bio_dict_to_json(bio.get("bio_rt_neg"))
    result["bio_intensity_pos"] = _bio_dict_to_json(bio.get("bio_intensity_pos"))
    result["bio_intensity_neg"] = _bio_dict_to_json(bio.get("bio_intensity_neg"))
    result["bio_mz_pos"] = _bio_dict_to_json(bio.get("bio_mz_pos"))
    result["bio_mz_neg"] = _bio_dict_to_json(bio.get("bio_mz_neg"))

    resources = {
        "instrument": instrument_id,
        "run_id": run_id,
        "chromatography": chromatography,
        "precursor_mass_dict": precursor_mz_dict,
        "retention_times_dict": retention_times_dict,
        "samples_completed": len(rows),
        "biological_standards": bio_names or None,
    }

    result["df_samples"] = pd.DataFrame(sample_records).to_json(orient="records")
    result["resources"] = json.dumps(resources)
    result["pos_internal_standards"] = json.dumps(sorted(pos_internal_standards))
    result["neg_internal_standards"] = json.dumps(sorted(neg_internal_standards))

    return result


def _identify_bio_standards(
    sample_ids: list[str],
    bio_std_names: list[str],
) -> dict[str, str]:
    """Return {sample_id: bio_standard_name} for every sample that matches a bio standard.

    Matching rules (case-insensitive):
      - exact match: sample_id == name
      - prefix match: sample_id starts with name + "_" or name + "-"
    """
    result: dict[str, str] = {}
    for sample_id in sample_ids:
        sid_lower = sample_id.lower()
        for name in bio_std_names:
            name_lower = name.lower()
            if (
                sid_lower == name_lower
                or sid_lower.startswith(name_lower + "_")
                or sid_lower.startswith(name_lower + "-")
            ):
                result[sample_id] = name
                break
    return result


def get_bio_standard_dataframes(
    session,
    instrument_id: str,
    run_id: str,
    chromatography: str = "HILIC",
) -> dict:
    """Build wide DataFrames for biological standard samples in a run.

    Returns a dict with keys:
      bio_standard_names: sorted list of matched bio standard names
      bio_rt_pos / bio_rt_neg: {name: df_records_json} RT DataFrames
      bio_intensity_pos / bio_intensity_neg: Height DataFrames
      bio_mz_pos / bio_mz_neg: library precursor_mz DataFrames

    Each DataFrame has columns: Name (sample_id), run_id, <IS1>, <IS2>, ...
    """
    _empty: dict = {"bio_standard_names": []}

    bio_stds = list_bio_standards(session, chromatography)
    if not bio_stds:
        return _empty

    bio_std_names = [b.name for b in bio_stds]
    rows = get_results_for_run(session, instrument_id, run_id)
    if not rows:
        return {**_empty, "bio_standard_names": bio_std_names}

    bio_map = _identify_bio_standards([r.sample_id for r in rows], bio_std_names)
    if not bio_map:
        return _empty

    # IS library for polarity detection and m/z lookups
    pos_is_df = get_internal_standards_df(chromatography, "Pos")
    neg_is_df = get_internal_standards_df(chromatography, "Neg")
    pos_names = set(pos_is_df["name"].tolist()) if not pos_is_df.empty else set()
    neg_names = set(neg_is_df["name"].tolist()) if not neg_is_df.empty else set()
    pos_mz_lookup = (
        dict(zip(pos_is_df["name"], pos_is_df["precursor_mz"]))
        if not pos_is_df.empty else {}
    )
    neg_mz_lookup = (
        dict(zip(neg_is_df["name"], neg_is_df["precursor_mz"]))
        if not neg_is_df.empty else {}
    )

    # Accumulate records per bio standard per polarity
    bio_data: dict[str, dict[str, list[dict]]] = {
        n: {"Pos": [], "Neg": []} for n in bio_std_names
    }

    for row in rows:
        std_name = bio_map.get(row.sample_id)
        if std_name is None or not row.details:
            continue

        names_in_details = {e.get("Name") for e in row.details if e.get("Name")}
        polarity = "Pos"
        if names_in_details & neg_names and not (names_in_details & pos_names):
            polarity = "Neg"

        mz_lookup = pos_mz_lookup if polarity == "Pos" else neg_mz_lookup

        rt_rec: dict = {"Name": row.sample_id, "run_id": run_id}
        int_rec: dict = {"Name": row.sample_id, "run_id": run_id}
        mz_rec: dict = {"Name": row.sample_id, "run_id": run_id}

        for entry in row.details:
            name = entry.get("Name")
            if name:
                rt_rec[name] = entry.get("RT (min)", "")
                int_rec[name] = entry.get("Height", "")
                mz_rec[name] = mz_lookup.get(name, "")

        bio_data[std_name][polarity].append({"rt": rt_rec, "int": int_rec, "mz": mz_rec})

    def _to_json(record_dicts: list[dict], key: str) -> str | None:
        recs = [r[key] for r in record_dicts]
        if not recs:
            return None
        return pd.DataFrame(recs).to_json(orient="records")

    bio_rt_pos: dict[str, str | None] = {}
    bio_rt_neg: dict[str, str | None] = {}
    bio_intensity_pos: dict[str, str | None] = {}
    bio_intensity_neg: dict[str, str | None] = {}
    bio_mz_pos: dict[str, str | None] = {}
    bio_mz_neg: dict[str, str | None] = {}
    found_names: list[str] = []

    for name in sorted(bio_std_names):
        pos_recs = bio_data[name]["Pos"]
        neg_recs = bio_data[name]["Neg"]
        if not pos_recs and not neg_recs:
            continue
        found_names.append(name)
        bio_rt_pos[name] = _to_json(pos_recs, "rt")
        bio_rt_neg[name] = _to_json(neg_recs, "rt")
        bio_intensity_pos[name] = _to_json(pos_recs, "int")
        bio_intensity_neg[name] = _to_json(neg_recs, "int")
        bio_mz_pos[name] = _to_json(pos_recs, "mz")
        bio_mz_neg[name] = _to_json(neg_recs, "mz")

    return {
        "bio_standard_names": found_names,
        "bio_rt_pos": bio_rt_pos,
        "bio_rt_neg": bio_rt_neg,
        "bio_intensity_pos": bio_intensity_pos,
        "bio_intensity_neg": bio_intensity_neg,
        "bio_mz_pos": bio_mz_pos,
        "bio_mz_neg": bio_mz_neg,
    }


def _build_mz_from_library(
    rows: list[QCResultModel],
    polarity_map: dict[str, str],
    target_polarity: str,
    is_df: pd.DataFrame,
) -> pd.DataFrame | None:
    """Build a wide m/z DataFrame from the IS library (library values, not observed).

    Shape: rows = samples, columns = ["Specimen"] + IS names.
    Each cell is the library precursor_mz for that IS.
    """
    if is_df.empty:
        return None

    mz_lookup = dict(zip(is_df["name"], is_df["precursor_mz"]))

    records: list[dict] = []
    for row in rows:
        if polarity_map.get(row.sample_id, "Pos") != target_polarity:
            continue
        if not row.details:
            continue
        record: dict = {"Specimen": row.sample_id}
        for entry in row.details:
            name = entry.get("Name")
            if name and name in mz_lookup:
                record[name] = mz_lookup[name]
        records.append(record)

    if not records:
        return None
    return pd.DataFrame(records)
