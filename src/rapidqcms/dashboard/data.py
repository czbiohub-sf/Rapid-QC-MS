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

from rapidqcms.db.features import get_internal_standards
from rapidqcms.db.models import QCResult as QCResultModel
from rapidqcms.db.settings import get_run

log = logging.getLogger(__name__)


def get_results_for_run(
    session: Session,
    instrument_id: str,
    run_id: str,
    qc_module: str = "metabolomics",
) -> list[QCResultModel]:
    """Return all QCResult rows for a run, ordered by acquisition time."""
    return (
        session.query(QCResultModel)
        .filter_by(
            instrument_id=instrument_id,
            run_id=run_id,
            qc_module=qc_module,
        )
        .order_by(QCResultModel.acquired_at)
        .all()
    )


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


def get_sample_table(
    session: Session,
    instrument_id: str,
    run_id: str,
) -> pd.DataFrame:
    """Return [Specimen, QC, Polarity] from QCResult rows for sample table."""
    rows = get_results_for_run(session, instrument_id, run_id)
    records = []
    for row in rows:
        records.append({
            "Specimen": row.sample_id,
            "Position": "",  # legacy field — not stored in new schema
            "QC": row.status,
            "Polarity": "",  # derived below from IS library
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
        # chromatography is not stored on Run directly; default to "HILIC"
        chromatography = "HILIC"

    # Build polarity lookup: IS library has polarity per standard name.
    # We infer sample polarity from which IS library produced results.
    # Strategy: if a sample's details contain standards found in Pos IS lib → Pos, else Neg.
    pos_is_df = get_internal_standards(session, chromatography, "Pos")
    neg_is_df = get_internal_standards(session, chromatography, "Neg")
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

    # Determine polarity for each sample by inspecting which IS names appear
    polarity_map: dict[str, str] = {}
    sample_records: list[dict] = []
    for row in rows:
        polarity = "Pos"
        if row.details:
            names_in_details = {e.get("Name") for e in row.details if e.get("Name")}
            if names_in_details & neg_names and not (names_in_details & pos_names):
                polarity = "Neg"
        polarity_map[row.sample_id] = polarity
        sample_records.append({
            "Specimen": row.sample_id,
            "Position": "",
            "QC": row.status,
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

    resources = {
        "instrument": instrument_id,
        "run_id": run_id,
        "status": "Complete",
        "chromatography": chromatography,
        "precursor_mass_dict": precursor_mz_dict,
        "retention_times_dict": retention_times_dict,
        "samples_completed": len(rows),
        "biological_standards": None,
    }

    result["df_samples"] = pd.DataFrame(sample_records).to_json(orient="records")
    result["resources"] = json.dumps(resources)
    result["pos_internal_standards"] = json.dumps(sorted(pos_internal_standards))
    result["neg_internal_standards"] = json.dumps(sorted(neg_internal_standards))

    return result


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
