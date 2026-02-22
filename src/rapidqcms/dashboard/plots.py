"""Dashboard plot functions.

Derived from PlotGeneration.py with the following changes:
  - Removed import of DatabaseFunctions
  - Removed the load_from == "csv" / Google Drive branch
  - get_qc_results() now calls data.get_run_dataframes(session, ...)
  - Pure plot functions (load_istd_rt_plot etc.) are unchanged
"""

import json
import logging
import traceback

import numpy as np
import pandas as pd
import plotly.express as px
from sqlalchemy.orm import Session

from rapidqcms.dashboard.data import get_run_dataframes, get_results_for_run
from rapidqcms.db.models import QCResult as QCResultModel

log = logging.getLogger(__name__)

# Bootstrap color dictionary (unchanged from PlotGeneration.py)
bootstrap_colors = {
    "blue": "rgb(0, 123, 255)",
    "red": "rgb(220, 53, 69)",
    "green": "rgb(40, 167, 69)",
    "yellow": "rgb(255, 193, 7)",
    "blue-low-opacity": "rgba(0, 123, 255, 0.4)",
    "red-low-opacity": "rgba(220, 53, 69, 0.4)",
    "green-low-opacity": "rgba(40, 167, 69, 0.4)",
    "yellow-low-opacity": "rgba(255, 193, 7, 0.4)",
}


def get_qc_results(
    session: Session,
    instrument_id: str,
    run_id: str,
    chromatography: str | None = None,
) -> tuple:
    """Load QC results from DB and return the same tuple as the old PlotGeneration version.

    Returns a 28-element tuple matching the original get_qc_results() signature:
        (df_rt_pos, df_rt_neg, df_intensity_pos, df_intensity_neg,
         df_mz_pos, df_mz_neg, df_sequence, df_metadata,
         df_bio_rt_pos, df_bio_rt_neg, df_bio_intensity_pos, df_bio_intensity_neg,
         df_bio_mz_pos, df_bio_mz_neg,
         resources, df_samples, pos_internal_standards, neg_internal_standards,
         df_delta_rt_pos, df_delta_rt_neg,
         df_in_run_delta_rt_pos, df_in_run_delta_rt_neg,
         df_delta_mz_pos, df_delta_mz_neg,
         df_warnings_pos, df_warnings_neg,
         df_fails_pos, df_fails_neg)
    """
    data = get_run_dataframes(session, instrument_id, run_id, chromatography)

    # Biological standard DataFrames (populated by get_run_dataframes via get_bio_standard_dataframes)
    df_bio_rt_pos = data.get("bio_rt_pos")
    df_bio_rt_neg = data.get("bio_rt_neg")
    df_bio_intensity_pos = data.get("bio_intensity_pos")
    df_bio_intensity_neg = data.get("bio_intensity_neg")
    df_bio_mz_pos = data.get("bio_mz_pos")
    df_bio_mz_neg = data.get("bio_mz_neg")

    # Sequence and metadata are not stored in the new DB schema
    df_sequence = pd.DataFrame()
    df_metadata = pd.DataFrame()

    return (
        data.get("df_rt_pos"),
        data.get("df_rt_neg"),
        data.get("df_intensity_pos"),
        data.get("df_intensity_neg"),
        data.get("df_mz_pos"),
        data.get("df_mz_neg"),
        df_sequence,
        df_metadata,
        df_bio_rt_pos,
        df_bio_rt_neg,
        df_bio_intensity_pos,
        df_bio_intensity_neg,
        df_bio_mz_pos,
        df_bio_mz_neg,
        data.get("resources"),
        data.get("df_samples"),
        data.get("pos_internal_standards"),
        data.get("neg_internal_standards"),
        data.get("df_delta_rt_pos"),
        data.get("df_delta_rt_neg"),
        data.get("df_in_run_delta_rt_pos"),
        data.get("df_in_run_delta_rt_neg"),
        data.get("df_delta_mz_pos"),
        data.get("df_delta_mz_neg"),
        data.get("df_warnings_pos"),
        data.get("df_warnings_neg"),
        data.get("df_fails_pos"),
        data.get("df_fails_neg"),
    )


def generate_sample_metadata_dataframe(
    sample,
    df_rt,
    df_mz,
    df_intensity,
    df_delta_rt,
    df_in_run_delta_rt,
    df_delta_mz,
    df_warnings,
    df_fails,
    df_sequence,
    df_metadata,
):
    """Aggregates tables of relevant data for a selected sample.

    Unchanged from PlotGeneration.py — purely operates on DataFrames.
    """
    df_sample_istd = pd.DataFrame()
    df_sample_info = pd.DataFrame()

    columns = df_rt.columns.tolist()
    internal_standards = df_rt.columns.tolist()
    internal_standards.remove("Specimen")
    df_sample_istd["Internal Standard"] = internal_standards

    df_mz = df_mz.loc[df_mz["Specimen"] == sample][columns]
    df_mz.drop(columns=["Specimen"], inplace=True)
    df_sample_istd["m/z"] = df_mz.iloc[0].astype(float).values.tolist()

    df_rt = df_rt.loc[df_rt["Specimen"] == sample][columns]
    df_rt.drop(columns=["Specimen"], inplace=True)
    df_sample_istd["RT"] = df_rt.iloc[0].astype(float).round(2).values.tolist()

    df_intensity = df_intensity.loc[df_intensity["Specimen"] == sample][columns]
    df_intensity.drop(columns=["Specimen"], inplace=True)
    intensities = df_intensity.iloc[0].fillna(0).values.tolist()
    df_sample_istd["Intensity"] = ["{:.2e}".format(x) for x in intensities]

    df_delta_mz.replace(" ", np.nan, inplace=True)
    df_delta_mz = df_delta_mz.loc[df_delta_mz["Specimen"] == sample][columns]
    df_delta_mz.drop(columns=["Specimen"], inplace=True)
    df_sample_istd["Delta m/z"] = df_delta_mz.iloc[0].astype(float).round(6).values.tolist()

    df_delta_rt.replace(" ", np.nan, inplace=True)
    df_delta_rt = df_delta_rt.loc[df_delta_rt["Specimen"] == sample][columns]
    df_delta_rt.drop(columns=["Specimen"], inplace=True)
    df_sample_istd["Delta RT"] = df_delta_rt.iloc[0].astype(float).round(3).values.tolist()

    df_in_run_delta_rt.replace(" ", np.nan, inplace=True)
    df_in_run_delta_rt = df_in_run_delta_rt.loc[df_in_run_delta_rt["Specimen"] == sample][columns]
    df_in_run_delta_rt.drop(columns=["Specimen"], inplace=True)
    df_sample_istd["In-Run Delta RT"] = (
        df_in_run_delta_rt.iloc[0].astype(float).round(3).values.tolist()
    )

    df_warnings.replace(" ", np.nan, inplace=True)
    df_warnings = df_warnings.loc[df_warnings["Specimen"] == sample][columns]
    df_warnings.drop(columns=["Specimen"], inplace=True)
    df_sample_istd["Warnings"] = df_warnings.iloc[0].astype(str).values.tolist()

    df_fails.replace(" ", np.nan, inplace=True)
    df_fails = df_fails.loc[df_fails["Specimen"] == sample][columns]
    df_fails.drop(columns=["Specimen"], inplace=True)
    df_sample_istd["Fails"] = df_fails.iloc[0].astype(str).values.tolist()

    if len(df_sequence) > 0:
        df_sequence = df_sequence.loc[df_sequence["File Name"].astype(str) == sample]
        df_sample_info["Sample ID"] = df_sequence["Sample ID"].astype(str).values
        df_sample_info["Position"] = df_sequence["Position"].astype(str).values
        df_sample_info["Injection Volume"] = (
            df_sequence["Inj Vol"].astype(str).values + " uL"
        )
        df_sample_info["Instrument Method"] = df_sequence["Instrument Method"].astype(str).values

    if len(df_metadata) > 0:
        df_metadata = df_metadata.loc[df_metadata["Filename"].astype(str) == sample]
        if len(df_metadata) > 0:
            df_sample_info["Species"] = df_metadata["Species"].astype(str).values
            df_sample_info["Matrix"] = df_metadata["Matrix"].astype(str).values
            df_sample_info["Growth-Harvest Conditions"] = (
                df_metadata["Growth-Harvest Conditions"].astype(str).values
            )
            df_sample_info["Treatment"] = df_metadata["Treatment"].astype(str).values

    return df_sample_istd, df_sample_info


def generate_bio_standard_dataframe(
    clicked_sample, instrument_id, run_id, df_rt, df_mz, df_intensity, session: Session
):
    """Aggregates data for a selected biological standard.

    Bio standard grading is deferred to Phase 5 — QC result is returned as N/A.
    """
    log.debug("generate_bio_standard_dataframe: %s", clicked_sample)

    metabolites = df_mz[df_mz["Name"] == clicked_sample].columns.tolist()
    metabolites.remove("Name")
    metabolites.remove("run_id")

    index = df_mz[df_mz["Name"] == clicked_sample].index[0]

    df_sample_features = pd.DataFrame()
    df_sample_features["Metabolite name"] = metabolites
    df_sample_features["Precursor m/z"] = (
        df_mz[metabolites].iloc[index].astype(float).values
    )
    df_sample_features["Retention time (min)"] = (
        df_rt[metabolites].iloc[index].astype(float).round(3).values
    )
    intensities = df_intensity[metabolites].iloc[index].fillna(0).astype(float).values.tolist()
    df_sample_features["Intensity"] = ["{:.2e}".format(x) for x in intensities]

    df_sample_info = pd.DataFrame()
    df_sample_info["Specimen ID"] = [clicked_sample]
    qc_result = (
        session.query(QCResultModel)
        .filter_by(instrument_id=instrument_id, run_id=run_id, sample_id=clicked_sample)
        .first()
    )
    df_sample_info["QC Result"] = [qc_result.status if qc_result else "N/A"]

    return df_sample_features, df_sample_info


# ---------------------------------------------------------------------------
# Pure plot functions — unchanged from PlotGeneration.py
# ---------------------------------------------------------------------------


def load_istd_rt_plot(dataframe, samples, internal_standard, retention_times):
    """Line plot of retention times for a selected internal standard across samples."""
    df_filtered_by_samples = dataframe.loc[dataframe["Specimen"].isin(samples)]
    df_filtered_by_samples[internal_standard] = (
        df_filtered_by_samples[internal_standard].astype(float).round(3)
    )

    y_min = retention_times[internal_standard] - 0.1
    y_max = retention_times[internal_standard] + 0.1

    fig = px.line(
        df_filtered_by_samples,
        title="Retention Time vs. Specimens – " + internal_standard,
        x=samples,
        y=internal_standard,
        height=600,
        markers=True,
        hover_name=samples,
        labels={
            "variable": "Internal Standard",
            "index": "Specimen",
            "value": "Retention Time",
        },
        log_x=False,
    )
    fig.update_layout(
        transition_duration=500,
        clickmode="event",
        showlegend=False,
        legend_title_text="Internal Standards",
        margin=dict(t=75, b=75, l=0, r=0),
    )
    fig.update_xaxes(showticklabels=False, title="Specimen")
    fig.update_yaxes(title="Retention Time (min)", range=[y_min, y_max])
    fig.add_hline(y=retention_times[internal_standard], line_width=2, line_dash="dash")
    fig.update_traces(hovertemplate="Sample: %{x} <br>Retention Time: %{y} min<br>")

    return fig


def load_istd_intensity_plot(dataframe, samples, internal_standard, treatments):
    """Bar plot of peak intensities for a selected internal standard across samples."""
    df_filtered_by_samples = dataframe.loc[dataframe["Specimen"].isin(samples)]

    if len(treatments) > 0:
        df_mapped = pd.DataFrame()
        df_mapped["Specimen"] = df_filtered_by_samples["Specimen"]
        df_mapped["Treatment"] = df_mapped.replace(
            treatments.set_index("Filename")["Treatment"]
        )
        df_filtered_by_samples["Treatment"] = df_mapped["Treatment"].astype(str)
    else:
        df_filtered_by_samples["Treatment"] = " "

    fig = px.bar(
        df_filtered_by_samples,
        title="Intensity vs. Specimens – " + internal_standard,
        x="Specimen",
        y=internal_standard,
        text="Specimen",
        color="Treatment",
        height=600,
    )
    fig.update_layout(
        showlegend=False,
        transition_duration=500,
        clickmode="event",
        xaxis=dict(rangeslider=dict(visible=True), autorange=True),
        legend=dict(font=dict(size=10)),
        margin=dict(t=75, b=75, l=0, r=0),
    )
    fig.update_xaxes(showticklabels=False, title="Specimen")
    fig.update_yaxes(title="Intensity")
    fig.update_traces(
        textposition="outside",
        hovertemplate="Sample: %{x}<br>Intensity: %{y:.2e}<br>",
    )

    return fig


def load_istd_delta_mz_plot(dataframe, samples, internal_standard):
    """Line plot of delta m/z for a selected internal standard across samples."""
    df_filtered_by_samples = dataframe.loc[dataframe["Specimen"].isin(samples)]

    fig = px.line(
        df_filtered_by_samples,
        title="Delta m/z vs. Specimens – " + internal_standard,
        x=samples,
        y=internal_standard,
        height=600,
        markers=True,
        hover_name=samples,
        labels={
            "variable": "Internal Standard",
            "index": "Specimen",
            "value": "Delta m/z",
        },
        log_x=False,
    )
    fig.update_layout(
        transition_duration=500,
        clickmode="event",
        showlegend=False,
        legend_title_text="Internal Standards",
        margin=dict(t=75, b=75, l=0, r=0),
    )
    fig.update_xaxes(showticklabels=False, title="Specimen")
    fig.update_yaxes(title="delta m/z", range=[-0.01, 0.01])
    fig.update_traces(hovertemplate="Sample: %{x} <br>Delta m/z: %{y}<br>")

    return fig


def load_bio_feature_plot(
    run_id, df_rt, df_mz, df_intensity, target_biostnd, source_biostnd, return_runids=False
):
    """Scatter plot of precursor m/z vs. retention time for biological standard features."""
    log.debug("load_bio_feature_plot locals()")

    metabolites = df_mz.columns.tolist()
    sample_names = df_mz["Name"].astype(str).tolist()
    del metabolites[0:2]

    bio_df = pd.DataFrame()
    bio_df["Metabolite name"] = metabolites
    bio_df["Precursor m/z"] = (
        df_mz.loc[df_mz["run_id"] == run_id][metabolites].iloc[0].astype(float).values
    )
    bio_df["Retention time (min)"] = (
        df_rt.loc[df_rt["run_id"] == run_id][metabolites].iloc[0].astype(float).values
    )
    bio_df["Intensity"] = (
        df_intensity.loc[df_intensity["run_id"] == run_id][metabolites].iloc[0].astype(float).values
    )

    if target_biostnd == "All previous":
        df_intensity = df_intensity.fillna(0)
        try:
            index_of_run = df_intensity.loc[df_intensity["run_id"] == run_id].index.tolist()[0]
            df_intensity = df_intensity[0 : index_of_run + 1]
        finally:
            feature_intensity_from_study = (
                df_intensity.loc[df_intensity["run_id"] == run_id][metabolites].iloc[0].astype(float).values
            )

        if len(df_intensity) > 1:
            average_intensity_in_studies = (
                df_intensity.loc[df_intensity["run_id"] != run_id][metabolites].astype(float).mean().values
            )
            bio_df["% Change"] = (
                (feature_intensity_from_study - average_intensity_in_studies)
                / average_intensity_in_studies
            ) * 100
            bio_df.replace(np.inf, 100, inplace=True)
            bio_df.replace(-np.inf, -100, inplace=True)
        else:
            bio_df["% Change"] = 0

    elif source_biostnd is not None and target_biostnd is not None:
        bio_df["Precursor m/z"] = (
            df_mz.loc[df_mz["Name"] == source_biostnd][metabolites].iloc[0].astype(float).values
        )
        bio_df["Retention time (min)"] = (
            df_rt.loc[df_rt["Name"] == source_biostnd][metabolites].iloc[0].astype(float).values
        )
        bio_df["Intensity"] = (
            df_intensity.loc[df_intensity["Name"] == source_biostnd][metabolites].iloc[0].astype(float).values
        )
        df_intensity = df_intensity.fillna(0)
        feature_intensity_from_study = (
            df_intensity.loc[df_intensity["Name"] == source_biostnd][metabolites].iloc[0].astype(float).values
        )
        if len(df_intensity) > 1:
            target_intensity = (
                df_intensity.loc[df_intensity["Name"] == target_biostnd][metabolites].iloc[0].astype(float).values
            )
            bio_df["% Change"] = (
                (feature_intensity_from_study - target_intensity) / target_intensity
            ) * 100
            bio_df.replace(np.inf, 100, inplace=True)
            bio_df.replace(-np.inf, -100, inplace=True)
        else:
            bio_df["% Change"] = 0
    else:
        bio_df["% Change"] = 0

    bio_df["Retention time (min)"] = bio_df["Retention time (min)"].round(2)
    bio_df["% Change"] = bio_df["% Change"].round(1).fillna(0)

    labels = {
        "Retention time (min)": "Retention time (min)",
        "Precursor m/z": "Precursor m/z",
        "Intensity": "Intensity",
        "Metabolite name": "Metabolite name",
    }
    diverging_colorscale = ["#1a88ff", "#3395ff", "#4da3ff", "#a186ca", "#e7727d", "#e35d6a", "#e04958"]
    diverging_colorscale.reverse()

    fig = px.scatter(
        bio_df,
        title="Biological Standard - Selected Compounds",
        x="Retention time (min)",
        y="Precursor m/z",
        height=600,
        hover_name="Metabolite name",
        color="% Change",
        color_continuous_scale=diverging_colorscale,
        labels=labels,
        log_x=False,
        range_color=[-100, 100],
    )
    fig.update_layout(
        showlegend=False,
        transition_duration=1,
        clickmode="event",
        margin=dict(t=75, b=75, l=0, r=0),
    )
    fig.update_xaxes(title="Retention time (min)")
    fig.update_yaxes(title="Precursor m/z")
    fig.update_traces(marker={"size": 30})

    if return_runids is False:
        return fig
    else:
        return sample_names


def load_bio_benchmark_plot(dataframe, metabolite_name, return_runids=False):
    """Bar plot of intensities for a targeted metabolite in a biological standard across runs."""
    log.debug("load_bio_benchmark_plot locals()")

    instrument_runs = dataframe["Name"].astype(str).tolist()
    intensities = dataframe[metabolite_name].values.tolist()
    if len(intensities) == 0:
        intensities = [0 for _ in instrument_runs]

    intensities_text = ["{:.2e}".format(x) for x in intensities] if intensities else []

    fig = px.bar(x=instrument_runs, y=intensities, text=intensities_text, height=600)
    fig.update_layout(
        title="Biological Standard Benchmark",
        showlegend=False,
        transition_duration=500,
        clickmode="event",
        xaxis=dict(rangeslider=dict(visible=True), autorange=True),
        legend=dict(font=dict(size=10)),
        margin=dict(t=75, b=75, l=0, r=0),
    )
    fig.update_xaxes(title="Study")
    fig.update_yaxes(title="Intensity")
    fig.update_traces(
        textposition="outside",
        hovertemplate=f"{metabolite_name}" + "<br>Study: %{x} <br>Intensity: %{text}<br>",
    )

    if return_runids is False:
        return fig
    else:
        return instrument_runs


def get_internal_standard_index(previous, next, max):
    """Button functionality for seeking through internal standards.

    Unchanged from PlotGeneration.py.
    """
    if previous is not None:
        if next is None or next == 0:
            return max - 1

    if previous is None:
        if next is None:
            index = 0
        else:
            index = next
    elif previous is not None:
        index = next - previous

    if index < 0 or index >= max:
        index = 0

    return index
