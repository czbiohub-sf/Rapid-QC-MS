"""ISTD and biological standard plot callbacks.

DB rewiring:
  db.get_biological_standard_identifiers() → {} (Phase 5 deferred)
  db.get_polarity_for_sample(...)          → inferred from QCResult IS names
  get_qc_results(instrument_id, run_id)    → plots.get_qc_results(session, ...)
  Bio-standard plot callbacks              → return empty figures (Phase 5)
"""

import io
import json
import logging
import traceback

import pandas as pd
from dash import Input, Output, State, ctx, dash_table, html
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

from rapidqcms.db.connection import get_session
from rapidqcms.config.library import get_internal_standards_df as get_internal_standards_df_lib
from rapidqcms.db.models import QCResult as QCResultModel
from rapidqcms.dashboard.plots import (
    generate_bio_standard_dataframe,
    generate_sample_metadata_dataframe,
    get_internal_standard_index,
    load_bio_benchmark_plot,
    load_bio_feature_plot,
    load_istd_delta_mz_plot,
    load_istd_intensity_plot,
    load_istd_rt_plot,
)

log = logging.getLogger(__name__)


def _get_polarity_for_sample(session, instrument_id, run_id, sample_id, chromatography="HILIC"):
    """Infer sample polarity from which IS library its details match."""
    result = (
        session.query(QCResultModel)
        .filter_by(instrument_id=instrument_id, run_id=run_id, sample_id=sample_id)
        .filter(QCResultModel.qc_module.in_(["metabolomics", "metabolomics_pre"]))
        .first()
    )
    if result is None or not result.details:
        return "Pos"

    pos_is = get_internal_standards_df_lib(chromatography, "Pos")
    neg_is = get_internal_standards_df_lib(chromatography, "Neg")
    pos_names = set(pos_is["name"].tolist()) if not pos_is.empty else set()
    neg_names = set(neg_is["name"].tolist()) if not neg_is.empty else set()

    names_in_details = {e.get("Name") for e in result.details if e.get("Name")}
    if names_in_details & neg_names and not (names_in_details & pos_names):
        return "Neg"
    return "Pos"


def _build_details_modal_body(session, instrument_id, run_id, sample_id):
    """Build modal body from QCResult.details for the clicked sample.

    Returns (body_component, feature_table_json, csv_filename) or None if no details.
    """
    result = (
        session.query(QCResultModel)
        .filter_by(instrument_id=instrument_id, run_id=run_id, sample_id=sample_id)
        .filter(QCResultModel.qc_module.in_(["metabolomics", "metabolomics_pre"]))
        .first()
    )
    if result is None or not isinstance(result.details, list) or not result.details:
        return None

    rows = []
    for e in result.details:
        delta_rt = e.get("Delta RT")
        delta_mz = e.get("Delta m/z")
        warns = e.get("Warnings", "")
        fails = e.get("Fails", "")
        rows.append({
            "Name":          e.get("Name", ""),
            "Height":        f"{e.get('Height', 0):,.0f}" if e.get("Height") else "—",
            "RT (min)":      f"{e['RT (min)']:.3f}" if e.get("RT (min)") is not None else "—",
            "ΔRT (min)":     f"{delta_rt:+.3f}" if delta_rt is not None else "—",
            "Δm/z (ppm)":    f"{delta_mz:+.1f}" if delta_mz is not None else "—",
            "Warnings":      warns,
            "Fails":         fails,
        })

    df = pd.DataFrame(rows)

    status_badge = {
        "Pass": "success", "Warn": "warning", "Fail": "danger",
    }.get(result.status, "secondary")

    header = dbc.Row(className="mb-2", children=[
        dbc.Col(dbc.Badge(result.status, color=status_badge, className="me-2")),
        dbc.Col(html.Small(f"Run: {run_id}  |  Module: {result.qc_module}", className="text-muted")),
    ])

    tbl = dash_table.DataTable(
        data=df.to_dict("records"),
        columns=[{"name": c, "id": c} for c in df.columns if c != "Fails"],
        style_cell={"textAlign": "left", "fontSize": "13px", "padding": "6px 10px"},
        style_data={"whiteSpace": "normal"},
        style_data_conditional=[
            {"if": {"filter_query": '{Warnings} != ""'}, "backgroundColor": "rgba(255,193,7,0.15)"},
            {"if": {"filter_query": '{Fails} != ""'}, "backgroundColor": "rgba(220,53,69,0.15)"},
        ],
        style_header={"fontWeight": "bold", "backgroundColor": "#f8f9fa"},
        page_action="none",
        style_table={"overflowX": "auto"},
    )

    body = html.Div([header, tbl])
    return body, df.to_json(), sample_id


def _pick_polarity(polarity, pos_data, neg_data):
    """Resolve None ('All polarities') to whichever side has data, preferring Pos."""
    if polarity is not None:
        return polarity
    return "Pos" if pos_data else "Neg"


def register(app):

    @app.callback(
        Output("istd-rt-plot", "figure"),
        Output("rt-prev-button", "n_clicks"),
        Output("rt-next-button", "n_clicks"),
        Output("istd-rt-dropdown", "value"),
        Output("istd-rt-div", "style"),
        Input("polarity-options", "value"),
        Input("istd-rt-dropdown", "value"),
        Input("rt-plot-sample-dropdown", "value"),
        Input("istd-rt-pos", "data"),
        Input("istd-rt-neg", "data"),
        State("specimens", "data"),
        State("study-resources", "data"),
        State("pos-internal-standards", "data"),
        State("neg-internal-standards", "data"),
        Input("rt-prev-button", "n_clicks"),
        Input("rt-next-button", "n_clicks"),
        prevent_initial_call=True,
    )
    def populate_istd_rt_plot(
        polarity, internal_standard, selected_samples,
        rt_pos, rt_neg, samples, resources,
        pos_internal_standards, neg_internal_standards, previous, next,
    ):
        """Populates internal standard retention time vs. sample plot."""
        if resources is not None:
            resources = json.loads(resources)
            retention_times = resources.get("retention_times_dict", {})
        else:
            retention_times = {}

        if rt_pos is None and rt_neg is None:
            return {}, None, None, None, {"display": "none"}

        trigger = ctx.triggered_id

        df_istd_rt_pos = pd.DataFrame(json.loads(rt_pos)) if rt_pos else pd.DataFrame()
        df_istd_rt_neg = pd.DataFrame(json.loads(rt_neg)) if rt_neg else pd.DataFrame()

        polarity = _pick_polarity(polarity, rt_pos, rt_neg)

        df_samples = pd.DataFrame(json.loads(samples))
        all_samples = (
            df_samples.loc[df_samples["Polarity"] == polarity]["Specimen"]
            .astype(str).tolist()
        )

        if polarity == "Pos":
            internal_standards = json.loads(pos_internal_standards)
            df_istd_rt = df_istd_rt_pos
        else:
            internal_standards = json.loads(neg_internal_standards)
            df_istd_rt = df_istd_rt_neg

        if not internal_standards:
            return {}, None, None, None, {"display": "none"}

        if not internal_standard or trigger == "polarity-options":
            internal_standard = internal_standards[0]

        if not selected_samples:
            selected_samples = all_samples

        if trigger in ("rt-prev-button", "rt-next-button"):
            index = get_internal_standard_index(previous, next, len(internal_standards))
            internal_standard = internal_standards[index]
        else:
            index = next

        try:
            return (
                load_istd_rt_plot(
                    dataframe=df_istd_rt,
                    samples=selected_samples,
                    internal_standard=internal_standard,
                    retention_times=retention_times,
                ),
                None,
                index,
                internal_standard,
                {"display": "block"},
            )
        except Exception:
            log.debug("Error in RT plot: %s", traceback.format_exc())
            return {}, None, None, None, {"display": "none"}

    @app.callback(
        Output("istd-intensity-plot", "figure"),
        Output("intensity-prev-button", "n_clicks"),
        Output("intensity-next-button", "n_clicks"),
        Output("istd-intensity-dropdown", "value"),
        Output("istd-intensity-div", "style"),
        Input("polarity-options", "value"),
        Input("istd-intensity-dropdown", "value"),
        Input("intensity-plot-sample-dropdown", "value"),
        Input("istd-intensity-pos", "data"),
        Input("istd-intensity-neg", "data"),
        State("specimens", "data"),
        State("metadata", "data"),
        State("study-resources", "data"),
        State("pos-internal-standards", "data"),
        State("neg-internal-standards", "data"),
        Input("intensity-prev-button", "n_clicks"),
        Input("intensity-next-button", "n_clicks"),
        prevent_initial_call=True,
    )
    def populate_istd_intensity_plot(
        polarity, internal_standard, selected_samples,
        intensity_pos, intensity_neg, samples, metadata, resources,
        pos_internal_standards, neg_internal_standards, previous, next,
    ):
        """Populates internal standard intensity vs. sample plot."""
        if intensity_pos is None and intensity_neg is None:
            return {}, None, None, None, {"display": "none"}

        trigger = ctx.triggered_id

        df_istd_intensity_pos = (
            pd.DataFrame(json.loads(intensity_pos)) if intensity_pos else pd.DataFrame()
        )
        df_istd_intensity_neg = (
            pd.DataFrame(json.loads(intensity_neg)) if intensity_neg else pd.DataFrame()
        )

        polarity = _pick_polarity(polarity, intensity_pos, intensity_neg)

        df_samples = pd.DataFrame(json.loads(samples))
        all_samples = (
            df_samples.loc[df_samples["Polarity"] == polarity]["Specimen"]
            .astype(str).tolist()
        )

        if polarity == "Pos":
            internal_standards = json.loads(pos_internal_standards)
            df_istd_intensity = df_istd_intensity_pos
        else:
            internal_standards = json.loads(neg_internal_standards)
            df_istd_intensity = df_istd_intensity_neg

        if not internal_standards:
            return {}, None, None, None, {"display": "none"}

        if not internal_standard or trigger == "polarity-options":
            internal_standard = internal_standards[0]

        if not selected_samples:
            selected_samples = all_samples
            treatments = pd.DataFrame()
        else:
            treatments = pd.DataFrame()
            if metadata is not None:
                try:
                    df_metadata = pd.read_json(metadata, orient="split")
                    df_metadata = df_metadata.loc[
                        df_metadata["Filename"].isin(selected_samples)
                    ]
                    treatments = df_metadata[["Filename", "Treatment"]]
                except Exception:
                    pass

        if trigger in ("intensity-prev-button", "intensity-next-button"):
            index = get_internal_standard_index(previous, next, len(internal_standards))
            internal_standard = internal_standards[index]
        else:
            index = next

        try:
            return (
                load_istd_intensity_plot(
                    dataframe=df_istd_intensity,
                    samples=selected_samples,
                    internal_standard=internal_standard,
                    treatments=treatments,
                ),
                None,
                index,
                internal_standard,
                {"display": "block"},
            )
        except Exception:
            log.debug("Error in intensity plot: %s", traceback.format_exc())
            return {}, None, None, None, {"display": "none"}

    @app.callback(
        Output("istd-mz-plot", "figure"),
        Output("mz-prev-button", "n_clicks"),
        Output("mz-next-button", "n_clicks"),
        Output("istd-mz-dropdown", "value"),
        Output("istd-mz-div", "style"),
        Input("polarity-options", "value"),
        Input("istd-mz-dropdown", "value"),
        Input("mz-plot-sample-dropdown", "value"),
        Input("istd-delta-mz-pos", "data"),
        Input("istd-delta-mz-neg", "data"),
        State("specimens", "data"),
        State("pos-internal-standards", "data"),
        State("neg-internal-standards", "data"),
        State("study-resources", "data"),
        Input("mz-prev-button", "n_clicks"),
        Input("mz-next-button", "n_clicks"),
        prevent_initial_call=True,
    )
    def populate_istd_mz_plot(
        polarity, internal_standard, selected_samples,
        delta_mz_pos, delta_mz_neg, samples,
        pos_internal_standards, neg_internal_standards, resources, previous, next,
    ):
        """Populates internal standard delta m/z vs. sample plot."""
        if delta_mz_pos is None and delta_mz_neg is None:
            return {}, None, None, None, {"display": "none"}

        trigger = ctx.triggered_id

        df_istd_mz_pos = (
            pd.DataFrame(json.loads(delta_mz_pos)) if delta_mz_pos else pd.DataFrame()
        )
        df_istd_mz_neg = (
            pd.DataFrame(json.loads(delta_mz_neg)) if delta_mz_neg else pd.DataFrame()
        )

        polarity = _pick_polarity(polarity, delta_mz_pos, delta_mz_neg)

        df_samples = pd.DataFrame(json.loads(samples))
        all_samples = (
            df_samples.loc[df_samples["Polarity"] == polarity]["Specimen"]
            .astype(str).tolist()
        )

        if polarity == "Pos":
            internal_standards = json.loads(pos_internal_standards)
            df_istd_mz = df_istd_mz_pos
        else:
            internal_standards = json.loads(neg_internal_standards)
            df_istd_mz = df_istd_mz_neg

        if not internal_standards:
            return {}, None, None, None, {"display": "none"}

        if not internal_standard or trigger == "polarity-options":
            internal_standard = internal_standards[0]

        if not selected_samples:
            selected_samples = all_samples

        if trigger in ("mz-prev-button", "mz-next-button"):
            index = get_internal_standard_index(previous, next, len(internal_standards))
            internal_standard = internal_standards[index]
        else:
            index = next

        try:
            return (
                load_istd_delta_mz_plot(
                    dataframe=df_istd_mz,
                    samples=selected_samples,
                    internal_standard=internal_standard,
                ),
                None,
                index,
                internal_standard,
                {"display": "block"},
            )
        except Exception:
            log.debug("Error in mz plot: %s", traceback.format_exc())
            return {}, None, None, None, {"display": "none"}

    @app.callback(
        Output("bio-standards-plot-dropdown-jobid", "options"),
        Output("bio-standards-plot-dropdown-jobid", "value"),
        Input("instrument-run-table", "data"),
        prevent_initial_call=True,
    )
    def populate_biological_standards_job_dropdown(run_table):
        run_id_list = ["All"]
        if run_table is not None:
            for row in run_table:
                run_id_list.append(row["Run ID"])
            return run_id_list, run_id_list[0]
        return [""], ""

    @app.callback(
        Output("bio-standards-plot-dropdown", "options"),
        Output("bio-standards-plot-dropdown", "value"),
        Input("study-resources", "data"),
        prevent_initial_call=True,
    )
    def populate_biological_standards_dropdown(resources):
        """Retrieves list of biological standards included in run."""
        if resources is not None:
            resources = json.loads(resources)
            biostnds = resources.get("biological_standards") or [""]
            if not biostnds:
                biostnds = [""]
            return biostnds, biostnds[0]
        return [""], ""

    @app.callback(
        Output("bio-standards-plot-dropdown-compare-target", "options"),
        Output("bio-standards-plot-dropdown-compare-target", "value"),
        Output("bio-standards-plot-dropdown-compare-source", "options"),
        Output("bio-standards-plot-dropdown-compare-source", "value"),
        Input("study-resources", "data"),
        Input("polarity-options", "value"),
        Input("bio-intensity-pos", "data"),
        Input("bio-intensity-neg", "data"),
        Input("bio-standards-plot-dropdown", "value"),
        Input("bio-standards-plot-dropdown-jobid", "value"),
        prevent_initial_call=True,
    )
    def populate_biological_standards_compare_dropdowns(
        resources, polarity, intensity_pos, intensity_neg, selected_bio_standard, runselector
    ):
        polarity = _pick_polarity(polarity, intensity_pos, intensity_neg)
        intensity_store = intensity_pos if polarity == "Pos" else intensity_neg
        if not intensity_store or not selected_bio_standard:
            return [""], "", [""], ""
        try:
            bio_dict = json.loads(intensity_store)
            df_json = bio_dict.get(selected_bio_standard)
            if not df_json:
                return [""], "", [""], ""
            df = pd.read_json(io.StringIO(df_json), orient="records")
            names = df["Name"].astype(str).tolist()
            options = ["All previous"] + names
            return options, options[0], options, options[0]
        except Exception:
            log.debug("Error in bio compare dropdowns: %s", traceback.format_exc())
            return [""], "", [""], ""

    @app.callback(
        Output("bio-standard-mz-rt-plot", "figure"),
        Output("bio-standard-benchmark-dropdown", "value"),
        Output("bio-standard-mz-rt-plot", "clickData"),
        Output("bio-standard-mz-rt-div", "style"),
        Input("polarity-options", "value"),
        Input("bio-rt-pos", "data"),
        Input("bio-rt-neg", "data"),
        State("bio-intensity-pos", "data"),
        State("bio-intensity-neg", "data"),
        State("bio-mz-pos", "data"),
        State("bio-mz-neg", "data"),
        State("study-resources", "data"),
        Input("bio-standard-mz-rt-plot", "clickData"),
        Input("bio-standards-plot-dropdown", "value"),
        Input("bio-standards-plot-dropdown-compare-target", "value"),
        Input("bio-standards-plot-dropdown-compare-source", "value"),
        Input("bio-standards-plot-dropdown-jobid", "value"),
        prevent_initial_call=True,
    )
    def populate_bio_standard_mz_rt_plot(
        polarity, rt_pos, rt_neg, intensity_pos, intensity_neg, mz_pos, mz_neg,
        resources, click_data, selected_bio_standard, target_biostnd, source_biostnd, jobid,
    ):
        polarity = _pick_polarity(polarity, rt_pos, rt_neg)
        rt_store = rt_pos if polarity == "Pos" else rt_neg
        intensity_store = intensity_pos if polarity == "Pos" else intensity_neg
        mz_store = mz_pos if polarity == "Pos" else mz_neg

        if not rt_store or not selected_bio_standard:
            return {}, None, None, {"display": "none"}

        try:
            rt_dict = json.loads(rt_store)
            int_dict = json.loads(intensity_store) if intensity_store else {}
            mz_dict = json.loads(mz_store) if mz_store else {}

            rt_json = rt_dict.get(selected_bio_standard)
            if not rt_json:
                return {}, None, None, {"display": "none"}

            df_rt = pd.read_json(io.StringIO(rt_json), orient="records")
            int_json = int_dict.get(selected_bio_standard)
            df_intensity = (
                pd.read_json(io.StringIO(int_json), orient="records")
                if int_json else df_rt.copy()
            )
            mz_json = mz_dict.get(selected_bio_standard)
            df_mz = (
                pd.read_json(io.StringIO(mz_json), orient="records")
                if mz_json else df_rt.copy()
            )

            resources_dict = json.loads(resources) if resources else {}
            run_id = resources_dict.get("run_id", "")

            selected_feature = None
            if click_data:
                try:
                    selected_feature = click_data["points"][0]["hovertext"]
                except (KeyError, IndexError):
                    pass

            fig = load_bio_feature_plot(
                run_id, df_rt, df_mz, df_intensity, target_biostnd, source_biostnd
            )
            return fig, selected_feature, None, {"display": "block"}
        except Exception:
            log.debug("Error in bio mz/rt plot: %s", traceback.format_exc())
            return {}, None, None, {"display": "none"}

    @app.callback(
        Output("bio-standard-benchmark-plot", "figure"),
        Output("bio-standard-benchmark-div", "style"),
        Input("polarity-options", "value"),
        Input("bio-standard-benchmark-dropdown", "value"),
        Input("bio-intensity-pos", "data"),
        Input("bio-intensity-neg", "data"),
        Input("bio-standards-plot-dropdown", "value"),
        State("study-resources", "data"),
        prevent_initial_call=True,
    )
    def populate_bio_standard_benchmark_plot(
        polarity, selected_feature, intensity_pos, intensity_neg, selected_bio_standard, resources
    ):
        polarity = _pick_polarity(polarity, intensity_pos, intensity_neg)
        intensity_store = intensity_pos if polarity == "Pos" else intensity_neg

        if not intensity_store or not selected_bio_standard or not selected_feature:
            return {}, {"display": "none"}

        try:
            bio_dict = json.loads(intensity_store)
            df_json = bio_dict.get(selected_bio_standard)
            if not df_json:
                return {}, {"display": "none"}

            df = pd.read_json(io.StringIO(df_json), orient="records")
            if selected_feature not in df.columns:
                return {}, {"display": "none"}

            fig = load_bio_benchmark_plot(df, selected_feature)
            return fig, {"display": "block"}
        except Exception:
            log.debug("Error in bio benchmark plot: %s", traceback.format_exc())
            return {}, {"display": "none"}

    @app.callback(
        Output("sample-info-modal", "is_open"),
        Output("sample-modal-title", "children"),
        Output("sample-modal-body", "children"),
        Output("sample-table", "selected_cells"),
        Output("sample-table", "active_cell"),
        Output("istd-rt-plot", "clickData"),
        Output("istd-intensity-plot", "clickData"),
        Output("istd-mz-plot", "clickData"),
        Output("feature-table-for-csv", "data"),
        Output("csv-filename", "data"),
        State("sample-info-modal", "is_open"),
        Input("sample-table", "active_cell"),
        State("sample-table", "data"),
        Input("istd-rt-plot", "clickData"),
        Input("istd-intensity-plot", "clickData"),
        Input("istd-mz-plot", "clickData"),
        State("istd-rt-pos", "data"),
        State("istd-rt-neg", "data"),
        State("istd-intensity-pos", "data"),
        State("istd-intensity-neg", "data"),
        State("istd-mz-pos", "data"),
        State("istd-mz-neg", "data"),
        State("istd-delta-rt-pos", "data"),
        State("istd-delta-rt-neg", "data"),
        State("istd-in-run-delta-rt-pos", "data"),
        State("istd-in-run-delta-rt-neg", "data"),
        State("istd-delta-mz-pos", "data"),
        State("istd-delta-mz-neg", "data"),
        State("qc-warnings-pos", "data"),
        State("qc-warnings-neg", "data"),
        State("qc-fails-pos", "data"),
        State("qc-fails-neg", "data"),
        State("bio-rt-pos", "data"),
        State("bio-rt-neg", "data"),
        State("bio-intensity-pos", "data"),
        State("bio-intensity-neg", "data"),
        State("bio-mz-pos", "data"),
        State("bio-mz-neg", "data"),
        State("sequence", "data"),
        State("metadata", "data"),
        State("study-resources", "data"),
        prevent_initial_call=True,
    )
    def toggle_sample_card(
        is_open, active_cell, table_data, rt_click, intensity_click, mz_click,
        rt_pos, rt_neg, intensity_pos, intensity_neg, mz_pos, mz_neg,
        delta_rt_pos, delta_rt_neg, in_run_delta_rt_pos, in_run_delta_rt_neg,
        delta_mz_pos, delta_mz_neg, qc_warnings_pos, qc_warnings_neg,
        qc_fails_pos, qc_fails_neg, bio_rt_pos, bio_rt_neg,
        bio_intensity_pos, bio_intensity_neg, bio_mz_pos, bio_mz_neg,
        sequence, metadata, resources,
    ):
        """Opens information modal when a sample is clicked from the sample table."""
        clicked_sample = None

        if active_cell:
            clicked_sample = table_data[active_cell["row"]][active_cell["column_id"]]
        if rt_click:
            clicked_sample = rt_click["points"][0]["x"].replace(": RT Info", "")
        if intensity_click:
            clicked_sample = intensity_click["points"][0]["x"].replace(": Height", "")
        if mz_click:
            clicked_sample = mz_click["points"][0]["x"].replace(": Precursor m/z Info", "")

        if clicked_sample is None or resources is None:
            raise PreventUpdate

        resources = json.loads(resources)
        instrument_id = resources["instrument"]
        run_id = resources["run_id"]

        # Build modal body from QCResult.details (DB-driven)
        with get_session() as session:
            detail_result = _build_details_modal_body(
                session, instrument_id, run_id, clicked_sample
            )

        if detail_result is not None:
            body, feature_table_json, csv_name = detail_result
            title = clicked_sample
            if is_open:
                return False, title, body, [], None, None, None, None, None, csv_name
            return True, title, body, [], None, None, None, None, feature_table_json, csv_name

        # Fallback: try old pivot-table approach for legacy data
        with get_session() as session:
            polarity = _get_polarity_for_sample(session, instrument_id, run_id, clicked_sample)

        df_sequence = pd.DataFrame()
        df_metadata = pd.DataFrame()
        if sequence:
            try:
                df_sequence = pd.read_json(io.StringIO(sequence), orient="split")
            except Exception:
                pass
        if metadata:
            try:
                df_metadata = pd.read_json(io.StringIO(metadata), orient="split")
            except Exception:
                pass

        try:
            if polarity == "Pos":
                df_rt = pd.DataFrame(json.loads(rt_pos))
                df_intensity = pd.DataFrame(json.loads(intensity_pos))
                df_mz = pd.DataFrame(json.loads(mz_pos))
                df_delta_rt = pd.DataFrame(json.loads(delta_rt_pos))
                df_in_run_delta_rt = pd.DataFrame(json.loads(in_run_delta_rt_pos))
                df_delta_mz = pd.DataFrame(json.loads(delta_mz_pos))
                df_warnings = pd.DataFrame(json.loads(qc_warnings_pos))
                df_fails = pd.DataFrame(json.loads(qc_fails_pos))
            else:
                df_rt = pd.DataFrame(json.loads(rt_neg))
                df_intensity = pd.DataFrame(json.loads(intensity_neg))
                df_mz = pd.DataFrame(json.loads(mz_neg))
                df_delta_rt = pd.DataFrame(json.loads(delta_rt_neg))
                df_in_run_delta_rt = pd.DataFrame(json.loads(in_run_delta_rt_neg))
                df_delta_mz = pd.DataFrame(json.loads(delta_mz_neg))
                df_warnings = pd.DataFrame(json.loads(qc_warnings_neg))
                df_fails = pd.DataFrame(json.loads(qc_fails_neg))

            df_sample_features, df_sample_info = generate_sample_metadata_dataframe(
                clicked_sample, df_rt, df_mz, df_intensity,
                df_delta_rt, df_in_run_delta_rt, df_delta_mz,
                df_warnings, df_fails, df_sequence, df_metadata,
            )
        except Exception:
            log.debug("Error building sample card: %s", traceback.format_exc())
            raise PreventUpdate

        metadata_table = dbc.Table.from_dataframe(
            df_sample_info, striped=True, bordered=True, hover=True
        )
        feature_table = dbc.Table.from_dataframe(
            df_sample_features, striped=True, bordered=True, hover=True
        )
        title = clicked_sample
        body = html.Div(children=[metadata_table, feature_table])

        if is_open:
            return False, title, body, [], None, None, None, None, None, clicked_sample
        return True, title, body, [], None, None, None, None, df_sample_features.to_json(), clicked_sample
