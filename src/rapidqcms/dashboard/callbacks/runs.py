"""Run table, sample table, and job control callbacks."""

import io
import json
import logging
import time

import pandas as pd
from dash import Input, Output, State, ctx, dcc, html
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

from rapidqcms.config.library import get_registered_chromatographies
from rapidqcms.db.connection import get_session
from rapidqcms.db.models import QCResult as QCResultModel
from rapidqcms.db.settings import (
    delete_run,
    list_instruments,
    list_runs_for_instrument,
    list_all_runs,
    get_run,
)
from rapidqcms.dashboard import plots as dashboard_plots

log = logging.getLogger(__name__)


def _get_instruments_list(session):
    return [i.id for i in list_instruments(session)]


def _get_instrument_runs_df(session, instrument_id) -> pd.DataFrame:
    """Build a DataFrame of runs for an instrument with legacy column names."""
    runs = list_runs_for_instrument(session, instrument_id)
    rows = [
        {
            "run_id": r.id,
            "chromatography": r.experiment_type,
        }
        for r in runs
    ]
    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["run_id", "chromatography"]
    )


def _get_completed_count(session, instrument_id, run_id) -> tuple[int, int]:
    """Return (completed_count, total_count) from QCResult rows."""
    results = (
        session.query(QCResultModel)
        .filter_by(instrument_id=instrument_id, run_id=run_id)
        .all()
    )
    completed = sum(
        1 for r in results if r.status in ("Pass", "Warn", "Fail")
    )
    return completed, len(results)


def register(app):

    @app.callback(
        Output("filter-instrument", "options"),
        Input("filter-date-range", "value"),
    )
    def populate_instrument_filter_dropdown(_):
        """Populates the instrument filter dropdown with all instruments."""
        with get_session() as session:
            instruments = list_instruments(session)
            return [{"label": i.id, "value": i.id} for i in instruments]

    @app.callback(
        Output("instrument-run-table", "active_cell"),
        Output("instrument-run-table", "selected_cells"),
        Input("filter-instrument", "value"),
        Input("filter-type", "value"),
        Input("filter-date-range", "value"),
        Input("job-deleted", "data"),
        prevent_initial_call=True,
    )
    def reset_run_selection(*_):
        """Clears run table selection when filters change or a job is deleted."""
        return None, []

    @app.callback(
        Output("instrument-run-table", "data"),
        Output("table-container", "style"),
        Output("plot-container", "style"),
        Input("filter-instrument", "value"),
        Input("filter-type", "value"),
        Input("filter-date-range", "value"),
        Input("job-deleted", "data"),
        State("study-resources", "data"),
    )
    def populate_run_browser(instrument_ids, exp_type, date_range, job_deleted, resources):
        """Populates the run browser table with runs across all instruments."""
        import datetime as _dt

        since = None
        now = _dt.datetime.now(_dt.timezone.utc)
        if date_range == "2w":
            since = now - _dt.timedelta(weeks=2)
        elif date_range == "1m":
            since = now - _dt.timedelta(days=30)
        elif date_range == "3m":
            since = now - _dt.timedelta(days=90)

        registered = sorted(get_registered_chromatographies())
        with get_session() as session:
            runs = list_all_runs(
                session,
                instrument_ids=instrument_ids or None,
                experiment_type=exp_type or None,
                chromatographies=registered,
                since=since,
            )
            rows = [{
                "Run ID":     r.id,
                "Instrument": r.instrument_id,
                "Date":       r.started_at.strftime("%Y-%m-%d") if r.started_at else "",
            } for r in runs]

        if not rows:
            empty = [{"Run ID": "N/A", "Instrument": "N/A", "Date": "N/A"}]
            return empty, {"display": "block"}, {"display": "none"}

        return rows, {"display": "block"}, {"display": "block"}

    @app.callback(
        Output("selected-instrument", "data"),
        Input("instrument-run-table", "active_cell"),
        State("instrument-run-table", "data"),
        prevent_initial_call=True,
    )
    def store_selected_instrument(active_cell, table_data):
        """Stores the instrument_id of the currently selected run."""
        if not active_cell or not table_data:
            raise PreventUpdate
        return table_data[active_cell["row"]]["Instrument"]

    @app.callback(
        Output("loading-modal", "is_open"),
        Output("loading-modal-title", "children"),
        Output("loading-modal-body", "children"),
        Input("instrument-run-table", "active_cell"),
        State("instrument-run-table", "data"),
        prevent_initial_call=True,
    )
    def open_loading_modal(active_cell, table_data):
        """Opens loading modal when user selects a run."""
        if not active_cell:
            raise PreventUpdate
        run_id = table_data[active_cell["row"]]["Run ID"]
        title = html.Div([
            html.Div(children=[
                dbc.Spinner(color="primary"),
                " Loading QC results for " + run_id,
            ])
        ])
        return True, title, "This may take a few seconds..."

    @app.callback(
        Output("loading-modal", "is_open", allow_duplicate=True),
        Input("load-finished", "data"),
        prevent_initial_call=True,
    )
    def close_loading_modal(load_finished):
        """Closes loading modal once load_data has finished."""
        if load_finished is None:
            raise PreventUpdate
        return False

    @app.callback(
        Output("istd-rt-pos", "data"),
        Output("istd-rt-neg", "data"),
        Output("istd-intensity-pos", "data"),
        Output("istd-intensity-neg", "data"),
        Output("istd-mz-pos", "data"),
        Output("istd-mz-neg", "data"),
        Output("sequence", "data"),
        Output("metadata", "data"),
        Output("bio-rt-pos", "data"),
        Output("bio-rt-neg", "data"),
        Output("bio-intensity-pos", "data"),
        Output("bio-intensity-neg", "data"),
        Output("bio-mz-pos", "data"),
        Output("bio-mz-neg", "data"),
        Output("study-resources", "data"),
        Output("specimens", "data"),
        Output("pos-internal-standards", "data"),
        Output("neg-internal-standards", "data"),
        Output("istd-delta-rt-pos", "data"),
        Output("istd-delta-rt-neg", "data"),
        Output("istd-in-run-delta-rt-pos", "data"),
        Output("istd-in-run-delta-rt-neg", "data"),
        Output("istd-delta-mz-pos", "data"),
        Output("istd-delta-mz-neg", "data"),
        Output("qc-warnings-pos", "data"),
        Output("qc-warnings-neg", "data"),
        Output("qc-fails-pos", "data"),
        Output("qc-fails-neg", "data"),
        Output("load-finished", "data"),
        Input("instrument-run-table", "active_cell"),
        Input("live-update-interval", "n_intervals"),
        State("instrument-run-table", "data"),
        State("study-resources", "data"),
        prevent_initial_call=True,
        suppress_callback_exceptions=True,
    )
    def load_data(active_cell, n_intervals, table_data, resources):
        """Updates and stores QC results in dcc.Store objects.

        Triggered by a user clicking a run in the table, or by the 60-second
        polling interval when a run is already selected.
        """
        _none29 = (None,) * 29

        trigger = ctx.triggered_id
        if trigger == "live-update-interval":
            # Interval tick — refresh the currently selected run if there is one
            if not resources:
                raise PreventUpdate
            r = json.loads(resources)
            run_id = r.get("run_id")
            instrument_id = r.get("instrument")
            if not run_id or not instrument_id:
                raise PreventUpdate
        else:
            # User clicked a row in the run browser table
            if not active_cell:
                return _none29
            run_id = table_data[active_cell["row"]]["Run ID"]
            instrument_id = table_data[active_cell["row"]]["Instrument"]

        try:
            with get_session() as session:
                result = dashboard_plots.get_qc_results(session, instrument_id, run_id)

            (
                rt_pos, rt_neg, int_pos, int_neg, mz_pos, mz_neg,
                sequence, metadata,
                bio_rt_pos, bio_rt_neg, bio_int_pos, bio_int_neg, bio_mz_pos, bio_mz_neg,
                study_resources, specimens, pos_is, neg_is,
                delta_rt_pos, delta_rt_neg,
                in_run_delta_rt_pos, in_run_delta_rt_neg,
                delta_mz_pos, delta_mz_neg,
                warn_pos, warn_neg, fail_pos, fail_neg,
            ) = result

            # Serialise DataFrame results that aren't already JSON strings
            def _df_to_json(x):
                if x is None:
                    return None
                if isinstance(x, pd.DataFrame):
                    return x.to_json(orient="split")
                return x  # already a JSON string

            return (
                rt_pos, rt_neg, int_pos, int_neg, mz_pos, mz_neg,
                _df_to_json(sequence), _df_to_json(metadata),
                bio_rt_pos, bio_rt_neg, bio_int_pos, bio_int_neg, bio_mz_pos, bio_mz_neg,
                study_resources, specimens, pos_is, neg_is,
                delta_rt_pos, delta_rt_neg,
                in_run_delta_rt_pos, in_run_delta_rt_neg,
                delta_mz_pos, delta_mz_neg,
                warn_pos, warn_neg, fail_pos, fail_neg,
                time.time(),
            )
        except Exception:
            log.exception("load_data failed")
            return _none29

    _POOL_RE  = r"^(QC|Pool)[_\-]"
    _BLANK_RE = r"^(BK|Blank)[_\-]"

    @app.callback(
        Output("sample-table", "data"),
        Input("specimens", "data"),
        Input("polarity-options", "value"),
        Input("sample-filtering-options", "value"),
        prevent_initial_call=True,
    )
    def populate_sample_tables(samples, polarity, sample_filter):
        """Populates table with list of samples for selected run.

        Filters rows by the active polarity button and sample-type filter so that
        clicking those controls updates the table even when there are no ISTD plots.
        """
        if samples is None:
            raise PreventUpdate

        df = pd.DataFrame(json.loads(samples))

        # Filter by polarity ("All" or None → no filter)
        if polarity and polarity != "All" and "Polarity" in df.columns:
            df = df.loc[df["Polarity"] == polarity]

        # Filter by sample type
        if sample_filter == "pools":
            df = df.loc[df["Specimen"].str.contains(_POOL_RE, na=False, regex=True)]
        elif sample_filter == "blanks":
            df = df.loc[df["Specimen"].str.contains(_BLANK_RE, na=False, regex=True)]
        elif sample_filter == "specimens":
            is_pool  = df["Specimen"].str.contains(_POOL_RE,  na=False, regex=True)
            is_blank = df["Specimen"].str.contains(_BLANK_RE, na=False, regex=True)
            df = df.loc[~(is_pool | is_blank)]

        cols = [c for c in ["Specimen", "Status", "Notes"] if c in df.columns]
        return df[cols].to_dict("records")

    @app.callback(
        Output("istd-rt-dropdown", "options"),
        Output("istd-mz-dropdown", "options"),
        Output("istd-intensity-dropdown", "options"),
        Output("bio-standard-benchmark-dropdown", "options"),
        Output("rt-plot-sample-dropdown", "options"),
        Output("mz-plot-sample-dropdown", "options"),
        Output("intensity-plot-sample-dropdown", "options"),
        Input("polarity-options", "value"),
        State("sample-table", "data"),
        Input("specimens", "data"),
        State("bio-intensity-pos", "data"),
        State("bio-intensity-neg", "data"),
        State("pos-internal-standards", "data"),
        State("neg-internal-standards", "data"),
        Input("bio-standards-plot-dropdown", "value"),
        Input("bio-standards-plot-dropdown-compare-target", "value"),
        Input("bio-standards-plot-dropdown-compare-source", "value"),
    )
    def update_dropdowns_on_polarity_change(
        polarity, table_data, samples, bio_intensity_pos, bio_intensity_neg,
        pos_internal_standards, neg_internal_standards,
        selected_bio_standard, biostandard_sample_ids, placeholder,
    ):
        """Updates dropdown lists with correct items for user-selected polarity."""
        if samples is None:
            return [], [], [], [], [], [], []

        df_samples = pd.DataFrame(json.loads(samples))

        if polarity == "Neg":
            istd_dropdown = json.loads(neg_internal_standards) if neg_internal_standards else []
            bio_dropdown = []
            if bio_intensity_neg is not None and selected_bio_standard:
                try:
                    df = pd.DataFrame(json.loads(bio_intensity_neg[selected_bio_standard]))
                    df.drop(columns=["Name", "run_id"], inplace=True, errors="ignore")
                    bio_dropdown = df.columns.tolist()
                except Exception:
                    pass
            sample_dropdown = df_samples.loc[
                df_samples["Specimen"].str.contains("Neg", na=False)
            ]["Specimen"].tolist()
        elif polarity == "Pos":
            istd_dropdown = json.loads(pos_internal_standards) if pos_internal_standards else []
            bio_dropdown = []
            if bio_intensity_pos is not None and selected_bio_standard:
                try:
                    df = pd.DataFrame(json.loads(bio_intensity_pos[selected_bio_standard]))
                    df.drop(columns=["Name", "run_id"], inplace=True, errors="ignore")
                    bio_dropdown = df.columns.tolist()
                except Exception:
                    pass
            sample_dropdown = df_samples.loc[
                df_samples["Specimen"].str.contains("Pos", na=False)
            ]["Specimen"].tolist()
        else:
            # "All polarities" — combine both IS lists, show all samples
            pos_is = json.loads(pos_internal_standards) if pos_internal_standards else []
            neg_is = json.loads(neg_internal_standards) if neg_internal_standards else []
            istd_dropdown = sorted(set(pos_is + neg_is))
            bio_dropdown = []
            for bio_store in [bio_intensity_pos, bio_intensity_neg]:
                if bio_store and selected_bio_standard:
                    try:
                        df = pd.DataFrame(json.loads(bio_store[selected_bio_standard]))
                        df.drop(columns=["Name", "run_id"], inplace=True, errors="ignore")
                        bio_dropdown = df.columns.tolist()
                        if bio_dropdown:
                            break
                    except Exception:
                        pass
            sample_dropdown = df_samples["Specimen"].tolist()

        return (
            istd_dropdown, istd_dropdown, istd_dropdown,
            bio_dropdown,
            sample_dropdown, sample_dropdown, sample_dropdown,
        )

    @app.callback(
        Output("rt-plot-sample-dropdown", "value"),
        Output("mz-plot-sample-dropdown", "value"),
        Output("intensity-plot-sample-dropdown", "value"),
        Input("sample-filtering-options", "value"),
        Input("polarity-options", "value"),
        Input("specimens", "data"),
        State("metadata", "data"),
        prevent_initial_call=True,
    )
    def apply_sample_filter_to_plots(filter, polarity, samples, metadata):
        """Apply sample filter to internal standard plots."""
        if samples is None:
            raise PreventUpdate

        df_samples = pd.DataFrame(json.loads(samples))
        if polarity and polarity != "All":
            df_samples = df_samples.loc[df_samples["Polarity"] == polarity]
        sample_list = df_samples["Specimen"].tolist()

        if filter == "all" or filter is None:
            return [], [], []

        if filter == "specimens" and metadata is not None:
            try:
                df_metadata = pd.read_json(metadata, orient="split")
                df_metadata = df_metadata.loc[df_metadata["Filename"].isin(sample_list)]
                samples_only = df_metadata["Filename"].tolist()
                return samples_only, samples_only, samples_only
            except Exception:
                return [], [], []

        import re as _re
        if filter == "pools":
            pools = [s for s in sample_list if _re.match(_POOL_RE, s)]
            return pools, pools, pools

        if filter == "blanks":
            blanks = [s for s in sample_list if _re.match(_BLANK_RE, s)]
            return blanks, blanks, blanks

        return [], [], []

    @app.callback(
        Output("job-controller-modal", "is_open"),
        Output("job-controller-modal-title", "children"),
        Output("job-controller-modal-body", "children"),
        Output("job-controller-confirm-button", "children"),
        Output("job-controller-confirm-button", "color"),
        Input("delete-job-button", "n_clicks"),
        Input("job-deleted", "data"),
        State("study-resources", "data"),
        prevent_initial_call=True,
    )
    def confirm_action_on_job(delete_job, job_deleted, resources):
        """Shows confirmation modal before deleting a job."""
        trigger = ctx.triggered_id
        if resources is None:
            raise PreventUpdate

        resources = json.loads(resources)
        instrument_id = resources["instrument"]
        run_id = resources["run_id"]

        if trigger == "delete-job-button":
            title = "Delete " + run_id + " on " + instrument_id + "?"
            body = dbc.Label(
                "This will delete all QC results for " + run_id + " on " + instrument_id +
                ". This process cannot be undone. Continue?"
            )
            return True, title, body, "Delete Job", "danger"

        elif trigger == "job-deleted":
            return False, None, None, None, None

        raise PreventUpdate

    @app.callback(
        Output("job-deleted", "data"),
        Input("job-controller-confirm-button", "n_clicks"),
        State("study-resources", "data"),
        prevent_initial_call=True,
    )
    def perform_action_on_job(confirm_button, resources):
        """Deletes the selected job."""
        if resources is None:
            raise PreventUpdate

        resources = json.loads(resources)
        run_id = resources["run_id"]

        try:
            with get_session() as session:
                delete_run(session, run_id)
                session.commit()
            return True
        except Exception:
            log.exception("Could not delete job")
            raise PreventUpdate

    @app.callback(
        Output("dumped-sample-info-card", "data"),
        Input("dump-sample-modal-to-csv", "n_clicks"),
        State("feature-table-for-csv", "data"),
        State("csv-filename", "data"),
        prevent_initial_call=True,
    )
    def dump_sample_modal_to_csv(button, feature_table, filename):
        """Exports sample feature table to CSV (stored in dcc.Store for download)."""
        if feature_table is None:
            raise PreventUpdate
        df = pd.read_json(feature_table)
        csv_filename = (filename or "sample_features") + ".csv"
        return {"csv": df.to_csv(index=False), "filename": csv_filename}
