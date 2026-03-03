"""Run table, sample table, job control, and new-run wizard callbacks.

DB rewiring:
  db.is_valid()                          → bool(list_instruments(session))
  db.get_instruments_list()              → [i.id for i in list_instruments(session)]
  db.get_instrument_runs(instrument_id)  → DataFrame from list_runs_for_instrument
  db.get_completed_samples_count(...)    → count QCResult rows
  db.get_run_progress(...)               → (completed / total) * 100
  db.delete_instrument_run(...)          → delete_run(session, run_id)
  db.mark_run_as_completed(...)          → complete_run(session, run_id)
  db.insert_new_run(...)                 → create_run(session, run_id, instrument_id, ...)
  db.get_device_identity()               → None (always — dashboard is server-only)
  db.sync_is_enabled()                   → False (GDrive removed)
  db.pipeline_valid(module)              → check settings env vars
  qc.sequence_is_valid / convert_*       → import from AutoQCProcessing
"""

import base64
import io
import json
import logging
import os
import sys
import time

import pandas as pd
from dash import Input, Output, State, ctx, dcc, html
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

from rapidqcms.config import get_settings
from rapidqcms.config.library import get_registered_chromatographies
from rapidqcms.db.connection import get_session
from rapidqcms.db.models import QCResult as QCResultModel
from rapidqcms.db.settings import (
    complete_run,
    create_run,
    delete_run,
    list_instruments,
    list_runs_for_instrument,
    list_all_runs,
    get_run,
)
from rapidqcms.dashboard import plots as dashboard_plots

try:
    import rapidqcms.AutoQCProcessing as _qc
    _HAS_QC = True
except Exception:
    _HAS_QC = False

log = logging.getLogger(__name__)


def _get_instruments_list(session):
    return [i.id for i in list_instruments(session)]


def _get_instrument_runs_df(session, instrument_id) -> pd.DataFrame:
    """Build a DataFrame of runs for an instrument with legacy column names."""
    runs = list_runs_for_instrument(session, instrument_id)
    rows = [
        {
            "run_id": r.id,
            "chromatography": r.experiment_type,  # experiment_type used as display field
            "status": r.status,
        }
        for r in runs
    ]
    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["run_id", "chromatography", "status"]
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


def _pipeline_valid(module=None):
    """Check whether the configured exe paths exist."""
    settings = get_settings()
    if module == "msconvert":
        return settings.msconvert_exe is not None and settings.msconvert_exe.exists()
    if module == "msdial":
        return settings.msdial_exe is not None and settings.msdial_exe.exists()
    msconvert_ok = settings.msconvert_exe is not None and settings.msconvert_exe.exists()
    msdial_ok = settings.msdial_exe is not None and settings.msdial_exe.exists()
    return msconvert_ok and msdial_ok


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
        State("instrument-run-table", "data"),
        State("study-resources", "data"),
        prevent_initial_call=True,
        suppress_callback_exceptions=True,
    )
    def load_data(active_cell, table_data, resources):
        """Updates and stores QC results in dcc.Store objects."""
        _none29 = (None,) * 29

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

        # Filter by sample type
        if sample_filter == "pools":
            df = df.loc[df["Specimen"].str.contains("QC", na=False)]
        elif sample_filter == "blanks":
            df = df.loc[df["Specimen"].str.contains("BK", na=False)]
        elif sample_filter == "specimens":
            df = df.loc[~df["Specimen"].str.contains("QC|BK", na=False, regex=True)]

        cols = [c for c in ["Specimen", "Status", "QC"] if c in df.columns]
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
        if polarity:
            df_samples = df_samples.loc[df_samples["Polarity"].str.contains(polarity, na=False)]
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

        if filter == "pools":
            pools = [s for s in sample_list if "QC" in s]
            return pools, pools, pools

        if filter == "blanks":
            blanks = [s for s in sample_list if "BK" in s]
            return blanks, blanks, blanks

        return [], [], []

    # -------------------------------------------------------------------------
    # New run modal
    # -------------------------------------------------------------------------

    @app.callback(
        Output("setup-new-run-modal", "is_open"),
        Output("setup-new-run-button", "n_clicks"),
        Output("setup-new-run-modal-title", "children"),
        Input("setup-new-run-button", "n_clicks"),
        Input("start-run-monitor-modal", "is_open"),
        State("selected-instrument", "data"),
        Input("data-acquisition-folder-button", "n_clicks"),
        Input("file-explorer-select-button", "n_clicks"),
        State("settings-modal", "is_open"),
        prevent_initial_call=True,
    )
    def toggle_new_run_modal(
        button_clicks, success, instrument_name,
        browse_folder_button, file_explorer_button, settings_modal_is_open,
    ):
        """Toggles modal for setting up AutoQC monitoring for a new instrument run."""
        button = ctx.triggered_id
        modal_title = "New QC Job – " + (instrument_name or "")

        if button == "data-acquisition-folder-button":
            return False, 0, modal_title
        elif button == "file-explorer-select-button":
            if settings_modal_is_open:
                return False, 0, modal_title
            return True, 1, modal_title

        if not success and button_clicks:
            return True, 1, modal_title
        return False, 0, modal_title

    @app.callback(
        Output("start-run-chromatography-dropdown", "options"),
        Output("start-run-bio-standards-dropdown", "options"),
        Output("start-run-qc-configs-dropdown", "options"),
        Input("setup-new-run-button", "n_clicks"),
        prevent_initial_call=True,
    )
    def populate_options_for_new_run(button_click):
        """Populates dropdowns for Setup New Rapid-QC-MS Job page."""
        from rapidqcms.db.features import list_bio_standards, list_qc_configurations
        from rapidqcms.dashboard.callbacks.settings import _get_chromatography_methods_df

        with get_session() as session:
            methods_df = _get_chromatography_methods_df(session)
            chromatography_methods = [
                {"value": m, "label": m} for m in methods_df["method_id"].tolist()
            ]
            bio_names = sorted(set(b.name for b in list_bio_standards(session)))
            biological_standards = [{"value": n, "label": n} for n in bio_names]
            qc_configs = list_qc_configurations(session)
            qc_configurations = [{"value": c.id, "label": c.id} for c in qc_configs]
            if not qc_configurations:
                qc_configurations = [{"value": "Default", "label": "Default"}]

        return chromatography_methods, biological_standards, qc_configurations

    @app.callback(
        Output("sequence-path", "value"),
        Output("new-sequence", "data"),
        Input("sequence-upload-button", "contents"),
        State("sequence-upload-button", "filename"),
        prevent_initial_call=True,
    )
    def capture_uploaded_sequence(contents, filename):
        """Converts sequence CSV file to JSON string and stores in dcc.Store."""
        content_type, content_string = contents.split(",")
        decoded = base64.b64decode(content_string)
        sequence_file = io.StringIO(decoded.decode("utf-8"))
        if _HAS_QC:
            sequence = _qc.convert_sequence_to_json(sequence_file)
        else:
            sequence = sequence_file.read()
        return filename, sequence

    @app.callback(
        Output("metadata-path", "value"),
        Output("new-metadata", "data"),
        Input("metadata-upload-button", "contents"),
        State("metadata-upload-button", "filename"),
        prevent_initial_call=True,
    )
    def capture_uploaded_metadata(contents, filename):
        """Converts metadata CSV file to JSON string and stores in dcc.Store."""
        content_type, content_string = contents.split(",")
        decoded = base64.b64decode(content_string)
        metadata_file = io.StringIO(decoded.decode("utf-8"))
        if _HAS_QC:
            metadata = _qc.convert_metadata_to_json(metadata_file)
        else:
            metadata = metadata_file.read()
        return filename, metadata

    @app.callback(
        Output("monitor-new-run-button", "children"),
        Output("data-acquisition-path-title", "children"),
        Output("data-acquisition-path-form-text", "children"),
        Input("setup-new-run-modal", "is_open"),
    )
    def update_new_job_button_text(is_open):
        """Updates New Rapid-QC-MS Job form submit button text."""
        button_text = "Start monitoring instrument run"
        text_field_title = "Data acquisition path"
        form_text = "Please enter the folder path to which incoming raw data files will be saved."

        msconvert_valid = _pipeline_valid("msconvert")
        msdial_valid = _pipeline_valid("msdial")

        if not msconvert_valid and not msdial_valid:
            button_text = "Error: MSConvert and MS-DIAL installations not found"
        elif not msdial_valid:
            button_text = "Error: Could not locate MS-DIAL console app"
        elif not msconvert_valid:
            button_text = "Error: Could not locate MSConvert installation"

        return button_text, text_field_title, form_text

    @app.callback(
        Output("instrument-run-id", "valid"),
        Output("instrument-run-id", "invalid"),
        Output("start-run-chromatography-dropdown", "valid"),
        Output("start-run-chromatography-dropdown", "invalid"),
        Output("start-run-qc-configs-dropdown", "valid"),
        Output("start-run-qc-configs-dropdown", "invalid"),
        Output("sequence-path", "valid"),
        Output("sequence-path", "invalid"),
        Output("metadata-path", "valid"),
        Output("metadata-path", "invalid"),
        Output("data-acquisition-folder-path", "valid"),
        Output("data-acquisition-folder-path", "invalid"),
        Input("instrument-run-id", "value"),
        Input("start-run-chromatography-dropdown", "value"),
        Input("start-run-bio-standards-dropdown", "value"),
        Input("start-run-qc-configs-dropdown", "value"),
        Input("sequence-upload-button", "contents"),
        State("sequence-upload-button", "filename"),
        Input("metadata-upload-button", "contents"),
        State("metadata-upload-button", "filename"),
        Input("data-acquisition-folder-path", "value"),
        State("instrument-run-id", "valid"),
        State("instrument-run-id", "invalid"),
        State("start-run-chromatography-dropdown", "valid"),
        State("start-run-chromatography-dropdown", "invalid"),
        State("start-run-qc-configs-dropdown", "valid"),
        State("start-run-qc-configs-dropdown", "invalid"),
        State("sequence-path", "valid"),
        State("sequence-path", "invalid"),
        State("metadata-path", "valid"),
        State("metadata-path", "invalid"),
        State("data-acquisition-folder-path", "valid"),
        State("data-acquisition-folder-path", "invalid"),
        State("selected-instrument", "data"),
        prevent_initial_call=True,
    )
    def validation_feedback_for_new_run_setup_form(
        run_id, chromatography, bio_standards, qc_config,
        sequence_contents, sequence_filename,
        metadata_contents, metadata_filename,
        data_acquisition_path,
        run_id_valid, run_id_invalid,
        chromatography_valid, chromatography_invalid,
        qc_config_valid, qc_config_invalid,
        sequence_valid, sequence_invalid,
        metadata_valid, metadata_invalid,
        path_valid, path_invalid,
        instrument,
    ):
        """Extensive form validation for setting up a new Rapid-QC-MS job."""
        if run_id is not None:
            with get_session() as session:
                existing_ids = [r.id for r in list_runs_for_instrument(session, instrument)]
            if run_id not in existing_ids:
                run_id_valid, run_id_invalid = True, False
            else:
                run_id_valid, run_id_invalid = False, True

        if chromatography is not None:
            if _HAS_QC and hasattr(_qc, "chromatography_valid"):
                if _qc.chromatography_valid(chromatography):
                    chromatography_valid, chromatography_invalid = True, False
                else:
                    chromatography_valid, chromatography_invalid = False, True
            else:
                chromatography_valid, chromatography_invalid = True, False

        if qc_config is not None:
            qc_config_valid = True

        if sequence_contents is not None:
            content_type, content_string = sequence_contents.split(",")
            decoded = base64.b64decode(content_string)
            seq_file = io.StringIO(decoded.decode("utf-8"))
            if _HAS_QC and hasattr(_qc, "sequence_is_valid"):
                if _qc.sequence_is_valid(sequence_filename, seq_file):
                    sequence_valid, sequence_invalid = True, False
                else:
                    sequence_valid, sequence_invalid = False, True
            else:
                sequence_valid, sequence_invalid = True, False

        if metadata_contents is not None:
            content_type, content_string = metadata_contents.split(",")
            decoded = base64.b64decode(content_string)
            meta_file = io.StringIO(decoded.decode("utf-8"))
            if _HAS_QC and hasattr(_qc, "metadata_is_valid"):
                if _qc.metadata_is_valid(metadata_filename, meta_file):
                    metadata_valid, metadata_invalid = True, False
                else:
                    metadata_valid, metadata_invalid = False, True
            else:
                metadata_valid, metadata_invalid = True, False

        if data_acquisition_path is not None:
            if os.path.exists(data_acquisition_path):
                path_valid, path_invalid = True, False
            else:
                path_valid, path_invalid = False, True

        return (
            run_id_valid, run_id_invalid,
            chromatography_valid, chromatography_invalid,
            qc_config_valid, qc_config_invalid,
            sequence_valid, sequence_invalid,
            metadata_valid, metadata_invalid,
            path_valid, path_invalid,
        )

    @app.callback(
        Output("monitor-new-run-button", "disabled"),
        Input("instrument-run-id", "valid"),
        Input("start-run-chromatography-dropdown", "valid"),
        Input("start-run-qc-configs-dropdown", "valid"),
        Input("sequence-path", "valid"),
        Input("data-acquisition-folder-path", "valid"),
        prevent_initial_call=True,
    )
    def enable_new_autoqc_job_button(
        run_id_valid, chromatography_valid, qc_config_valid, sequence_valid, path_valid
    ):
        """Enables 'submit' button for New Rapid-QC-MS Job form."""
        if run_id_valid and chromatography_valid and qc_config_valid and sequence_valid and path_valid:
            return False
        return True

    @app.callback(
        Output("start-run-monitor-modal", "is_open"),
        Output("new-job-error-modal", "is_open"),
        Input("monitor-new-run-button", "n_clicks"),
        State("instrument-run-id", "value"),
        State("selected-instrument", "data"),
        State("start-run-chromatography-dropdown", "value"),
        State("start-run-bio-standards-dropdown", "value"),
        State("new-sequence", "data"),
        State("new-metadata", "data"),
        State("data-acquisition-folder-path", "value"),
        State("start-run-qc-configs-dropdown", "value"),
        prevent_initial_call=True,
    )
    def new_autoqc_job_setup(
        button_clicks, run_id, instrument_id, chromatography, bio_standards,
        sequence, metadata, acquisition_path, qc_config_id,
    ):
        """Creates new run record in the database.

        Note: starting the acquisition listener is done separately via
        `rapidqcms listen` on the instrument computer — not from the dashboard.
        """
        if run_id is None:
            return False, True

        with get_session() as session:
            existing_ids = [r.id for r in list_runs_for_instrument(session, instrument_id)]
            if run_id in existing_ids:
                return False, True

            # Store chromatography in experiment_type for display purposes
            experiment_label = f"{chromatography or 'unknown'}"
            create_run(
                session,
                run_id=run_id,
                instrument_id=instrument_id,
                experiment_type=experiment_label,
            )
            session.commit()

        return True, False

    # -------------------------------------------------------------------------
    # File explorer (pure UI — no DB calls needed)
    # -------------------------------------------------------------------------

    @app.callback(
        Output("file-explorer-modal", "is_open"),
        Input("data-acquisition-folder-button", "n_clicks"),
        Input("file-explorer-select-button", "n_clicks"),
        State("setup-new-run-modal", "is_open"),
        Input("msdial-folder-button", "n_clicks"),
        prevent_initial_call=True,
    )
    def open_file_explorer(
        new_job_browse_folder_button, select_folder_button,
        new_run_modal_is_open, msdial_select_folder_button,
    ):
        """Opens custom file explorer modal."""
        button = ctx.triggered_id
        if button in ("msdial-folder-button", "data-acquisition-folder-button"):
            return True
        elif button == "file-explorer-select-button":
            return False
        raise PreventUpdate

    @app.callback(
        Output("file-explorer-modal-body", "children"),
        Input("file-explorer-modal", "is_open"),
        Input("selected-data-folder", "data"),
        Input("selected-msdial-folder", "data"),
        State("settings-modal", "is_open"),
        prevent_initial_call=True,
    )
    def list_directories_in_file_explorer(
        file_explorer_is_open, selected_data_folder, selected_msdial_folder, settings_is_open
    ):
        """Lists directories for a user to select in the file explorer modal."""
        if not file_explorer_is_open:
            raise PreventUpdate

        link_components = []
        start_folder = None

        if not settings_is_open and selected_data_folder is not None:
            start_folder = selected_data_folder
        elif settings_is_open and selected_msdial_folder is not None:
            start_folder = selected_msdial_folder

        if start_folder is None:
            start_folder = "/Users/" if sys.platform == "darwin" else "C:/"

        try:
            folders = [f.path for f in os.scandir(start_folder) if f.is_dir()]
        except Exception:
            folders = []

        for index, folder in enumerate(folders[:30]):
            link = html.A(folder, href="#", id="dir-" + str(index + 1))
            link_components.extend([link, html.Br()])

        for index in range(len(folders), 30):
            link_components.append(html.A("", id="dir-" + str(index + 1)))

        return link_components

    @app.callback(
        Output("selected-data-folder", "data"),
        Output("selected-msdial-folder", "data"),
        Input("dir-1", "n_clicks"), Input("dir-2", "n_clicks"), Input("dir-3", "n_clicks"),
        Input("dir-4", "n_clicks"), Input("dir-5", "n_clicks"), Input("dir-6", "n_clicks"),
        Input("dir-7", "n_clicks"), Input("dir-8", "n_clicks"), Input("dir-9", "n_clicks"),
        Input("dir-10", "n_clicks"), Input("dir-11", "n_clicks"), Input("dir-12", "n_clicks"),
        Input("dir-13", "n_clicks"), Input("dir-14", "n_clicks"), Input("dir-15", "n_clicks"),
        Input("dir-16", "n_clicks"), Input("dir-17", "n_clicks"), Input("dir-18", "n_clicks"),
        Input("dir-19", "n_clicks"), Input("dir-20", "n_clicks"), Input("dir-21", "n_clicks"),
        Input("dir-22", "n_clicks"), Input("dir-23", "n_clicks"), Input("dir-24", "n_clicks"),
        Input("dir-25", "n_clicks"), Input("dir-26", "n_clicks"), Input("dir-27", "n_clicks"),
        Input("dir-28", "n_clicks"), Input("dir-29", "n_clicks"), Input("dir-30", "n_clicks"),
        State("dir-1", "children"), State("dir-2", "children"), State("dir-3", "children"),
        State("dir-4", "children"), State("dir-5", "children"), State("dir-6", "children"),
        State("dir-7", "children"), State("dir-8", "children"), State("dir-9", "children"),
        State("dir-10", "children"), State("dir-11", "children"), State("dir-12", "children"),
        State("dir-13", "children"), State("dir-14", "children"), State("dir-15", "children"),
        State("dir-16", "children"), State("dir-17", "children"), State("dir-18", "children"),
        State("dir-19", "children"), State("dir-20", "children"), State("dir-21", "children"),
        State("dir-22", "children"), State("dir-23", "children"), State("dir-24", "children"),
        State("dir-25", "children"), State("dir-26", "children"), State("dir-27", "children"),
        State("dir-28", "children"), State("dir-29", "children"), State("dir-30", "children"),
        Input("selected-data-folder", "data"),
        Input("selected-msdial-folder", "data"),
        Input("file-explorer-back-button", "n_clicks"),
        State("settings-modal", "is_open"),
        prevent_initial_call=True,
    )
    def the_most_inefficient_callback_in_history(
        *args,
    ):
        """Handles user selection of folder in the file explorer modal."""
        # Unpack: 30 n_clicks, 30 children, data_folder, msdial_folder, back_button, settings_is_open
        n_clicks = args[:30]
        children = args[30:60]
        selected_data_folder = args[60]
        selected_msdial_folder = args[61]
        back_button = args[62]
        settings_is_open = args[63]

        if settings_is_open:
            selected_folder = selected_msdial_folder
        else:
            selected_folder = selected_data_folder

        if selected_folder is None:
            start = "/Users/" if sys.platform == "darwin" else "C:/Users/"
            if settings_is_open:
                return None, start
            return start, None

        selected_component = ctx.triggered_id

        if selected_component == "file-explorer-back-button":
            parts = selected_folder.rstrip("/").rsplit("/", 1)
            previous = parts[0] + "/" if len(parts) > 1 else "/"
            if settings_is_open:
                return None, previous
            return previous, None

        # Map component IDs to children values
        components = tuple("dir-" + str(i + 1) for i in range(30))
        folders = dict(zip(components, children))

        if selected_component in folders:
            selected_folder = folders[selected_component]
            if selected_folder:
                selected_folder = selected_folder.replace("\\", "/")
                if settings_is_open:
                    return None, selected_folder
                return selected_folder, None

        raise PreventUpdate

    @app.callback(
        Output("file-explorer-modal-title", "children"),
        Input("selected-data-folder", "data"),
        Input("selected-msdial-folder", "data"),
        State("settings-modal", "is_open"),
        prevent_initial_call=True,
    )
    def update_file_explorer_title(selected_data_folder, selected_msdial_folder, settings_is_open):
        """Populates file explorer title with current folder."""
        if not settings_is_open:
            return selected_data_folder
        return selected_msdial_folder

    @app.callback(
        Output("data-acquisition-folder-path", "value"),
        Input("file-explorer-select-button", "n_clicks"),
        State("selected-data-folder", "data"),
        State("settings-modal", "is_open"),
        prevent_initial_call=True,
    )
    def update_folder_path_text_field(select_folder_button, selected_folder, settings_is_open):
        """Populates data acquisition path text field with user selection."""
        if not settings_is_open:
            return selected_folder

    @app.callback(
        Output("setup-new-run-button", "style"),
        Input("filter-date-range", "value"),
        prevent_initial_call=True,
    )
    def hide_elements_for_non_instrument_devices(_):
        """Shows new run button (always visible on server dashboard)."""
        with get_session() as session:
            if list_instruments(session):
                return {"display": "block", "margin-top": "15px", "line-height": "1.75"}
        raise PreventUpdate

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
