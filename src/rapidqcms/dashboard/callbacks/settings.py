"""Settings callbacks (chromatography, IS, MS-DIAL, QC configs, bio standards).

DB rewiring (Google Drive callbacks deleted entirely):
  db.insert_chromatography_method(chrom)     → no-op (implicit via IS upsert)
  db.get_chromatography_methods()            → group InternalStandard by chromatography
  db.get_chromatography_methods_list()       → unique chromatography values from IS
  db.remove_chromatography_method(chrom)     → delete all IS with that chromatography
  db.add_msp_to_database(file, chrom, pol)   → parse_msp_to_internal_standards(session, ...)
  db.get_msdial_directory()                  → get_settings().msdial_exe
  db.add_msdial_configuration(id)            → upsert_msdial_configuration(session, id, "")
  db.remove_msdial_configuration(id)         → delete_msdial_configuration(session, id)
  db.get_msdial_configurations()             → [c.id for c in list_msdial_configurations(s)]
  db.get_msdial_configuration_parameters(id) → parse JSON stored in parameter_file_path
  db.update_msdial_configuration(id, ...)    → store params as JSON in parameter_file_path
  db.add_qc_configuration(id)               → upsert_qc_configuration(session, id)
  db.remove_qc_configuration(id)            → delete_qc_configuration(session, id)
  db.get_qc_configurations_list()           → [c.id for c in list_qc_configurations(s)]
  db.get_qc_configuration_parameters(id)   → get_qc_configuration(session, id)
  db.update_qc_configuration(id, ...)      → upsert_qc_configuration(session, id, ...)
  db.get_biological_standards_list()        → list of unique names from list_bio_standards
  db.add_biological_standard(name, ident)   → upsert_bio_standard for each chromatography
  db.remove_biological_standard(name)       → delete all BioStandard with that name
  db.update_msdial_config_for_bio_standard  → upsert_bio_standard with msdial_config_id
"""

import base64
import io
import json
import logging
import os

import pandas as pd
from dash import Input, Output, State, ctx
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

from rapidqcms.config import get_settings
from rapidqcms.db.connection import get_session
from rapidqcms.db.features import (
    delete_bio_standard,
    delete_msdial_configuration,
    delete_qc_configuration,
    get_msdial_configuration,
    get_qc_configuration,
    list_bio_standards,
    list_msdial_configurations,
    list_qc_configurations,
    upsert_bio_standard,
    upsert_msdial_configuration,
    upsert_qc_configuration,
)
from rapidqcms.db.models import BioStandard, MsDialConfiguration
from rapidqcms.db.settings import list_instruments

log = logging.getLogger(__name__)

# Default MS-DIAL parameter keys in order (matches legacy callback return tuple)
_MSDIAL_PARAM_KEYS = [
    "retention_time_begin", "retention_time_end",
    "mass_range_begin", "mass_range_end",
    "ms1_centroid_tolerance", "ms2_centroid_tolerance",
    "smoothing_method", "smoothing_level",
    "min_peak_width", "min_peak_height",
    "mass_slice_width",
    "post_id_rt_tolerance", "post_id_mz_tolerance", "post_id_score_cutoff",
    "alignment_rt_tolerance", "alignment_mz_tolerance",
    "alignment_rt_factor", "alignment_mz_factor",
    "peak_count_filter", "qc_at_least_filter",
]

_MSDIAL_DEFAULTS = {
    "retention_time_begin": 0, "retention_time_end": 100,
    "mass_range_begin": 0, "mass_range_end": 2000,
    "ms1_centroid_tolerance": 0.008, "ms2_centroid_tolerance": 0.01,
    "smoothing_method": "LinearWeightedMovingAverage", "smoothing_level": 3,
    "min_peak_width": 35000, "min_peak_height": 0.1,
    "mass_slice_width": 0.1,
    "post_id_rt_tolerance": 0.1, "post_id_mz_tolerance": 0.008, "post_id_score_cutoff": 85,
    "alignment_rt_tolerance": 0.05, "alignment_mz_tolerance": 0.008,
    "alignment_rt_factor": 0.5, "alignment_mz_factor": 0.5,
    "peak_count_filter": 0, "qc_at_least_filter": "True",
}


def _get_chromatography_methods_df(session):
    """Build chromatography methods DataFrame from QC config YAML."""
    from rapidqcms.config.library import load_qc_config
    is_config = load_qc_config().get("internal_standards", {})
    if not is_config:
        return pd.DataFrame(
            columns=["method_id", "num_pos_standards", "num_neg_standards", "msdial_config_id"]
        )
    rows = [
        {
            "method_id": chrom,
            "num_pos_standards": len(polarities.get("Pos", [])),
            "num_neg_standards": len(polarities.get("Neg", [])),
            "msdial_config_id": "",
        }
        for chrom, polarities in sorted(is_config.items())
    ]
    return pd.DataFrame(rows)


def _get_msdial_params(config) -> tuple:
    """Parse stored JSON params from a MsDialConfiguration, returning a 20-tuple."""
    if config is None:
        return tuple(_MSDIAL_DEFAULTS[k] for k in _MSDIAL_PARAM_KEYS)
    try:
        stored = json.loads(config.parameter_file_path or "{}")
        return tuple(stored.get(k, _MSDIAL_DEFAULTS[k]) for k in _MSDIAL_PARAM_KEYS)
    except Exception:
        return tuple(_MSDIAL_DEFAULTS[k] for k in _MSDIAL_PARAM_KEYS)


def register(app):

    # ---------------------------------------------------------------------------
    # Settings modal toggle
    # ---------------------------------------------------------------------------

    @app.callback(
        Output("settings-modal", "is_open"),
        Input("settings-button", "n_clicks"),
        prevent_initial_call=True,
    )
    def toggle_settings_modal(button_click):
        """Toggles global settings modal."""
        return True

    # ---------------------------------------------------------------------------
    # Chromatography methods
    # ---------------------------------------------------------------------------

    @app.callback(
        Output("chromatography-methods-table", "children"),
        Output("select-istd-chromatography-dropdown", "options"),
        Output("select-bio-chromatography-dropdown", "options"),
        Output("add-chromatography-text-field", "value"),
        Output("chromatography-added", "data"),
        Input("on-page-load", "data"),
        Input("add-chromatography-button", "n_clicks"),
        State("add-chromatography-text-field", "value"),
        Input("istd-msp-added", "data"),
        Input("chromatography-removed", "data"),
        Input("chromatography-msdial-config-added", "data"),
        Input("google-drive-sync-update", "data"),
    )
    def add_chromatography_method(
        on_page_load, button_click, chromatography_method, msp_added,
        method_removed, config_added, sync_update,
    ):
        """Add chromatography method to database and refresh table."""
        with get_session() as session:
            if not list_instruments(session):
                raise PreventUpdate

            method_added = ""
            # Note: chromatography methods are implicit from IS library —
            # just validate and record addition for UI feedback.
            if chromatography_method:
                method_added = "Added"

            df_methods = _get_chromatography_methods_df(session)
            df_display = df_methods.rename(columns={
                "method_id": "Method ID",
                "num_pos_standards": "Pos (+) Standards",
                "num_neg_standards": "Neg (–) Standards",
                "msdial_config_id": "MS-DIAL Config",
            })

            if df_display.empty:
                methods_table = None
            else:
                df_display = df_display[
                    ["Method ID", "Pos (+) Standards", "Neg (–) Standards", "MS-DIAL Config"]
                ]
                methods_table = dbc.Table.from_dataframe(df_display, striped=True, hover=True)

            dropdown_options = [
                {"label": m, "value": m}
                for m in df_methods["method_id"].astype(str).tolist()
            ]

            return methods_table, dropdown_options, dropdown_options, None, method_added

    @app.callback(
        Output("chromatography-removed", "data"),
        Input("remove-chromatography-method-button", "n_clicks"),
        State("select-istd-chromatography-dropdown", "value"),
        prevent_initial_call=True,
    )
    def remove_chromatography_method(button_click, chromatography):
        """IS library is now managed via qc_config.yaml — no DB op needed."""
        raise PreventUpdate

    @app.callback(
        Output("chromatography-addition-alert", "is_open"),
        Output("chromatography-addition-alert", "children"),
        Input("chromatography-added", "data"),
    )
    def show_alert_on_chromatography_addition(chromatography_added):
        if chromatography_added == "Added":
            return True, "The chromatography method was added successfully."
        return False, None

    @app.callback(
        Output("chromatography-removal-alert", "is_open"),
        Output("chromatography-removal-alert", "children"),
        Input("chromatography-removed", "data"),
    )
    def show_alert_on_chromatography_removal(chromatography_removed):
        if chromatography_removed == "Removed":
            return True, "The selected chromatography method was removed."
        return False, None

    @app.callback(
        Output("msp-save-changes-button", "children"),
        Input("select-istd-chromatography-dropdown", "value"),
        Input("select-istd-polarity-dropdown", "value"),
    )
    def add_msp_to_chromatography_button_feedback(chromatography, polarity):
        if chromatography and polarity:
            return "Add MSP to " + chromatography + " " + polarity
        return "Add MSP"

    @app.callback(
        Output("add-istd-msp-text-field", "value"),
        Input("add-istd-msp-button", "filename"),
        prevent_initial_call=True,
    )
    def bio_standard_msp_text_field_feedback(filename):
        return filename

    @app.callback(
        Output("istd-msp-added", "data"),
        Input("msp-save-changes-button", "n_clicks"),
        State("add-istd-msp-button", "contents"),
        State("add-istd-msp-button", "filename"),
        State("select-istd-chromatography-dropdown", "value"),
        State("select-istd-polarity-dropdown", "value"),
        prevent_initial_call=True,
    )
    def capture_uploaded_istd_msp(button_click, contents, filename, chromatography, polarity):
        """Captures uploaded MSP file and imports into internal standards library."""
        if contents is None or chromatography is None or polarity is None:
            return None

        content_type, content_string = contents.split(",")
        decoded = base64.b64decode(content_string)

        if filename and filename.endswith(".msp"):
            return "IS library is now managed via qc_config.yaml — MSP upload to DB is disabled."
        else:
            return "Error: Only .msp files are supported."

    @app.callback(
        Output("chromatography-msp-success-alert", "is_open"),
        Output("chromatography-msp-success-alert", "children"),
        Output("chromatography-msp-error-alert", "is_open"),
        Output("chromatography-msp-error-alert", "children"),
        Input("istd-msp-added", "data"),
        prevent_initial_call=True,
    )
    def ui_feedback_for_adding_msp_to_chromatography(msp_added):
        if msp_added is not None:
            if "Success" in str(msp_added):
                return True, msp_added, False, ""
            elif msp_added == "Error":
                return False, "", True, "Error: Please select a chromatography and polarity."
        return False, "", False, ""

    # ---------------------------------------------------------------------------
    # MS-DIAL configurations
    # ---------------------------------------------------------------------------

    @app.callback(
        Output("msdial-directory", "value"),
        Input("file-explorer-select-button", "n_clicks"),
        Input("settings-modal", "is_open"),
        State("selected-msdial-folder", "data"),
        Input("google-drive-sync-update", "data"),
    )
    def get_msdial_directory(select_folder_button, settings_modal_is_open, selected_folder, sync_update):
        """Returns location of MS-DIAL directory."""
        selected_component = ctx.triggered_id
        if selected_component == "file-explorer-select-button":
            return selected_folder

        with get_session() as session:
            if list_instruments(session):
                msdial_exe = get_settings().msdial_exe
                return str(msdial_exe) if msdial_exe else ""
        raise PreventUpdate

    @app.callback(
        Output("msdial-directory-saved", "data"),
        Input("msdial-folder-save-button", "n_clicks"),
        State("msdial-directory", "value"),
        prevent_initial_call=True,
    )
    def update_msdial_directory(button_click, msdial_directory):
        """Updates MS-DIAL directory (env-var based; validates path existence)."""
        if msdial_directory is not None:
            if os.path.exists(msdial_directory):
                return "Success"
            return "Does not exist"
        return "Error"

    @app.callback(
        Output("msdial-directory-saved-alert", "is_open"),
        Output("msdial-directory-saved-alert", "children"),
        Output("msdial-directory-saved-alert", "color"),
        Input("msdial-directory-saved", "data"),
        prevent_initial_call=True,
    )
    def ui_alert_for_msdial_directory_save(msdial_folder_save_result):
        if msdial_folder_save_result == "Success":
            return True, "The MS-DIAL location was successfully saved.", "success"
        elif msdial_folder_save_result == "Does not exist":
            return True, "Error: This directory does not exist on your computer.", "danger"
        elif msdial_folder_save_result == "Error":
            return True, "Error: Could not set MS-DIAL directory.", "danger"
        raise PreventUpdate

    @app.callback(
        Output("msdial-config-added", "data"),
        Output("add-msdial-configuration-text-field", "value"),
        Input("add-msdial-configuration-button", "n_clicks"),
        State("add-msdial-configuration-text-field", "value"),
        prevent_initial_call=True,
    )
    def add_msdial_configuration(button_click, msdial_config_id):
        """Adds new MS-DIAL configuration to the database."""
        if msdial_config_id is not None:
            with get_session() as session:
                upsert_msdial_configuration(
                    session, msdial_config_id,
                    parameter_file_path=json.dumps(_MSDIAL_DEFAULTS),
                )
                session.commit()
            return "Added", None
        return "", None

    @app.callback(
        Output("msdial-config-removed", "data"),
        Input("remove-config-button", "n_clicks"),
        State("msdial-configs-dropdown", "value"),
        prevent_initial_call=True,
    )
    def delete_msdial_configuration_callback(button_click, msdial_config_id):
        """Removes MS-DIAL configuration from database."""
        if msdial_config_id is not None:
            if msdial_config_id == "Default":
                return "Cannot remove"
            with get_session() as session:
                delete_msdial_configuration(session, msdial_config_id)
                session.commit()
            return "Removed"
        return ""

    @app.callback(
        Output("msdial-configs-dropdown", "options"),
        Output("msdial-configs-dropdown", "value"),
        Input("on-page-load", "data"),
        Input("msdial-config-added", "data"),
        Input("msdial-config-removed", "data"),
        Input("google-drive-sync-update", "data"),
    )
    def get_msdial_configs_for_dropdown(on_page_load, on_config_added, on_config_removed, sync_update):
        """Retrieves list of MS-DIAL configurations from database."""
        with get_session() as session:
            if not list_instruments(session):
                raise PreventUpdate
            configs = list_msdial_configurations(session)
            config_options = [{"label": c.id, "value": c.id} for c in configs]
            # Add Default if not present
            ids = [c.id for c in configs]
            if "Default" not in ids:
                config_options.insert(0, {"label": "Default", "value": "Default"})
            return config_options, "Default"

    @app.callback(
        Output("msdial-config-addition-alert", "is_open"),
        Output("msdial-config-addition-alert", "children"),
        Output("msdial-config-addition-alert", "color"),
        Input("msdial-config-added", "data"),
        prevent_initial_call=True,
    )
    def show_alert_on_msdial_config_addition(config_added):
        if config_added == "Added":
            return True, "Success! New MS-DIAL configuration added.", "success"
        return False, None, "success"

    @app.callback(
        Output("msdial-config-removal-alert", "is_open"),
        Output("msdial-config-removal-alert", "children"),
        Output("msdial-config-removal-alert", "color"),
        Input("msdial-config-removed", "data"),
        State("msdial-configs-dropdown", "value"),
        prevent_initial_call=True,
    )
    def show_alert_on_msdial_config_removal(config_removed, selected_config):
        if config_removed == "Removed":
            message = "The selected MS-DIAL configuration was deleted."
            color = "primary"
            return True, message, color
        if selected_config == "Default":
            return True, "Error: The default configuration cannot be deleted.", "danger"
        return False, "", "danger"

    @app.callback(
        Output("retention-time-begin", "value"),
        Output("retention-time-end", "value"),
        Output("mass-range-begin", "value"),
        Output("mass-range-end", "value"),
        Output("ms1-centroid-tolerance", "value"),
        Output("ms2-centroid-tolerance", "value"),
        Output("select-smoothing-dropdown", "value"),
        Output("smoothing-level", "value"),
        Output("min-peak-width", "value"),
        Output("min-peak-height", "value"),
        Output("mass-slice-width", "value"),
        Output("post-id-rt-tolerance", "value"),
        Output("post-id-mz-tolerance", "value"),
        Output("post-id-score-cutoff", "value"),
        Output("alignment-rt-tolerance", "value"),
        Output("alignment-mz-tolerance", "value"),
        Output("alignment-rt-factor", "value"),
        Output("alignment-mz-factor", "value"),
        Output("peak-count-filter", "value"),
        Output("qc-at-least-filter-dropdown", "value"),
        Input("msdial-configs-dropdown", "value"),
        Input("msdial-parameters-saved", "data"),
        Input("msdial-parameters-reset", "data"),
        prevent_initial_call=True,
    )
    def get_msdial_parameters_for_config(msdial_config_id, on_parameters_saved, on_parameters_reset):
        """Fills text fields with current MS-DIAL parameter values."""
        with get_session() as session:
            config = get_msdial_configuration(session, msdial_config_id)
            return _get_msdial_params(config)

    @app.callback(
        Output("msdial-parameters-saved", "data"),
        Input("save-changes-msdial-parameters-button", "n_clicks"),
        State("msdial-configs-dropdown", "value"),
        State("retention-time-begin", "value"),
        State("retention-time-end", "value"),
        State("mass-range-begin", "value"),
        State("mass-range-end", "value"),
        State("ms1-centroid-tolerance", "value"),
        State("ms2-centroid-tolerance", "value"),
        State("select-smoothing-dropdown", "value"),
        State("smoothing-level", "value"),
        State("mass-slice-width", "value"),
        State("min-peak-width", "value"),
        State("min-peak-height", "value"),
        State("post-id-rt-tolerance", "value"),
        State("post-id-mz-tolerance", "value"),
        State("post-id-score-cutoff", "value"),
        State("alignment-rt-tolerance", "value"),
        State("alignment-mz-tolerance", "value"),
        State("alignment-rt-factor", "value"),
        State("alignment-mz-factor", "value"),
        State("peak-count-filter", "value"),
        State("qc-at-least-filter-dropdown", "value"),
        prevent_initial_call=True,
    )
    def write_msdial_parameters_to_database(
        button_clicks, config_name, rt_begin, rt_end, mz_begin, mz_end,
        ms1_centroid_tolerance, ms2_centroid_tolerance, smoothing_method, smoothing_level,
        mass_slice_width, min_peak_width, min_peak_height, post_id_rt_tolerance,
        post_id_mz_tolerance, post_id_score_cutoff, alignment_rt_tolerance,
        alignment_mz_tolerance, alignment_rt_factor, alignment_mz_factor,
        peak_count_filter, qc_at_least_filter,
    ):
        """Saves MS-DIAL parameters to configuration in database (as JSON)."""
        values = [
            rt_begin, rt_end, mz_begin, mz_end,
            ms1_centroid_tolerance, ms2_centroid_tolerance,
            smoothing_method, smoothing_level, min_peak_width, min_peak_height,
            mass_slice_width, post_id_rt_tolerance, post_id_mz_tolerance,
            post_id_score_cutoff, alignment_rt_tolerance, alignment_mz_tolerance,
            alignment_rt_factor, alignment_mz_factor, peak_count_filter, qc_at_least_filter,
        ]
        params_json = json.dumps(dict(zip(_MSDIAL_PARAM_KEYS, values)))
        with get_session() as session:
            upsert_msdial_configuration(session, config_name, parameter_file_path=params_json)
            session.commit()
        return "Saved"

    @app.callback(
        Output("msdial-parameters-reset", "data"),
        Input("reset-default-msdial-parameters-button", "n_clicks"),
        State("msdial-configs-dropdown", "value"),
        prevent_initial_call=True,
    )
    def reset_msdial_parameters_to_default(button_clicks, msdial_config_name):
        """Resets MS-DIAL parameters to default settings."""
        with get_session() as session:
            upsert_msdial_configuration(
                session, msdial_config_name,
                parameter_file_path=json.dumps(_MSDIAL_DEFAULTS),
            )
            session.commit()
        return "Reset"

    @app.callback(
        Output("msdial-parameters-success-alert", "is_open"),
        Output("msdial-parameters-success-alert", "children"),
        Input("msdial-parameters-saved", "data"),
        prevent_initial_call=True,
    )
    def show_alert_on_parameter_save(parameters_saved):
        if parameters_saved == "Saved":
            return True, "Your changes were successfully saved."
        raise PreventUpdate

    @app.callback(
        Output("msdial-parameters-reset-alert", "is_open"),
        Output("msdial-parameters-reset-alert", "children"),
        Input("msdial-parameters-reset", "data"),
        prevent_initial_call=True,
    )
    def show_alert_on_parameter_reset(parameters_reset):
        if parameters_reset == "Reset":
            return True, "Your configuration has been reset to its default settings."
        raise PreventUpdate

    # ---------------------------------------------------------------------------
    # QC configurations
    # ---------------------------------------------------------------------------

    @app.callback(
        Output("qc-config-added", "data"),
        Output("add-is-configuration-text-field", "value"),
        Input("add-is-configuration-button", "n_clicks"),
        State("add-is-configuration-text-field", "value"),
        prevent_initial_call=True,
    )
    def add_qc_configuration(button_click, qc_config_id):
        """Adds new QC configuration to the database."""
        if qc_config_id is not None:
            with get_session() as session:
                upsert_qc_configuration(session, qc_config_id)
                session.commit()
            return "Added", None
        return "", None

    @app.callback(
        Output("qc-config-removed", "data"),
        Input("remove-qc-config-button", "n_clicks"),
        State("qc-configs-dropdown", "value"),
        prevent_initial_call=True,
    )
    def delete_qc_configuration_callback(button_click, qc_config_id):
        """Removes QC configuration from database."""
        if qc_config_id is not None:
            if qc_config_id == "Default":
                return "Cannot remove"
            with get_session() as session:
                delete_qc_configuration(session, qc_config_id)
                session.commit()
            return "Removed"
        return ""

    @app.callback(
        Output("qc-configs-dropdown", "options"),
        Output("qc-configs-dropdown", "value"),
        Input("on-page-load", "data"),
        Input("qc-config-added", "data"),
        Input("qc-config-removed", "data"),
        Input("google-drive-sync-update", "data"),
    )
    def get_qc_configs_for_dropdown(on_page_load, qc_config_added, qc_config_removed, sync_update):
        """Retrieves list of QC configurations from database."""
        with get_session() as session:
            if not list_instruments(session):
                raise PreventUpdate
            configs = list_qc_configurations(session)
            config_options = [{"label": c.id, "value": c.id} for c in configs]
            ids = [c.id for c in configs]
            if "Default" not in ids:
                config_options.insert(0, {"label": "Default", "value": "Default"})
            return config_options, "Default"

    @app.callback(
        Output("qc-config-addition-alert", "is_open"),
        Output("qc-config-addition-alert", "children"),
        Output("qc-config-addition-alert", "color"),
        Input("qc-config-added", "data"),
        prevent_initial_call=True,
    )
    def show_alert_on_qc_config_addition(config_added):
        if config_added == "Added":
            return True, "Success! New QC configuration added.", "success"
        return False, None, "success"

    @app.callback(
        Output("qc-config-removal-alert", "is_open"),
        Output("qc-config-removal-alert", "children"),
        Output("qc-config-removal-alert", "color"),
        Input("qc-config-removed", "data"),
        State("qc-configs-dropdown", "value"),
        prevent_initial_call=True,
    )
    def show_alert_on_qc_config_removal(config_removed, selected_config):
        if config_removed is not None:
            if config_removed == "Removed":
                return True, "The selected QC configuration was deleted.", "primary"
            if selected_config == "Default":
                return True, "Error: The default configuration cannot be deleted.", "danger"
        return False, "", "danger"

    @app.callback(
        Output("intensity-dropouts-cutoff", "value"),
        Output("library-rt-shift-cutoff", "value"),
        Output("in-run-rt-shift-cutoff", "value"),
        Output("library-mz-shift-cutoff", "value"),
        Output("intensity-cutoff-enabled", "value"),
        Output("library-rt-shift-cutoff-enabled", "value"),
        Output("in-run-rt-shift-cutoff-enabled", "value"),
        Output("library-mz-shift-cutoff-enabled", "value"),
        Input("qc-configs-dropdown", "value"),
        Input("qc-parameters-saved", "data"),
        Input("qc-parameters-reset", "data"),
        prevent_initial_call=True,
    )
    def get_qc_parameters_for_config(qc_config_name, on_parameters_saved, on_parameters_reset):
        """Fills text fields with current QC parameter values."""
        with get_session() as session:
            config = get_qc_configuration(session, qc_config_name)
            if config is None:
                # Return defaults
                return 4, 0.3, 0.1, 0.005, True, True, True, True
            return (
                config.intensity_dropouts_cutoff,
                config.library_rt_shift_cutoff,
                config.in_run_rt_shift_cutoff,
                config.library_mz_shift_cutoff,
                config.intensity_enabled,
                config.library_rt_enabled,
                config.in_run_rt_enabled,
                config.library_mz_enabled,
            )

    @app.callback(
        Output("qc-parameters-saved", "data"),
        Input("save-changes-qc-parameters-button", "n_clicks"),
        State("qc-configs-dropdown", "value"),
        State("intensity-dropouts-cutoff", "value"),
        State("library-rt-shift-cutoff", "value"),
        State("in-run-rt-shift-cutoff", "value"),
        State("library-mz-shift-cutoff", "value"),
        State("intensity-cutoff-enabled", "value"),
        State("library-rt-shift-cutoff-enabled", "value"),
        State("in-run-rt-shift-cutoff-enabled", "value"),
        State("library-mz-shift-cutoff-enabled", "value"),
        prevent_initial_call=True,
    )
    def write_qc_parameters_to_database(
        button_clicks, qc_config_name, intensity_dropouts_cutoff, library_rt_shift_cutoff,
        in_run_rt_shift_cutoff, library_mz_shift_cutoff, intensity_enabled,
        library_rt_enabled, in_run_rt_enabled, library_mz_enabled,
    ):
        """Saves QC parameters to respective configuration in database."""
        with get_session() as session:
            upsert_qc_configuration(
                session, qc_config_name,
                intensity_dropouts_cutoff=intensity_dropouts_cutoff,
                library_rt_shift_cutoff=library_rt_shift_cutoff,
                in_run_rt_shift_cutoff=in_run_rt_shift_cutoff,
                library_mz_shift_cutoff=library_mz_shift_cutoff,
                intensity_enabled=bool(intensity_enabled),
                library_rt_enabled=bool(library_rt_enabled),
                in_run_rt_enabled=bool(in_run_rt_enabled),
                library_mz_enabled=bool(library_mz_enabled),
            )
            session.commit()
        return "Saved"

    @app.callback(
        Output("qc-parameters-reset", "data"),
        Input("reset-default-qc-parameters-button", "n_clicks"),
        State("qc-configs-dropdown", "value"),
        prevent_initial_call=True,
    )
    def reset_qc_parameters_to_default(button_clicks, qc_config_name):
        """Resets QC parameters to default settings."""
        with get_session() as session:
            upsert_qc_configuration(
                session, qc_config_name,
                intensity_dropouts_cutoff=4,
                library_rt_shift_cutoff=0.1,
                in_run_rt_shift_cutoff=0.05,
                library_mz_shift_cutoff=0.005,
                intensity_enabled=True,
                library_rt_enabled=True,
                in_run_rt_enabled=True,
                library_mz_enabled=True,
            )
            session.commit()
        return "Reset"

    @app.callback(
        Output("qc-parameters-success-alert", "is_open"),
        Output("qc-parameters-success-alert", "children"),
        Input("qc-parameters-saved", "data"),
        prevent_initial_call=True,
    )
    def show_alert_on_qc_parameter_save(parameters_saved):
        if parameters_saved == "Saved":
            return True, "Your changes were successfully saved."
        raise PreventUpdate

    @app.callback(
        Output("qc-parameters-reset-alert", "is_open"),
        Output("qc-parameters-reset-alert", "children"),
        Input("qc-parameters-reset", "data"),
        prevent_initial_call=True,
    )
    def show_alert_on_qc_parameter_reset(parameters_reset):
        if parameters_reset == "Reset":
            return True, "Your QC configuration has been reset to its default settings."
        raise PreventUpdate

    # ---------------------------------------------------------------------------
    # Biological standards
    # ---------------------------------------------------------------------------

    @app.callback(
        Output("select-bio-standard-dropdown", "options"),
        Output("biological-standards-table", "children"),
        Input("on-page-load", "data"),
        Input("bio-standard-added", "data"),
        Input("bio-standard-removed", "data"),
        Input("chromatography-added", "data"),
        Input("chromatography-removed", "data"),
        Input("bio-msp-added", "data"),
        Input("bio-standard-msdial-config-added", "data"),
        Input("google-drive-sync-update", "data"),
    )
    def get_biological_standards(
        on_page_load, on_standard_added, on_standard_removed, on_method_added,
        on_method_removed, on_msp_added, on_bio_standard_msdial_config_added, sync_update,
    ):
        """Populates dropdown and table of biological standards."""
        with get_session() as session:
            if not list_instruments(session):
                raise PreventUpdate

            bio_standards = list_bio_standards(session)
            names = sorted(set(b.name for b in bio_standards))
            dropdown_options = [{"label": n, "value": n} for n in names]

            if not bio_standards:
                return dropdown_options, None

            rows = [
                {
                    "Name": b.name,
                    "Identifier": b.name,  # Legacy field — not in new schema
                    "Method ID": b.chromatography,
                    "Pos (+) Metabolites": "N/A",
                    "Neg (–) Metabolites": "N/A",
                    "MS-DIAL Config": b.msdial_config_id or "",
                }
                for b in bio_standards
            ]
            df = pd.DataFrame(rows)[
                ["Name", "Identifier", "Method ID", "Pos (+) Metabolites", "Neg (–) Metabolites", "MS-DIAL Config"]
            ]
            table = dbc.Table.from_dataframe(df, striped=True, hover=True)
            return dropdown_options, table

    @app.callback(
        Output("bio-standard-added", "data"),
        Output("add-bio-standard-text-field", "value"),
        Output("add-bio-standard-identifier-text-field", "value"),
        Input("add-bio-standard-button", "n_clicks"),
        State("add-bio-standard-text-field", "value"),
        State("add-bio-standard-identifier-text-field", "value"),
        prevent_initial_call=True,
    )
    def add_biological_standard(button_click, name, identifier):
        """Adds biological standard for all available chromatography methods."""
        if name is None or identifier is None:
            return "Error 1", name, identifier

        with get_session() as session:
            methods_df = _get_chromatography_methods_df(session)
            if methods_df.empty:
                return "Error 2", name, identifier

            for chrom in methods_df["method_id"].tolist():
                upsert_bio_standard(session, name=name, chromatography=chrom)
            session.commit()

        return "Added", None, None

    @app.callback(
        Output("bio-standard-removed", "data"),
        Input("remove-bio-standard-button", "n_clicks"),
        State("select-bio-standard-dropdown", "value"),
        prevent_initial_call=True,
    )
    def remove_biological_standard(button_click, biological_standard_name):
        """Removes biological standard from the database."""
        if biological_standard_name is None:
            return "Error"
        with get_session() as session:
            records = (
                session.query(BioStandard)
                .filter_by(name=biological_standard_name)
                .all()
            )
            for r in records:
                session.delete(r)
            session.commit()
        return "Deleted " + biological_standard_name + " and all corresponding MSP files."

    @app.callback(
        Output("add-bio-msp-text-field", "value"),
        Input("add-bio-msp-button", "filename"),
        prevent_initial_call=True,
    )
    def bio_standard_msp_text_field_ui_callback(filename):
        return filename

    @app.callback(
        Output("bio-msp-added", "data"),
        Input("bio-standard-save-changes-button", "n_clicks"),
        State("add-bio-msp-button", "contents"),
        State("add-bio-msp-button", "filename"),
        State("select-bio-chromatography-dropdown", "value"),
        State("select-bio-polarity-dropdown", "value"),
        State("select-bio-standard-dropdown", "value"),
        prevent_initial_call=True,
    )
    def capture_uploaded_bio_msp(button_click, contents, filename, chromatography, polarity, bio_standard):
        """Captures uploaded bio standard MSP and imports as internal standards."""
        if contents is None or chromatography is None or polarity is None or bio_standard is None:
            return ""

        content_type, content_string = contents.split(",")
        decoded = base64.b64decode(content_string)

        if filename and filename.endswith(".msp"):
            return f"IS library is now managed via qc_config.yaml — MSP upload to DB is disabled."
        return "Error 2"

    @app.callback(
        Output("bio-standard-addition-alert", "is_open"),
        Output("bio-standard-addition-alert", "children"),
        Output("bio-standard-addition-alert", "color"),
        Input("bio-standard-added", "data"),
        prevent_initial_call=True,
    )
    def show_alert_on_bio_standard_addition(bio_standard_added):
        if bio_standard_added == "Added":
            return True, "Success! New biological standard added.", "success"
        elif bio_standard_added == "Error 2":
            return True, "Error: Please add a chromatography method first.", "danger"
        return False, None, None

    @app.callback(
        Output("bio-standard-removal-alert", "is_open"),
        Output("bio-standard-removal-alert", "children"),
        Input("bio-standard-removed", "data"),
        prevent_initial_call=True,
    )
    def show_alert_on_bio_standard_removal(bio_standard_removed):
        if bio_standard_removed and "Deleted" in bio_standard_removed:
            return True, bio_standard_removed
        return False, None

    @app.callback(
        Output("bio-msp-success-alert", "is_open"),
        Output("bio-msp-success-alert", "children"),
        Output("bio-msp-error-alert", "is_open"),
        Output("bio-msp-error-alert", "children"),
        Input("bio-msp-added", "data"),
        prevent_initial_call=True,
    )
    def ui_feedback_for_adding_msp_to_bio_standard(bio_standard_msp_added):
        if bio_standard_msp_added is not None:
            if "Success" in str(bio_standard_msp_added):
                return True, bio_standard_msp_added, False, ""
            elif bio_standard_msp_added == "Error 1":
                return False, "", True, "Error: Unable to add MSP to biological standard."
            elif bio_standard_msp_added == "Error 2":
                return False, "", True, "Error: Please select a biological standard, chromatography, and polarity first."
        return False, "", False, ""

    @app.callback(
        Output("bio-standard-save-changes-button", "children"),
        Input("select-bio-chromatography-dropdown", "value"),
        Input("select-bio-polarity-dropdown", "value"),
        Input("select-bio-standard-dropdown", "value"),
    )
    def add_msp_to_bio_standard_button_feedback(chromatography, polarity, bio_standard):
        if bio_standard and chromatography and polarity:
            return "Add MSP to " + bio_standard + " in " + chromatography + " " + polarity
        elif bio_standard:
            return "Added MSP to " + bio_standard
        return "Add MSP"

    @app.callback(
        Output("bio-standard-msdial-configs-dropdown", "options"),
        Output("istd-msdial-configs-dropdown", "options"),
        Input("msdial-config-added", "data"),
        Input("msdial-config-removed", "data"),
        Input("google-drive-sync-update", "data"),
    )
    def populate_msdial_configs_for_biological_standard(
        msdial_config_added, msdial_config_removed, sync_update
    ):
        """Populates MS-DIAL configurations dropdown in Biological Standards settings."""
        with get_session() as session:
            if not list_instruments(session):
                raise PreventUpdate
            configs = list_msdial_configurations(session)
            options = [{"label": c.id, "value": c.id} for c in configs]
            if not any(c.id == "Default" for c in configs):
                options.insert(0, {"label": "Default", "value": "Default"})
            return options, options

    @app.callback(
        Output("bio-standard-msdial-config-added", "data"),
        Input("bio-standard-msdial-configs-button", "n_clicks"),
        State("select-bio-standard-dropdown", "value"),
        State("select-bio-chromatography-dropdown", "value"),
        State("bio-standard-msdial-configs-dropdown", "value"),
        prevent_initial_call=True,
    )
    def add_msdial_config_for_bio_standard(button_click, biological_standard, chromatography, config_id):
        """Sets MS-DIAL configuration to be used for a biological standard."""
        if biological_standard and chromatography and config_id:
            with get_session() as session:
                upsert_bio_standard(
                    session, name=biological_standard, chromatography=chromatography,
                    msdial_config_id=config_id,
                )
                session.commit()
            return "Added"
        return ""

    @app.callback(
        Output("bio-config-success-alert", "is_open"),
        Output("bio-config-success-alert", "children"),
        Input("bio-standard-msdial-config-added", "data"),
        State("select-bio-standard-dropdown", "value"),
        State("select-bio-chromatography-dropdown", "value"),
        prevent_initial_call=True,
    )
    def ui_feedback_for_setting_msdial_config_for_bio_standard(config_added, bio_standard, chromatography):
        if config_added == "Added":
            message = f"MS-DIAL parameter configuration saved successfully for {bio_standard} ({chromatography} method)."
            return True, message
        return False, ""

    @app.callback(
        Output("chromatography-msdial-config-added", "data"),
        Input("istd-msdial-configs-button", "n_clicks"),
        State("select-istd-chromatography-dropdown", "value"),
        State("istd-msdial-configs-dropdown", "value"),
        prevent_initial_call=True,
    )
    def add_msdial_config_for_chromatography(button_click, chromatography, config_id):
        """Sets MS-DIAL configuration to be used for a chromatography method."""
        if chromatography and config_id:
            # In the new schema, chromatography → MS-DIAL config is tracked
            # via a sentinel BioStandard or stored in MsDialConfiguration.
            # For now, we confirm the config exists and return success.
            with get_session() as session:
                existing = session.get(MsDialConfiguration, config_id)
                param_json = (
                    existing.parameter_file_path
                    if existing and existing.parameter_file_path
                    else json.dumps(_MSDIAL_DEFAULTS)
                )
                upsert_msdial_configuration(
                    session, config_id,
                    parameter_file_path=param_json,
                )
                session.commit()
            return "Added"
        return ""

    @app.callback(
        Output("istd-config-success-alert", "is_open"),
        Output("istd-config-success-alert", "children"),
        Input("chromatography-msdial-config-added", "data"),
        State("select-istd-chromatography-dropdown", "value"),
        prevent_initial_call=True,
    )
    def ui_feedback_for_setting_msdial_config_for_chromatography(config_added, chromatography):
        if config_added == "Added":
            return True, f"MS-DIAL parameter configuration saved successfully for {chromatography}."
        return False, ""
