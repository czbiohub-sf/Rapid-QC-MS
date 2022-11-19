import io, sys, subprocess, time
import base64, webbrowser, json
import pandas as pd
import sqlalchemy as sa
from dash import dash, dcc, html, dash_table, Input, Output, State, ctx
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from QCPlotGeneration import *
from AcquisitionListener import *
import DatabaseFunctions as db
import AutoQCProcessing as qc

local_stylesheet = {
    "href": "https://fonts.googleapis.com/css2?"
            "family=Lato:wght@400;700&display=swap",
    "rel": "stylesheet"
}

# Google Drive authentication
current_directory = os.getcwd()
drive_settings_file = current_directory + "/assets/settings.yaml"
gauth_holder = [GoogleAuth(settings_file=drive_settings_file)]
# GoogleAuth.DEFAULT_SETTINGS["client_config_file"] = current_directory + "/assets/client_secrets.json"
credentials_file = current_directory + "/assets/credentials.txt"

"""
Dash app layout
"""

# Initialize Dash app
app = dash.Dash(__name__, title="MS-AutoQC", suppress_callback_exceptions=True,
                external_stylesheets=[local_stylesheet, dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP],
                meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}])

def serve_layout():

    biohub_logo = "https://user-images.githubusercontent.com/7220175/184942387-0acf5deb-d81e-4962-ab27-05b453c7a688.png"

    return html.Div(className="app-layout", children=[

        # Navigation bar
        dbc.Navbar(
            dbc.Container(style={"height": "50px"}, children=[
                # Logo and title
                html.A(
                    dbc.Row([
                        dbc.Col(html.Img(src=biohub_logo, height="30px")),
                        dbc.Col(dbc.NavbarBrand(id="header", children="MS-AutoQC", className="ms-2")),
                        ], align="center", className="g-0",
                    ), href="https://biohub.org", style={"textDecoration": "none"},
                ),
                # Settings button
                dbc.Row([
                    dbc.Nav([
                        dbc.NavItem(dbc.NavLink("About", href="https://github.com/czbiohub/MS-AutoQC", className="navbar-button")),
                        dbc.NavItem(dbc.NavLink("Help", href="https://github.com/czbiohub/MS-AutoQC/wiki", className="navbar-button")),
                        dbc.NavItem(dbc.NavLink("Settings", href="#", id="settings-button", className="navbar-button")),
                    ], className="me-auto")
                ], className="g-0 ms-auto flex-nowrap mt-3 mt-md-0")
            ]), color="dark", dark=True
        ),

        # App layout
        html.Div(className="page", children=[

            dbc.Row(justify="center", children=[

                dbc.Col(width=11, children=[

                    dbc.Row(justify="center", children=[

                        # Tabs to switch between instruments
                        dcc.Tabs(id="tabs", className="instrument-tabs"),

                        dbc.Col(width=12, lg=4, children=[

                            html.Div(id="table-container", className="table-container", style={"display": "none"}, children=[

                                # Table of past/active instrument runs
                                dash_table.DataTable(id="instrument-run-table", page_action="none",
                                    fixed_rows={"headers": True},
                                    cell_selectable=True,
                                    style_cell={
                                        "textAlign": "left",
                                        "fontSize": "15px",
                                        "fontFamily": "sans-serif",
                                        "lineHeight": "25px",
                                        "padding": "10px",
                                        "borderRadius": "5px"},
                                    style_data={"whiteSpace": "normal",
                                        "textOverflow": "ellipsis",
                                        "maxWidth": 0},
                                    style_table={
                                        "max-height": "285px",
                                           "overflowY": "auto"},
                                    style_data_conditional=[
                                        {"if": {"state": "active"},
                                        "backgroundColor": bootstrap_colors[
                                        "blue-low-opacity"],
                                        "border": "1px solid " + bootstrap_colors["blue"]
                                        }],
                                    style_cell_conditional=[
                                        {"if": {"column_id": "Run ID"},
                                        "width": "40%"},
                                        {"if": {"column_id": "Chromatography"},
                                            "width": "35%"},
                                        {"if": {"column_id": "Status"},
                                        "width": "25%"}
                                    ]
                                ),

                                # Button to start monitoring a new run
                                html.Div(className="d-grid gap-2", children=[
                                    dbc.Button("New MS-AutoQC Job",
                                               id="setup-new-run-button",
                                               style={"margin-top": "15px",
                                                      "line-height": "1.75"},
                                               outline=True,
                                               color="primary"),
                                ]),

                                # Polarity filtering options
                                html.Div(className="radio-group-container", children=[
                                    html.Div(className="radio-group margin-top-30", children=[
                                        dbc.RadioItems(
                                            id="polarity-options",
                                            className="btn-group",
                                            inputClassName="btn-check",
                                            labelClassName="btn btn-outline-primary",
                                            inputCheckedClassName="active",
                                            options=[
                                                {"label": "Positive Mode", "value": "Pos"},
                                                {"label": "Negative Mode", "value": "Neg"}],
                                            value="Pos"
                                        ),
                                    ])
                                ]),

                                # Sample / blank / pool / treatment filtering options
                                html.Div(className="radio-group-container", children=[
                                    html.Div(className="radio-group margin-top-30", children=[
                                        dbc.RadioItems(
                                            id="sample-filtering-options",
                                            className="btn-group",
                                            inputClassName="btn-check",
                                            labelClassName="btn btn-outline-primary",
                                            inputCheckedClassName="active",
                                            value="all",
                                            options=[
                                                {"label": "All", "value": "all"},
                                                {"label": "Samples", "value": "samples"},
                                                {"label": "Pools", "value": "pools"},
                                                {"label": "Blanks", "value": "blanks"}],
                                        ),
                                    ])
                                ]),

                                # Table of samples run for a particular study
                                dash_table.DataTable(id="sample-table", page_action="none",
                                    fixed_rows={"headers": True},
                                    # cell_selectable=True,
                                    style_cell={
                                        "textAlign": "left",
                                        "fontSize": "15px",
                                        "fontFamily": "sans-serif",
                                        "lineHeight": "25px",
                                        "whiteSpace": "normal",
                                        "padding": "10px",
                                        "borderRadius": "5px"},
                                    style_data={
                                        "whiteSpace": "normal",
                                        "textOverflow": "ellipsis",
                                        "maxWidth": 0},
                                    style_table={
                                        "height": "475px",
                                        "overflowY": "auto"},
                                    style_data_conditional=[
                                        {"if": {"filter_query": "{QC} = 'Fail'"},
                                        "backgroundColor": bootstrap_colors[
                                        "red-low-opacity"],
                                        "font-weight": "bold"
                                        },
                                        {"if": {"filter_query": "{QC} = 'Check'"},
                                        "backgroundColor": bootstrap_colors[
                                        "yellow-low-opacity"]
                                        },
                                        {"if": {"state": "active"},
                                        "backgroundColor": bootstrap_colors[
                                        "blue-low-opacity"],
                                        "border": "1px solid " + bootstrap_colors["blue"]
                                        }
                                    ],
                                    style_cell_conditional=[
                                        {"if": {"column_id": "Sample"},
                                        "width": "60%"},
                                        {"if": {"column_id": "Position"},
                                        "width": "20%"},
                                        {"if": {"column_id": "QC"},
                                        "width": "20%"},
                                    ]
                                )
                            ]),
                        ]),

                        dbc.Col(width=12, lg=8, children=[

                            # Container for all plots
                            html.Div(id="plot-container", className="all-plots-container", style={"display": "none"}, children=[

                                html.Div(className="istd-plot-div", children=[

                                    html.Div(className="plot-container", children=[

                                        # Internal standard selection controls
                                        html.Div(style={"width": "100%"}, children=[
                                            # Dropdown for selecting an internal standard for the RT vs. sample plot
                                            html.Div(className="istd-dropdown-style", children=[
                                                dcc.Dropdown(
                                                    id="istd-rt-dropdown",
                                                    options=[],
                                                    placeholder="Select internal standards...",
                                                    style={"text-align": "left",
                                                           "height": "1.5",
                                                           "width": "100%"}
                                                )]
                                            ),

                                            # Buttons for skipping through the internal standards
                                            html.Div(className="istd-button-style", children=[
                                                dbc.Button(html.I(className="bi bi-arrow-left"),
                                                    id="rt-prev-button", color="light", className="me-1"),
                                                dbc.Button(html.I(className="bi bi-arrow-right"),
                                                    id="rt-next-button", color="light", className="me-1"),
                                            ]),
                                        ]),

                                        # Dropdown for filtering by sample for the RT vs. sample plot
                                        dcc.Dropdown(
                                            id="rt-plot-sample-dropdown",
                                            options=[],
                                            placeholder="Select samples...",
                                            style={"text-align": "left",
                                                   "height": "1.5",
                                                   "width": "100%",
                                                   "display": "inline-block"},
                                            multi=True),

                                        # Scatter plot of internal standard retention times vs. samples
                                        dcc.Graph(id="istd-rt-plot"),
                                    ]),

                                    html.Div(className="plot-container", children=[

                                        # Internal standard selection controls
                                        html.Div(style={"width": "100%"}, children=[
                                            # Dropdown for selecting an internal standard for the intensity vs. sample plot
                                            html.Div(className="istd-dropdown-style", children=[
                                                dcc.Dropdown(
                                                    id="istd-intensity-dropdown",
                                                    options=[],
                                                    placeholder="Select internal standards...",
                                                    style={"text-align": "left",
                                                           "height": "1.5",
                                                           "width": "100%"}
                                                )]
                                            ),

                                            # Buttons for skipping through the internal standards
                                            html.Div(className="istd-button-style", children=[
                                                dbc.Button(html.I(className="bi bi-arrow-left"),
                                                    id="intensity-prev-button", color="light", className="me-1"),
                                                dbc.Button(html.I(className="bi bi-arrow-right"),
                                                    id="intensity-next-button", color="light", className="me-1"),
                                            ]),
                                        ]),

                                        # Dropdown for filtering by sample for the intensity vs. sample plot
                                        dcc.Dropdown(
                                            id="intensity-plot-sample-dropdown",
                                            options=[],
                                            placeholder="Select samples...",
                                            style={"text-align": "left",
                                                   "height": "1.5",
                                                   "width": "100%",
                                                   "display": "inline-block"},
                                            multi=True,
                                        ),

                                        # Bar plot of internal standard intensity vs. samples
                                        dcc.Graph(id="istd-intensity-plot")
                                    ]),

                                    html.Div(className="plot-container", children=[

                                        # Internal standard selection controls
                                        html.Div(style={"width": "100%"}, children=[
                                            # Dropdown for selecting an internal standard for the delta m/z vs. sample plot
                                            html.Div(className="istd-dropdown-style", children=[
                                                dcc.Dropdown(
                                                    id="istd-mz-dropdown",
                                                    options=[],
                                                    placeholder="Select internal standards...",
                                                    style={"text-align": "left",
                                                           "height": "1.5",
                                                           "width": "100%"}
                                                )]
                                            ),

                                            # Buttons for skipping through the internal standards
                                            html.Div(className="istd-button-style", children=[
                                                dbc.Button(html.I(className="bi bi-arrow-left"),
                                                    id="mz-prev-button", color="light", className="me-1"),
                                                dbc.Button(html.I(className="bi bi-arrow-right"),
                                                    id="mz-next-button", color="light", className="me-1"),
                                            ]),
                                        ]),

                                        # Dropdown for filtering by sample for the delta m/z vs. sample plot
                                        dcc.Dropdown(
                                            id="mz-plot-sample-dropdown",
                                            options=[],
                                            placeholder="Select samples...",
                                            style={"text-align": "left",
                                                   "height": "1.5",
                                                   "width": "100%",
                                                   "display": "inline-block"},
                                            multi=True),

                                        # Scatter plot of internal standard delta m/z vs. samples
                                        dcc.Graph(id="istd-mz-plot")
                                    ]),

                                ]),

                                html.Div(className="bio-plot-div", children=[

                                    # Scatter plot for biological standard m/z vs. RT
                                    html.Div(className="plot-container", children=[
                                        dcc.Graph(id="bio-standard-mz-rt-plot")
                                    ]),

                                    # Bar plot for biological standard feature intensity vs. run
                                    html.Div(className="plot-container", children=[

                                        # Dropdown for biological standard feature intensity plot
                                        dcc.Dropdown(
                                            id="bio-standard-benchmark-dropdown",
                                            options=[],
                                            placeholder="Select targeted metabolite...",
                                            style={"text-align": "left",
                                                   "height": "35px",
                                                   "width": "100%",
                                                   "display": "inline-block"}
                                        ),

                                        dcc.Graph(id="bio-standard-benchmark-plot", animate=False)
                                    ])
                                ])
                            ]),
                        ]),

                        # Modal for sample information card
                        dbc.Modal(id="sample-info-modal", size="xl", centered=True, is_open=False, scrollable=True, children=[
                            dbc.ModalHeader(dbc.ModalTitle(id="sample-modal-title"), close_button=True),
                            dbc.ModalBody(id="sample-modal-body")
                        ]),

                        # Modal for alerting user that data is loading
                        dbc.Modal(id="loading-modal", size="md", centered=True, is_open=False, scrollable=True,
                                  keyboard=False, backdrop="static", children=[
                            dbc.ModalHeader(dbc.ModalTitle(id="loading-modal-title"), close_button=False),
                            dbc.ModalBody(id="loading-modal-body")
                        ]),

                        # Modal for progress feedback while database syncs to Google Drive
                        dbc.Modal(id="google-drive-sync-modal", size="md", centered=True, is_open=False, scrollable=True,
                            keyboard=True, backdrop="static", children=[
                                dbc.ModalHeader(dbc.ModalTitle(
                                    html.Div(children=[
                                        dbc.Spinner(color="primary"), " Syncing to Google Drive"])),
                                    close_button=False),
                                dbc.ModalBody("This may take a few seconds...")
                        ]),

                        # Custom file explorer modal for new job setup
                        dbc.Modal(id="file-explorer-modal", size="md", centered=True, is_open=False, scrollable=True,
                            keyboard=True, children=[
                                dbc.ModalHeader(dbc.ModalTitle(id="file-explorer-modal-title")),
                                dbc.ModalBody(id="file-explorer-modal-body"),
                                dbc.ModalFooter(children=[
                                    dbc.Button("Go Back", id="file-explorer-back-button", color="secondary"),
                                    dbc.Button("Select Current Folder", id="file-explorer-select-button")
                                ])
                        ]),

                        # Modal for first-time workspace setup
                        dbc.Modal(id="workspace-setup-modal", size="lg", centered=True, scrollable=True,
                                  keyboard=False, backdrop="static", children=[
                            dbc.ModalHeader(dbc.ModalTitle("Welcome to MS-AutoQC", id="setup-user-modal-title"), close_button=False),
                            dbc.ModalBody(id="setup-user-modal-body", className="modal-styles-2", children=[

                                html.Div([
                                    html.H5("Let's help you get started."),
                                    html.P("Looks like this is a new installation. What would you like to do today?"),
                                    dbc.Accordion(start_collapsed=True, children=[

                                        # Setting up MS-AutoQC for the first time
                                        dbc.AccordionItem(title="I'm setting up MS-AutoQC on a new instrument", children=[
                                            html.Div(className="modal-styles-3", children=[

                                                # Instrument name text field
                                                html.Div([
                                                    dbc.Label("Instrument name"),
                                                    dbc.InputGroup([
                                                        dbc.Input(id="first-time-instrument-id", type="text",
                                                                  placeholder="Ex: Thermo Q-Exactive HF 1"),
                                                        dbc.DropdownMenu(id="first-time-instrument-vendor",
                                                            label="Choose Vendor", color="primary", children=[
                                                                dbc.DropdownMenuItem("Thermo Fisher", id="thermo-fisher-item"),
                                                                dbc.DropdownMenuItem("Agilent", id="agilent-item"),
                                                                dbc.DropdownMenuItem("Bruker", id="bruker-item"),
                                                                dbc.DropdownMenuItem("Sciex", id="sciex-item"),
                                                                dbc.DropdownMenuItem("Waters", id="waters-item")
                                                        ]),
                                                    ]),
                                                    dbc.FormText("Please choose a name and vendor for this instrument."),
                                                ]),

                                                html.Br(),

                                                # Google Drive authentication button
                                                html.Div([
                                                    dbc.Label("Sync with Google Drive (recommended)"),
                                                    html.Br(),
                                                    dbc.Button("Sign in to Google Drive", id="setup-google-drive-button-1",
                                                       color="primary", outline=True),
                                                    html.Br(),
                                                    dbc.FormText("This will allow you to access your QC results from any device."),
                                                    dbc.Tooltip("If you have Google Drive sync enabled on an instrument already, " +
                                                        "please sign in with the same Google account to merge workspaces.",
                                                        target="setup-google-drive-button-1", placement="left"),
                                                    dbc.Popover(id="google-drive-button-1-popover", is_open=False,
                                                        target="setup-google-drive-button-1", placement="right")
                                                ]),

                                                html.Br(),

                                                # Complete setup button
                                                html.Div([
                                                    html.Div([
                                                        dbc.Button(children="Complete setup", id="first-time-complete-setup-button",
                                                            disabled=True, style={"line-height": "1.75"}, color="success"),
                                                    ], className="d-grid gap-2 col-12 mx-auto"),
                                                ])
                                            ]),
                                        ]),

                                        # Signing in from another device
                                        dbc.AccordionItem(title="I'm signing in to an existing MS-AutoQC workspace", children=[
                                            html.Div(className="modal-styles-3", children=[

                                                # Google Drive authentication button
                                                html.Div([
                                                    dbc.Label("Sign in to access MS-AutoQC"),
                                                    html.Br(),
                                                    dbc.Button("Sign in to Google Drive", id="setup-google-drive-button-2",
                                                        color="primary", outline=False),
                                                    html.Br(),
                                                    dbc.FormText(
                                                        "Please ensure that your Google account has been registered to " +
                                                        "access your MS-AutoQC workspace by visiting Settings > General."),
                                                    dbc.Popover(id="google-drive-button-2-popover", is_open=False,
                                                                target="setup-google-drive-button-2", placement="right")
                                                ]),

                                                html.Br(),

                                                # Workspace sign-in button
                                                html.Div([
                                                    html.Div([
                                                        dbc.Button("Sign in to MS-AutoQC workspace", id="first-time-sign-in-button",
                                                            disabled=True, style={"line-height": "1.75"}, color="success"),
                                                    ], className="d-grid gap-2 col-12 mx-auto"),
                                                ])
                                            ]),
                                        ]),
                                    ]),
                                ]),
                            ])
                        ]),

                        # Modal for starting an instrument run listener
                        dbc.Modal(id="setup-new-run-modal", size="lg", centered=True, is_open=False, scrollable=True, children=[
                            dbc.ModalHeader(dbc.ModalTitle(id="setup-new-run-modal-title", children="New MS-AutoQC Job"), close_button=True),
                            dbc.ModalBody(id="setup-new-run-modal-body", className="modal-styles-2", children=[

                                # Text field for entering your run ID
                                html.Div([
                                    dbc.Label("Instrument run ID"),
                                    dbc.Input(id="instrument-run-id", placeholder="Give your instrument run a unique name", type="text"),
                                    dbc.FormFeedback("Looks good!", type="valid"),
                                    dbc.FormFeedback("Please enter a unique ID for this run.", type="invalid"),
                                ]),

                                html.Br(),

                                # Select chromatography
                                html.Div([
                                    dbc.Label("Select chromatography"),
                                    dbc.Select(id="start-run-chromatography-dropdown",
                                               placeholder="No chromatography selected"),
                                    dbc.FormFeedback("Looks good!", type="valid"),
                                    dbc.FormFeedback(
                                        "Please ensure that your chromatography method has identification files "
                                        "(MSP or CSV) configured for positive and negative mode in Settings > "
                                        "Internal Standards.", type="invalid")
                                ]),

                                html.Br(),

                                # Select biological standard used in this study
                                html.Div(children=[
                                    dbc.Label("Select biological standards (optional)"),
                                    dcc.Dropdown(id="start-run-bio-standards-dropdown",
                                        options=[], placeholder="Select biological standards...",
                                        style={"text-align": "left", "height": "1.5", "font-size": "1rem",
                                            "width": "100%", "display": "inline-block"},
                                        multi=True),
                                    dbc.FormFeedback("Looks good!", type="valid"),
                                    dbc.FormFeedback(
                                        "Please ensure that your biological standard has MSP files configured "
                                        "for both positive and negative mode in Settings > Biological Standards.",
                                        type="invalid")
                                ]),

                                html.Br(),

                                # Select AutoQC configuration
                                html.Div(children=[
                                    dbc.Label("Select MS-AutoQC configuration"),
                                    dbc.Select(id="start-run-qc-configs-dropdown",
                                               placeholder="No configuration selected"),
                                ]),

                                html.Br(),

                                # Button and field for selecting a sequence file
                                html.Div([
                                    dbc.Label("Acquisition sequence (.csv)"),
                                    dbc.InputGroup([
                                        dbc.Input(id="sequence-path",
                                                  placeholder="No file selected"),
                                        dbc.Button(dcc.Upload(
                                            id="sequence-upload-button",
                                            children=[html.A("Browse Files")]),
                                            color="secondary"),
                                        dbc.FormFeedback("Looks good!", type="valid"),
                                        dbc.FormFeedback("Please ensure that the sequence file is a CSV file "
                                            "and in the correct vendor format.", type="invalid"),
                                    ]),
                                ]),

                                html.Br(),

                                # Button and field for selecting a sample metadata file
                                html.Div([
                                    dbc.Label("Sample metadata (.csv) (optional)"),
                                    dbc.InputGroup([
                                        dbc.Input(id="metadata-path",
                                                  placeholder="No file selected"),
                                        dbc.Button(dcc.Upload(
                                            id="metadata-upload-button",
                                            children=[html.A("Browse Files")]),
                                            color="secondary"),
                                        dbc.FormFeedback("Looks good!", type="valid"),
                                        dbc.FormFeedback("Please ensure that the metadata file is a CSV and contains "
                                            "the following columns: Sample Name, Species, Matrix, Treatment, "
                                            "and Growth-Harvest Conditions", type="invalid"),
                                    ]),
                                ]),

                                html.Br(),

                                # Button and field for selecting the data acquisition directory
                                html.Div([
                                    dbc.Label("Data file directory", id="data-acquisition-path-title"),
                                    dbc.InputGroup([
                                        dbc.Input(placeholder="Browse folders or enter the folder path",
                                                  id="data-acquisition-folder-path"),
                                        dbc.Button("Browse Folders", id="data-acquisition-folder-button",
                                                  color="secondary"),
                                        dbc.FormFeedback("Looks good!", type="valid"),
                                        dbc.FormFeedback(
                                            "This path does not exist. Please enter a valid path.", type="invalid"),
                                    ]),
                                    dbc.FormText(id="data-acquisition-path-form-text",
                                        children="Please type the folder path to which incoming data files will be saved."),

                                ]),

                                html.Br(),

                                # Switch between running AutoQC on a live run vs. past completed run
                                html.Div(children=[
                                    dbc.Label("Is this an active or completed instrument run?"),
                                    dbc.RadioItems(id="autoqc-job-type", value="active", options=[
                                        {"label": "Monitor an active instrument run",
                                         "value": "active"},
                                        {"label": "QC a completed instrument run",
                                         "value": "completed"}],
                                    ),
                                ]),

                                html.Br(),

                                html.Div([
                                    dbc.Button("Start monitoring instrument run", id="monitor-new-run-button", disabled=True,
                                    style={"line-height": "1.75"}, color="primary")],
                                className="d-grid gap-2")
                            ]),
                        ]),

                        # Modal to alert user that run monitoring has started
                        dbc.Modal(id="start-run-monitor-modal", size="md", centered=True, is_open=False, children=[
                            dbc.ModalHeader(dbc.ModalTitle(id="start-run-monitor-modal-title", children="Success!"), close_button=True),
                            dbc.ModalBody(id="start-run-monitor-modal-body", className="modal-styles", children=[
                                dbc.Alert("MS-AutoQC will start monitoring your run. Please do not restart your computer.", color="success")
                            ]),
                        ]),

                        # Progress modal for bulk QC process
                        dbc.Modal(id="start-bulk-qc-modal", size="md", centered=True, is_open=False,
                                  keyboard=False, backdrop="static", children=[
                            dbc.ModalHeader(dbc.ModalTitle(id="start-bulk-qc-modal-title", children="Processing data files..."), close_button=False),
                            dbc.ModalBody(id="start-bulk-qc-modal-body", className="modal-styles", children=[
                                html.Div([
                                    dcc.Interval(id="progress-interval", n_intervals=0, interval=10000),
                                    dbc.Label("Please do not refresh the page or close this window."),
                                    dbc.Progress(id="bulk-qc-progress-bar", striped=True, animated=True, style={"height": "30px"}),
                                ])
                            ]),
                        ]),

                        # TODO: Error modal for new AutoQC job setup
                        dbc.Modal(id="new-job-error-modal", size="md", centered=True, is_open=False, children=[
                            dbc.ModalHeader(dbc.ModalTitle(id="new-job-error-modal-title"), close_button=False),
                            dbc.ModalBody(id="new-job-error-modal-body", className="modal-styles"),
                        ]),

                        # MS-AutoQC settings
                        dbc.Modal(id="settings-modal", fullscreen=True, centered=True, is_open=False, scrollable=True, children=[
                            dbc.ModalHeader(dbc.ModalTitle(children="Settings"), close_button=True),
                            dbc.ModalBody(id="settings-modal-body", className="modal-styles-fullscreen", children=[

                                # Tabbed interface
                                dbc.Tabs(children=[

                                    # General settings
                                    dbc.Tab(label="General", className="modal-styles", children=[

                                        html.Br(),

                                        dbc.Alert(id="google-drive-sign-in-from-settings-alert", is_open=False,
                                        dismissable=True, color="danger", children=[
                                            html.H4(
                                                "This Google account already has an MS-AutoQC workspace."),
                                            html.P(
                                                "Please sign in with a different Google account to enable cloud "
                                                "sync for this workspace."),
                                            html.P(
                                                "Or, if you'd like to add a new instrument to an existing MS-AutoQC "
                                                "workspace, please reinstall MS-AutoQC on this instrument and enable "
                                                "cloud sync during setup.")
                                        ]),

                                        dbc.Label("Manage workspace access", style={"font-weight": "bold"}),
                                        html.Br(),

                                        # Google Drive cloud storage
                                        dbc.Label("Cloud sync with Google Drive"),
                                        html.Br(),
                                        dbc.Button("Sync with Google Drive",
                                            id="google-drive-sync-button", color="primary", outline=False),
                                        html.Br(),
                                        dbc.FormText(id="google-drive-sync-form-text", children=
                                            "This will allow you to monitor your instrument runs on other devices."),
                                        html.Br(), html.Br(),

                                        # Alerts for modifying workspace access
                                        dbc.Alert(id="user-addition-alert", color="success", is_open=False, duration=6000),
                                        dbc.Alert(id="user-deletion-alert", color="primary", is_open=False, duration=6000),

                                        # Google Drive sharing
                                        dbc.Label("Add / remove workspace users"),
                                        html.Br(),
                                        dbc.InputGroup([
                                            dbc.Input(placeholder="example@gmail.com",
                                                      id="add-user-text-field"),
                                            dbc.Button("Add user", color="primary", outline=True,
                                                       id="add-user-button", n_clicks=0),
                                            dbc.Button("Delete user", color="danger", outline=True,
                                                       id="delete-user-button", n_clicks=0),
                                        ]),
                                        dbc.FormText(
                                            "Adding new users grants full read-and-write access to this MS-AutoQC workspace."),
                                        html.Br(), html.Br(),

                                        # Table of users with workspace access
                                        html.Div(id="workspace-users-table"),
                                        html.Br(),

                                        dbc.Label("Notifications", style={"font-weight": "bold"}),
                                        html.Br(),

                                        # Slack notifications
                                        dbc.Label("Slack notifications"),
                                        html.Br(),
                                        dbc.Button("Sign in with Slack", color="primary",
                                                   id="slack-sync-button"),
                                        html.Br(),
                                        dbc.FormText(
                                            "This will allow you to be notified of QC fails and warnings via Slack."),
                                        html.Br(), html.Br(),

                                        # Channel for Slack notifications
                                        dbc.Label("Slack channels"),
                                        html.Br(),
                                        dbc.InputGroup([
                                            dbc.Input(placeholder="#my-slack-channel",
                                                      id="add-slack-channel-text-field"),
                                            dbc.Button("Register channel", color="primary", outline=True,
                                                       id="add-slack-channel-button", n_clicks=0),
                                        ]),
                                        dbc.FormText(
                                            "Please enter the name of the Slack channel for MS-AutoQC Bot to join."),
                                        html.Br(), html.Br(),

                                        # Email notifications
                                        dbc.Label("Email notifications"),
                                        html.Br(),
                                        dbc.InputGroup([
                                            dbc.Input(placeholder="name@example.com",
                                                      id="add-email-text-field"),
                                            dbc.Button("Register email", color="primary", outline=True,
                                                       id="add-email-button", n_clicks=0),
                                        ]),
                                        dbc.FormText(
                                            "Please enter a valid email address to register for email notifications."),
                                        html.Br(), html.Br(),

                                        # Table of users registered for email notifications
                                        html.Div(id="email-notification-users-table")
                                    ]),

                                    # Internal standards
                                    dbc.Tab(label="Internal standards", className="modal-styles", children=[

                                        html.Br(),

                                        # Alerts for user feedback on biological standard addition/removal
                                        dbc.Alert(id="chromatography-addition-alert", color="success", is_open=False, duration=6000),
                                        dbc.Alert(id="chromatography-removal-alert", color="primary", is_open=False, duration=6000),

                                        dbc.Label("Manage chromatography methods", style={"font-weight": "bold"}),
                                        html.Br(),

                                        # Add new chromatography method
                                        html.Div([
                                            dbc.Label("Add new chromatography method"),
                                            dbc.InputGroup([
                                                dbc.Input(id="add-chromatography-text-field", type="text",
                                                          placeholder="Name of chromatography to add"),
                                                dbc.Button("Add method", color="primary", outline=True,
                                                           id="add-chromatography-button", n_clicks=0),
                                            ]),
                                            dbc.FormText("Example: HILIC, Reverse Phase, RP (30 mins)"),
                                        ]), html.Br(),

                                        # Chromatography methods table
                                        dbc.Label("Chromatography methods", style={"font-weight": "bold"}),
                                        html.Br(),
                                        html.Div(id="chromatography-methods-table"),
                                        html.Br(),

                                        dbc.Label("Configure chromatography methods", style={"font-weight": "bold"}),
                                        html.Br(),

                                        # Select chromatography
                                        html.Div([
                                            dbc.Label("Select chromatography to modify"),
                                            dbc.InputGroup([
                                                dbc.Select(id="select-istd-chromatography-dropdown",
                                                    placeholder="No chromatography selected"),
                                                dbc.Button("Remove", color="danger", outline=True,
                                                    id="remove-chromatography-method-button", n_clicks=0),
                                                dbc.Popover("You are about to delete this chromatography method and "
                                                    "all of its corresponding MSP files. Are you sure?",
                                                    target="remove-chromatography-method-button", trigger="hover", body=True)
                                            ]),
                                        ]),

                                        html.Br(),

                                        # Select polarity
                                        html.Div([
                                            dbc.Label("Select polarity to modify"),
                                            dbc.Select(id="select-istd-polarity-dropdown", options=[
                                                {"label": "Positive Mode", "value": "Positive Mode"},
                                                {"label": "Negative Mode", "value": "Negative Mode"},
                                            ], placeholder="No polarity selected"),
                                        ]),

                                        html.Br(),

                                        dbc.Alert(id="istd-config-success-alert", color="success", is_open=False, duration=6000),

                                        # Set MS-DIAL configuration for selected chromatography
                                        html.Div(children=[
                                            dbc.Label("Set MS-DIAL processing configuration",
                                                      id="istd-medial-configs-label"),
                                            dbc.InputGroup([
                                                dbc.Select(id="istd-msdial-configs-dropdown",
                                                           placeholder="No configuration selected"),
                                                dbc.Button("Set configuration", color="primary", outline=True,
                                                           id="istd-msdial-configs-button", n_clicks=0),
                                            ])
                                        ]),

                                        html.Br(),

                                        # UI feedback on adding MSP to chromatography method
                                        dbc.Alert(id="chromatography-msp-success-alert", color="success", is_open=False,
                                                  duration=6000),
                                        dbc.Alert(id="chromatography-msp-error-alert", color="danger", is_open=False,
                                                  duration=6000),

                                        dbc.Label("Add internal standard identification files", style={"font-weight": "bold"}),
                                        html.Br(),

                                        html.Div([
                                            dbc.Label("Add internal standards (MSP or CSV format)"),
                                            dbc.InputGroup([
                                                dbc.Input(placeholder="No file selected",
                                                          id="add-istd-msp-text-field"),
                                                dbc.Button(dcc.Upload(
                                                    id="add-istd-msp-button",
                                                    children=[html.A("Browse Files")]),
                                                    color="secondary"),
                                            ]),
                                            dbc.FormText(
                                                "Please ensure that each internal standard has a name, m/z, RT, and MS/MS spectrum."),
                                        ]),

                                        html.Br(),

                                        html.Div([
                                            html.Div([
                                                dbc.Button("Save changes", id="msp-save-changes-button",
                                                           style={"line-height": "1.75"}, color="primary"),
                                            ], className="d-grid gap-2 col-12 mx-auto"),
                                        ]),
                                    ]),

                                    # Biological standards
                                    dbc.Tab(label="Biological standards", className="modal-styles", children=[

                                        html.Br(),

                                        # UI feedback for biological standard addition/removal
                                        dbc.Alert(id="bio-standard-addition-alert", is_open=False, duration=6000),

                                        dbc.Label("Manage biological standards", style={"font-weight": "bold"}),
                                        html.Br(),

                                        html.Div([
                                            dbc.Label("Add new biological standard"),
                                            dbc.InputGroup([
                                                dbc.Input(id="add-bio-standard-text-field",
                                                          placeholder="Name of biological standard"),
                                                dbc.Input(id="add-bio-standard-identifier-text-field",
                                                          placeholder="Sequence identifier"),
                                                dbc.Button("Add biological standard", color="primary", outline=True,
                                                           id="add-bio-standard-button", n_clicks=0),
                                            ]),
                                            dbc.FormText(
                                                "The sequence identifier is the label that denotes your biological standard in the sequence."),
                                        ]),

                                        html.Br(),

                                        # Table of biological standards
                                        dbc.Label("Biological standards", style={"font-weight": "bold"}),
                                        html.Br(),

                                        html.Div(id="biological-standards-table"),
                                        html.Br(),

                                        dbc.Alert(id="bio-standard-removal-alert", color="primary", is_open=False, duration=6000),

                                        dbc.Label("Configure biological standards and add MSP files",
                                                  style={"font-weight": "bold"}),
                                        html.Br(),

                                        # Select biological standard
                                        html.Div([
                                            dbc.Label("Select biological standard to modify"),
                                            dbc.InputGroup([
                                                dbc.Select(id="select-bio-standard-dropdown",
                                                           placeholder="No biological standard selected"),
                                                dbc.Button("Remove", color="danger", outline=True,
                                                           id="remove-bio-standard-button", n_clicks=0),
                                                dbc.Popover("You are about to delete this biological standard and "
                                                            "all of its corresponding MSP files. Are you sure?",
                                                            target="remove-bio-standard-button", trigger="hover",
                                                            body=True)
                                            ]),
                                        ]),

                                        html.Br(),

                                        html.Div([
                                            dbc.Label("Select chromatography and polarity to modify"),
                                            html.Div(className="parent-container", children=[
                                                # Select chromatography
                                                html.Div(className="child-container", children=[
                                                    dbc.Select(id="select-bio-chromatography-dropdown",
                                                               placeholder="No chromatography selected"),
                                                ]),

                                                # Select polarity
                                                html.Div(className="child-container", children=[
                                                    dbc.Select(id="select-bio-polarity-dropdown", options=[
                                                        {"label": "Positive Mode", "value": "Positive Mode"},
                                                        {"label": "Negative Mode", "value": "Negative Mode"},
                                                    ], placeholder="No polarity selected"),
                                                    html.Br(),
                                                ]),
                                            ]),
                                        ]),

                                        html.Br(), html.Br(),

                                        dbc.Alert(id="bio-config-success-alert", color="success", is_open=False, duration=6000),

                                        # Set MS-DIAL configuration for selected biological standard
                                        html.Div(children=[
                                            dbc.Label("Set MS-DIAL processing configuration",
                                                      id="bio-standard-msdial-configs-label"),
                                            dbc.InputGroup([
                                                dbc.Select(id="bio-standard-msdial-configs-dropdown",
                                                           placeholder="No configuration selected"),
                                                dbc.Button("Set configuration", color="primary", outline=True,
                                                           id="bio-standard-msdial-configs-button", n_clicks=0),
                                            ])
                                        ]),

                                        html.Br(),

                                        # UI feedback on adding MSP to biological standard
                                        dbc.Alert(id="bio-msp-success-alert", color="success", is_open=False,
                                                  duration=6000),
                                        dbc.Alert(id="bio-msp-error-alert", color="danger", is_open=False,
                                                  duration=6000),

                                        html.Div([
                                            dbc.Label("Edit targeted metabolites list (MSP format)"),
                                            html.Br(),
                                            dbc.InputGroup([
                                                dbc.Input(placeholder="No MSP file selected",
                                                          id="add-bio-msp-text-field"),
                                                dbc.Button(dcc.Upload(
                                                    id="add-bio-msp-button",
                                                    children=[html.A("Browse Files")]),
                                                    color="secondary"),
                                            ]),
                                            dbc.FormText(
                                                "Please ensure that each feature has a name, m/z, RT, and MS/MS spectrum."),
                                        ]),

                                        html.Br(),

                                        html.Div([
                                            html.Div([
                                                dbc.Button("Save changes", id="bio-standard-save-changes-button",
                                                           style={"line-height": "1.75"}, color="primary"),
                                            ], className="d-grid gap-2 col-12 mx-auto"),
                                        ]),
                                    ]),

                                    # AutoQC parameters
                                    dbc.Tab(label="QC configurations", className="modal-styles", children=[

                                        html.Br(),

                                        # UI feedback on adding / removing QC configurations
                                        dbc.Alert(id="qc-config-addition-alert", is_open=False, duration=6000),
                                        dbc.Alert(id="qc-config-removal-alert", is_open=False, duration=6000),

                                        dbc.Label("Manage QC configurations", style={"font-weight": "bold"}),
                                        html.Br(),

                                        html.Div([
                                            dbc.Label("Add new QC configuration"),
                                            dbc.InputGroup([
                                                dbc.Input(id="add-qc-configuration-text-field",
                                                          placeholder="Name of configuration to add"),
                                                dbc.Button("Add new config", color="primary", outline=True,
                                                           id="add-qc-configuration-button", n_clicks=0),
                                            ]),
                                            dbc.FormText("Give your custom QC configuration a unique name"),
                                        ]),

                                        html.Br(),

                                        # Select configuration
                                        html.Div(children=[
                                            dbc.Label("Select QC configuration to edit"),
                                            dbc.InputGroup([
                                                dbc.Select(id="qc-configs-dropdown",
                                                           placeholder="No configuration selected"),
                                                dbc.Button("Remove", color="danger", outline=True,
                                                           id="remove-qc-config-button", n_clicks=0),
                                                dbc.Popover("You are about to delete this QC configuration. Are you sure?",
                                                            target="remove-qc-config-button", trigger="hover", body=True)
                                            ])
                                        ]),

                                        html.Br(),

                                        dbc.Label("Edit QC configuration parameters", style={"font-weight": "bold"}),
                                        html.Br(),

                                        html.Div([
                                            dbc.Label("Cutoff for intensity dropouts"),
                                            dbc.InputGroup(children=[
                                                dbc.Input(
                                                    id="intensity-dropouts-cutoff", type="number", placeholder="4"),
                                                dbc.InputGroupText(
                                                    dbc.Switch(id="intensity-cutoff-enabled", label="Enabled")),
                                            ]),
                                            dbc.FormText("The minimum number of missing internal " +
                                                         "standards in a sample to trigger a QC fail."),
                                        ]),

                                        html.Br(),

                                        html.Div([
                                            dbc.Label("Cutoff for RT shift from library value"),
                                            dbc.InputGroup(children=[
                                                dbc.Input(id="library-rt-shift-cutoff", type="number", placeholder="0.1"),
                                                dbc.InputGroupText(
                                                    dbc.Switch(id="library-rt-shift-cutoff-enabled", label="Enabled")),
                                            ]),
                                            dbc.FormText(
                                                "The minimum shift in retention time (in minutes) from " +
                                                "the library value to trigger a QC fail."),
                                        ]),

                                        html.Br(),

                                        html.Div([
                                            dbc.Label("Cutoff for RT shift from in-run average"),
                                            dbc.InputGroup(children=[
                                                dbc.Input(id="in-run-rt-shift-cutoff", type="number", placeholder="0.05"),
                                                dbc.InputGroupText(
                                                    dbc.Switch(id="in-run-rt-shift-cutoff-enabled", label="Enabled")),
                                            ]),
                                            dbc.FormText(
                                                "The minimum shift in retention time (in minutes) from " +
                                                "the in-run average to trigger a QC fail."),
                                        ]),

                                        html.Br(),

                                        html.Div([
                                            dbc.Label("Cutoff for m/z shift from library value"),
                                            dbc.InputGroup(children=[
                                                dbc.Input(id="library-mz-shift-cutoff", type="number", placeholder="0.005"),
                                                dbc.InputGroupText(
                                                    dbc.Switch(id="library-mz-shift-cutoff-enabled", label="Enabled")),
                                            ]),
                                            dbc.FormText(
                                                "The minimum shift in precursor m/z (in minutes) from " +
                                                "the library value to trigger a QC fail."),
                                        ]),

                                        html.Br(),

                                        # UI feedback on saving changes to MS-DIAL parameters
                                        dbc.Alert(id="qc-parameters-success-alert",
                                                  color="success", is_open=False, duration=6000),
                                        dbc.Alert(id="qc-parameters-reset-alert",
                                                  color="primary", is_open=False, duration=6000),
                                        dbc.Alert(id="qc-parameters-error-alert",
                                                  color="danger", is_open=False, duration=6000),

                                        html.Div([
                                            html.Div([
                                                dbc.Button("Save changes", id="save-changes-qc-parameters-button",
                                                           style={"line-height": "1.75"}, color="primary"),
                                                dbc.Button("Reset default settings", id="reset-default-qc-parameters-button",
                                                           style={"line-height": "1.75"}, color="secondary"),
                                            ], className="d-grid gap-2 col-12 mx-auto"),
                                        ]),
                                    ]),

                                    # MS-DIAL parameters
                                    dbc.Tab(label="MS-DIAL configurations", className="modal-styles", children=[

                                        html.Br(),

                                        # UI feedback on configuration addition/removal
                                        dbc.Alert(id="msdial-config-addition-alert", is_open=False, duration=6000),
                                        dbc.Alert(id="msdial-config-removal-alert", is_open=False, duration=6000),

                                        dbc.Label("MS-DIAL installation", style={"font-weight": "bold"}),
                                        html.Br(),

                                        # Button and field for selecting the data acquisition directory
                                        html.Div([
                                            dbc.Label("MS-DIAL download location"),
                                            dbc.InputGroup([
                                                dbc.Input(placeholder="C:/Users/Me/Downloads/MS-DIAL",
                                                    id="msdial-directory"),
                                                dbc.Button("Browse Folders", id="msdial-folder-button",
                                                    color="secondary"),
                                                dbc.Button("Save changes", id="msdial-folder-save-button",
                                                    color="primary", outline=True)
                                            ]),
                                            dbc.FormText(
                                                "Browse for (or type) the path of your downloaded MS-DIAL folder."),
                                        ]),

                                        html.Br(),

                                        dbc.Label("Manage configurations", style={"font-weight": "bold"}),
                                        html.Br(),

                                        html.Div([
                                            dbc.Label("Add new MS-DIAL configuration"),
                                            dbc.InputGroup([
                                                dbc.Input(id="add-msdial-configuration-text-field",
                                                          placeholder="Name of configuration to add"),
                                                dbc.Button("Add new config", color="primary", outline=True,
                                                           id="add-msdial-configuration-button", n_clicks=0),
                                            ]),
                                            dbc.FormText("Give your custom configuration a unique name"),
                                        ]), html.Br(),

                                        # Select configuration
                                        html.Div(children=[
                                            dbc.Label("Select configuration to edit"),
                                            dbc.InputGroup([
                                                dbc.Select(id="msdial-configs-dropdown",
                                                           placeholder="No configuration selected"),
                                                dbc.Button("Remove", color="danger", outline=True,
                                                           id="remove-config-button", n_clicks=0),
                                                dbc.Popover("You are about to delete this configuration. Are you sure?",
                                                            target="remove-config-button", trigger="hover", body=True)
                                            ])
                                        ]), html.Br(),

                                        # Data collection parameters
                                        dbc.Label("Data collection parameters", style={"font-weight": "bold"}),
                                        html.Br(),

                                        html.Div(className="parent-container", children=[
                                            # Retention time begin
                                            html.Div(className="child-container", children=[
                                                dbc.Label("Retention time begin"),
                                                dbc.Input(id="retention-time-begin", placeholder="0"),
                                            ]),
                                            # Retention time end
                                            html.Div(className="child-container", children=[
                                                dbc.Label("Retention time end"),
                                                dbc.Input(id="retention-time-end", placeholder="100"),
                                                html.Br(),
                                            ]),
                                        ]),

                                        html.Div(className="parent-container", children=[
                                            # Mass range begin
                                            html.Div(className="child-container", children=[
                                                dbc.Label("Mass range begin"),
                                                dbc.Input(id="mass-range-begin", placeholder="0"),
                                            ]),
                                            # Mass range end
                                            html.Div(className="child-container", children=[
                                                dbc.Label("Mass range end"),
                                                dbc.Input(id="mass-range-end", placeholder="2000"),
                                                html.Br(),
                                            ]),
                                        ]),

                                        # Centroid parameters
                                        dbc.Label("Centroid parameters", style={"font-weight": "bold"}),
                                        html.Br(),

                                        html.Div(className="parent-container", children=[
                                            # MS1 centroid tolerance
                                            html.Div(className="child-container", children=[
                                                dbc.Label("MS1 centroid tolerance"),
                                                dbc.Input(id="ms1-centroid-tolerance", placeholder="0.008"),
                                            ]),
                                            # MS2 centroid tolerance
                                            html.Div(className="child-container", children=[
                                                dbc.Label("MS2 centroid tolerance"),
                                                dbc.Input(id="ms2-centroid-tolerance", placeholder="0.01"),
                                                html.Br(),
                                            ]),
                                        ]),

                                        # Peak detection parameters
                                        dbc.Label("Peak detection parameters", style={"font-weight": "bold"}),
                                        html.Br(),

                                        dbc.Label("Smoothing method"),
                                        dbc.Select(id="select-smoothing-dropdown", options=[
                                            {"label": "Simple moving average",
                                             "value": "SimpleMovingAverage"},
                                            {"label": "Linear weighted moving average",
                                             "value": "LinearWeightedMovingAverage"},
                                            {"label": "Savitzky-Golay filter",
                                             "value": "SavitzkyGolayFilter"},
                                            {"label": "Binomial filter",
                                             "value": "BinomialFilter"},
                                        ], placeholder="Linear weighted moving average"),
                                        html.Br(),

                                        html.Div(className="parent-container", children=[
                                            # Smoothing level
                                            html.Div(className="child-container", children=[
                                                dbc.Label("Smoothing level"),
                                                dbc.Input(id="smoothing-level", placeholder="3"),
                                            ]),
                                            # Mass slice width
                                            html.Div(className="child-container", children=[
                                                dbc.Label("Mass slice width"),
                                                dbc.Input(id="mass-slice-width", placeholder="0.1"),
                                                html.Br(),
                                            ]),
                                        ]),
                                        html.Br(),

                                        html.Div(className="parent-container", children=[
                                            # Minimum peak width
                                            html.Div(className="child-container", children=[
                                                dbc.Label("Minimum peak width"),
                                                dbc.Input(id="min-peak-width", placeholder="4"),
                                            ]),
                                            # Minimum peak height
                                            html.Div(className="child-container", children=[
                                                dbc.Label("Minimum peak height"),
                                                dbc.Input(id="min-peak-height", placeholder="50000"),
                                                html.Br(),
                                            ]),
                                        ]),
                                        html.Br(),

                                        # Identification parameters
                                        dbc.Label("Identification parameters", style={"font-weight": "bold"}),
                                        html.Br(),

                                        html.Div(className="parent-container", children=[
                                            # Retention time tolerance
                                            html.Div(className="child-container", children=[
                                                dbc.Label("Post-identification retention time tolerance"),
                                                dbc.Input(id="post-id-rt-tolerance", placeholder="0.3"),
                                            ]),
                                            # Accurate mass tolerance
                                            html.Div(className="child-container", children=[
                                                dbc.Label("Post-identification accurate MS1 tolerance"),
                                                dbc.Input(id="post-id-mz-tolerance", placeholder="0.008"),
                                                html.Br(),
                                            ]),
                                        ]),
                                        html.Br(),

                                        html.Div([
                                            dbc.Label("Identification score cutoff"),
                                            dbc.Input(id="post-id-score-cutoff", placeholder="85"),
                                        ]),
                                        html.Br(),

                                        # Alignment parameters
                                        dbc.Label("Alignment parameters", style={"font-weight": "bold"}),
                                        html.Br(),

                                        html.Div(className="parent-container", children=[
                                            # Retention time tolerance
                                            html.Div(className="child-container", children=[
                                                dbc.Label("Alignment retention time tolerance"),
                                                dbc.Input(id="alignment-rt-tolerance", placeholder="0.05"),
                                            ]),
                                            # Accurate mass tolerance
                                            html.Div(className="child-container", children=[
                                                dbc.Label("Alignment MS1 tolerance"),
                                                dbc.Input(id="alignment-mz-tolerance", placeholder="0.008"),
                                                html.Br(),
                                            ]),
                                        ]),
                                        html.Br(),

                                        html.Div(className="parent-container", children=[
                                            # Retention time factor
                                            html.Div(className="child-container", children=[
                                                dbc.Label("Alignment retention time factor"),
                                                dbc.Input(id="alignment-rt-factor", placeholder="0.5"),
                                            ]),
                                            # Accurate mass factor
                                            html.Div(className="child-container", children=[
                                                dbc.Label("Alignment MS1 factor"),
                                                dbc.Input(id="alignment-mz-factor", placeholder="0.5"),
                                                html.Br(),
                                            ]),
                                        ]),
                                        html.Br(),

                                        html.Div(className="parent-container", children=[
                                            # Peak count filter
                                            html.Div(className="child-container", children=[
                                                dbc.Label("Peak count filter"),
                                                dbc.Input(id="peak-count-filter", placeholder="0"),
                                            ]),
                                            # QC at least filter
                                            html.Div(className="child-container", children=[
                                                dbc.Label("QC at least filter"),
                                                dbc.Select(id="qc-at-least-filter-dropdown", options=[
                                                    {"label": "True", "value": "True"},
                                                    {"label": "False", "value": "False"},
                                                ], placeholder="True"),
                                                html.Br(),
                                            ]),
                                        ]),

                                        html.Br(), html.Br(), html.Br(), html.Br(), html.Br(),
                                        html.Br(), html.Br(), html.Br(), html.Br(), html.Br(),

                                        html.Div([
                                            # UI feedback on saving changes to MS-DIAL parameters
                                            dbc.Alert(id="msdial-parameters-success-alert",
                                                      color="success", is_open=False, duration=6000),
                                            dbc.Alert(id="msdial-parameters-reset-alert",
                                                      color="primary", is_open=False, duration=6000),
                                            dbc.Alert(id="msdial-parameters-error-alert",
                                                      color="danger", is_open=False, duration=6000),
                                        ]),

                                        html.Div([
                                            html.Div([
                                                dbc.Button("Save changes", id="save-changes-msdial-parameters-button",
                                                           style={"line-height": "1.75"}, color="primary"),
                                                dbc.Button("Reset default settings", id="reset-default-msdial-parameters-button",
                                                           style={"line-height": "1.75"}, color="secondary"),
                                            ], className="d-grid gap-2 col-12 mx-auto"),
                                        ]),
                                    ]),
                                ])
                            ])
                        ]),
                    ]),
                ]),
            ]),

            # Dummy input object for callbacks on page load
            dcc.Store(id="on-page-load"),
            dcc.Store(id="google-drive-authenticated"),

            # Storage of all DataFrames necessary for QC plot generation
            dcc.Store(id="istd-rt-pos"),
            dcc.Store(id="istd-rt-neg"),
            dcc.Store(id="istd-intensity-pos"),
            dcc.Store(id="istd-intensity-neg"),
            dcc.Store(id="istd-mz-pos"),
            dcc.Store(id="istd-mz-neg"),
            dcc.Store(id="istd-delta-rt-pos"),
            dcc.Store(id="istd-delta-rt-neg"),
            dcc.Store(id="istd-in-run-delta-rt-pos"),
            dcc.Store(id="istd-in-run-delta-rt-neg"),
            dcc.Store(id="istd-delta-mz-pos"),
            dcc.Store(id="istd-delta-mz-neg"),
            dcc.Store(id="sequence"),
            dcc.Store(id="metadata"),
            dcc.Store(id="bio-rt-pos"),
            dcc.Store(id="bio-rt-neg"),
            dcc.Store(id="bio-intensity-pos"),
            dcc.Store(id="bio-intensity-neg"),
            dcc.Store(id="bio-mz-pos"),
            dcc.Store(id="bio-mz-neg"),
            dcc.Store(id="study-resources"),
            dcc.Store(id="samples"),
            dcc.Store(id="pos-internal-standards"),
            dcc.Store(id="neg-internal-standards"),
            dcc.Store(id="instruments"),

            # Data for starting a new AutoQC job
            dcc.Store(id="new-sequence"),
            dcc.Store(id="new-metadata"),
            dcc.Store(id="filenames-for-bulk-qc"),
            
            # Dummy inputs for UI update callbacks
            dcc.Store(id="chromatography-added"),
            dcc.Store(id="chromatography-removed"),
            dcc.Store(id="chromatography-msdial-config-added"),
            dcc.Store(id="istd-msp-added"),
            dcc.Store(id="bio-standard-added"),
            dcc.Store(id="bio-standard-removed"),
            dcc.Store(id="bio-msp-added"),
            dcc.Store(id="bio-standard-msdial-config-added"),
            dcc.Store(id="qc-config-added"),
            dcc.Store(id="qc-config-removed"),
            dcc.Store(id="qc-parameters-saved"),
            dcc.Store(id="qc-parameters-reset"),
            dcc.Store(id="msdial-config-added"),
            dcc.Store(id="msdial-config-removed"),
            dcc.Store(id="msdial-parameters-saved"),
            dcc.Store(id="msdial-parameters-reset"),
            dcc.Store(id="msdial-directory-data"),
            dcc.Store(id="google-drive-sync-finished"),
            dcc.Store(id="close-sync-modal"),
            dcc.Store(id="database-md5"),
            dcc.Store(id="selected-data-folder"),
            dcc.Store(id="selected-msdial-folder"),
            dcc.Store(id="google-drive-user-added"),
            dcc.Store(id="google-drive-user-deleted"),

            # Dummy inputs for Google Drive authentication
            dcc.Store(id="workspace-has-been-setup-1"),
            dcc.Store(id="workspace-has-been-setup-2"),
            dcc.Store(id="google-drive-authenticated-1"),
            dcc.Store(id="gdrive-folder-id-1"),
            dcc.Store(id="gdrive-database-file-id-1"),
            dcc.Store(id="google-drive-authenticated-2"),
            dcc.Store(id="gdrive-folder-id-2"),
            dcc.Store(id="gdrive-database-file-id-2"),
            dcc.Store(id="google-drive-authenticated-3"),
            dcc.Store(id="gdrive-folder-id-3"),
            dcc.Store(id="gdrive-database-file-id-3"),
        ])
    ])

# Serve app layout
app.layout = serve_layout

"""
Dash callbacks
"""

@app.callback(Output("google-drive-authenticated", "data"),
              Input("on-page-load", "data"))
def authenticate_with_google_drive(on_page_load):

    """
    Authenticates with Google Drive if the credentials file is found
    """

    if db.sync_is_enabled():

        # Create Google Drive instance
        gauth_holder[0] = GoogleAuth(settings_file=drive_settings_file)
        gauth = gauth_holder[0]

        # If no credentials file, make user authenticate
        if not os.path.exists(credentials_file) and db.is_valid():
            gauth.LocalWebserverAuth()

        # Try to load saved client credentials
        gauth.LoadCredentialsFile(credentials_file)

        # Initialize saved credentials
        if gauth.credentials is not None:

            # Refresh credentials if expired
            if gauth.access_token_expired:
                gauth.Refresh()

            # Otherwise, authorize saved credentials
            else:
                gauth.Authorize()

        # If no saved credentials, make user authenticate again
        elif gauth.credentials is None:
            gauth.LocalWebserverAuth()

        if not os.path.exists(credentials_file) and db.is_valid():
            gauth.SaveCredentialsFile(credentials_file)

        return os.path.exists(credentials_file)

    else:
        raise PreventUpdate


@app.callback(Output("google-drive-authenticated-1", "data"),
              Output("google-drive-authenticated-2", "data"),
              Output("google-drive-authenticated-3", "data"),
              Input("setup-google-drive-button-1", "n_clicks"),
              Input("setup-google-drive-button-2", "n_clicks"),
              Input("google-drive-sync-button", "n_clicks"), prevent_initial_call=True)
def launch_google_drive_authentication(setup_auth_button_clicks, sign_in_auth_button_clicks, settings_button_clicks):

    """
    Launches Google Drive authentication window from first-time setup
    """

    # Get the correct authentication button
    button_id = ctx.triggered_id

    # If user clicks a sign-in button, launch Google authentication page
    if button_id is not None:
        # Authenticate, then save the credentials to a file
        gauth_holder[0] = GoogleAuth(settings_file=drive_settings_file)
        gauth = gauth_holder[0]
        gauth.LocalWebserverAuth()

    if button_id == "setup-google-drive-button-1":
        return True, None, None
    elif button_id == "setup-google-drive-button-2":
        return None, True, None
    elif button_id == "google-drive-sync-button":
        return None, None, True
    else:
        raise PreventUpdate


@app.callback(Output("setup-google-drive-button-1", "children"),
              Output("setup-google-drive-button-1", "color"),
              Output("setup-google-drive-button-1", "outline"),
              Output("google-drive-button-1-popover", "children"),
              Output("google-drive-button-1-popover", "is_open"),
              Output("gdrive-folder-id-1", "data"),
              Output("gdrive-database-file-id-1", "data"),
              Input("google-drive-authenticated-1", "data"), prevent_initial_call=True)
def check_first_time_google_drive_authentication(google_drive_is_authenticated):

    """
    UI feedback for Google Drive authentication in Welcome > Setup New Instrument page
    """

    if google_drive_is_authenticated:

        drive = GoogleDrive(gauth_holder[0])

        # Initial values
        gdrive_folder_id = None
        gdrive_database_file_id = None
        popover_message = [dbc.PopoverHeader("No existing workspace found."),
                           dbc.PopoverBody("A new MS-AutoQC workspace will be created.")]

        # Check for database in Google Drive
        for file in drive.ListFile({"q": "'root' in parents and trashed=false"}).GetList():
            if file["title"] == "MS-AutoQC":
                gdrive_folder_id = file["id"]
                break

        # If Google Drive folder is found, look for database next
        if gdrive_folder_id is not None:
            for file in drive.ListFile({"q": "'" + gdrive_folder_id + "' in parents and trashed=false"}).GetList():
                if file["title"] == "QC Database.db":
                    file.GetContentFile(file["title"])
                    gdrive_database_file_id = file["id"]
                    popover_message = [dbc.PopoverHeader("Workspace found!"),
                        dbc.PopoverBody("This instrument will be added to the existing MS-AutoQC workspace.")]
                    break

        return "You're signed in!", "success", False, popover_message, True, gdrive_folder_id, gdrive_database_file_id

    else:
        return "Sign in to Google Drive", "primary", True, "", False, "", ""


@app.callback(Output("first-time-instrument-vendor", "label"),
              Output("thermo-fisher-item", "n_clicks"),
              Output("agilent-item", "n_clicks"),
              Output("bruker-item", "n_clicks"),
              Output("sciex-item", "n_clicks"),
              Output("waters-item", "n_clicks"),
              Input("thermo-fisher-item", "n_clicks"),
              Input("agilent-item", "n_clicks"),
              Input("bruker-item", "n_clicks"),
              Input("sciex-item", "n_clicks"),
              Input("waters-item", "n_clicks"), prevent_initial_call=True)
def vendor_dropdown_handling(thermo_fisher_click, agilent_click, bruker_click, sciex_click, waters_click):

    """
    Why didn't Dash Bootstrap Components implement this themselves?
    The world may never know...
    """

    thermo_selected = "Thermo Fisher", 0, 0, 0, 0, 0
    agilent_selected = "Agilent", 0, 0, 0, 0, 0,
    bruker_selected = "Bruker", 0, 0, 0, 0, 0
    sciex_selected = "Sciex", 0, 0, 0, 0, 0
    waters_selected = "Waters", 0, 0, 0, 0, 0

    inputs = [thermo_fisher_click, agilent_click, bruker_click, sciex_click, waters_click]
    outputs = [thermo_selected, agilent_selected, bruker_selected, sciex_selected, waters_selected]

    for index, input in enumerate(inputs):
        if input is not None:
            if input > 0:
                return outputs[index]


@app.callback(Output("first-time-complete-setup-button", "disabled"),
              Output("first-time-instrument-id", "valid"),
              Input("first-time-instrument-id", "value"),
              Input("first-time-instrument-vendor", "label"), prevent_initial_call=True)
def enable_complete_setup_button(instrument_name, instrument_vendor):

    """
    Enables "Complete setup" button upon form completion in Welcome > Setup New Instrument page
    """

    valid = False, True
    invalid = True, False

    if instrument_name is not None:
        if len(instrument_name) > 3 and instrument_vendor != "Choose Vendor":
            return valid
        else:
            return invalid
    else:
        return invalid


@app.callback(Output("first-time-complete-setup-button", "children"),
              Input("first-time-complete-setup-button", "n_clicks"), prevent_initial_call=True)
def ui_feedback_for_complete_setup_button(button_click):

    """
    Returns loading feedback on complete setup button
    """

    return [dbc.Spinner(size="sm"), " Finishing up, please wait..."]


@app.callback(Output("workspace-has-been-setup-1", "data"),
              Input("first-time-complete-setup-button", "children"),
              State("first-time-instrument-id", "value"),
              State("first-time-instrument-vendor", "label"),
              State("google-drive-authenticated-1", "data"),
              State("gdrive-folder-id-1", "data"),
              State("gdrive-database-file-id-1", "data"), prevent_initial_call=True)
def complete_first_time_setup(button_click, instrument_id, instrument_vendor, google_drive_authenticated,
                              gdrive_folder_id, gdrive_file_id):

    """
    Upon "Complete setup" button click, this callback completes the following:
    1. If database DOES exist in Google Drive, downloads database
    2. If database DOES NOT exist in Google Drive, initializes new SQLite database in /data
    3. Adds instrument to "instruments" table
    4. Uploads database to Google Drive folder
    5. Dismisses setup window
    """

    if button_click:
        drive = GoogleDrive(gauth_holder[0])

        # Initialize a new database if one does not exist
        if not db.is_valid():
            db.create_database()

        # Handle Google Drive sync
        if google_drive_authenticated:

            # Create necessary folders if not found
            if gdrive_folder_id is None:

                # Create MS-AutoQC folder
                folder_metadata = {
                    "title": "MS-AutoQC",
                    "mimeType": "application/vnd.google-apps.folder"
                }
                folder = drive.CreateFile(folder_metadata)
                folder.Upload()

                # Get Google Drive ID of folder
                for file in drive.ListFile({"q": "'root' in parents and trashed=false"}).GetList():
                    if file["title"] == "MS-AutoQC":
                        gdrive_folder_id = file["id"]
                        break

                # Create methods folder inside of MS-AutoQC folder
                folder_metadata = {
                    "title": "methods",
                    "parents": [{"id": gdrive_folder_id}],
                    "mimeType": "application/vnd.google-apps.folder"
                }
                folder = drive.CreateFile(folder_metadata)
                folder.Upload()

            # Update database in Google Drive folder
            if gdrive_file_id is not None:
                file = drive.CreateFile({"id": gdrive_file_id})
            else:
                metadata = {
                    "title": "QC Database.db",
                    "parents": [{"id": gdrive_folder_id}],
                }
                file = drive.CreateFile(metadata=metadata)

            file.SetContentFile("QC Database.db")
            file.Upload()

            if gdrive_file_id is None:
                # Get Google Drive ID of database file
                for file in drive.ListFile({"q": "'" + gdrive_folder_id + "' in parents and trashed=false"}).GetList():
                    if file["title"] == "QC Database.db":
                        gdrive_file_id = file["id"]
                        break

            # Save user credentials
            gauth_holder[0].SaveCredentialsFile(credentials_file)

        # Create local methods directory
        methods_directory = os.path.join(current_directory, "methods")
        if not os.path.exists(methods_directory):
            os.makedirs(methods_directory)

        # Add instrument to database
        db.insert_new_instrument(instrument_id, instrument_vendor, gdrive_folder_id, gdrive_file_id)

        # Dismiss setup window by returning True for workspace_has_been_setup boolean
        return db.is_valid()

    else:
        raise PreventUpdate


@app.callback(Output("setup-google-drive-button-2", "children"),
              Output("setup-google-drive-button-2", "color"),
              Output("setup-google-drive-button-2", "outline"),
              Output("google-drive-button-2-popover", "children"),
              Output("google-drive-button-2-popover", "is_open"),
              Output("gdrive-folder-id-2", "data"),
              Output("gdrive-database-file-id-2", "data"),
              Input("google-drive-authenticated-2", "data"), prevent_initial_call=True)
def check_workspace_login_google_drive_authentication(google_drive_is_authenticated):

    """
    UI feedback for Google Drive authentication in Welcome > Sign In To Workspace page
    """

    if google_drive_is_authenticated:
        drive = GoogleDrive(gauth_holder[0])

        # Initial values
        gdrive_folder_id = None
        gdrive_database_file_id = None
        methods_folder_id = None

        # Failed popover message
        button_text = "Sign in to Google Drive"
        button_color = "danger"
        popover_message = [dbc.PopoverHeader("No workspace found"),
                           dbc.PopoverBody("Double-check that your Google account has access in " +
                                           "Settings > General, or sign in from a different account.")]

        # Check for MS-AutoQC folder in Google Drive root directory
        for file in drive.ListFile({"q": "'root' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"}).GetList():
            if file["title"] == "MS-AutoQC":
                gdrive_folder_id = file["id"]
                break

        # If it's not there, check "Shared With Me" and copy it over to root directory
        if gdrive_folder_id is None:
            for file in drive.ListFile({"q": "sharedWithMe and mimeType='application/vnd.google-apps.folder' and trashed=false"}).GetList():
                if file["title"] == "MS-AutoQC":
                    gdrive_folder_id = file["id"]
                    break

        # If Google Drive folder is found, look for database next
        if gdrive_folder_id is not None:
            for file in drive.ListFile({"q": "'" + gdrive_folder_id + "' in parents and trashed=false"}).GetList():
                # Download database if found
                if file["title"] == "QC Database.db":
                    file.GetContentFile(file["title"])
                    gdrive_database_file_id = file["id"]

                    # Popover alert
                    button_text = "Signed in to Google Drive"
                    button_color = "success"
                    popover_message = [dbc.PopoverHeader("Workspace found!"),
                        dbc.PopoverBody("Click the button below to sign in.")]

        return button_text, button_color, False, popover_message, True, gdrive_folder_id, gdrive_database_file_id

    else:
        return "Sign in to Google Drive", "primary", True, "", False, "", ""


@app.callback(Output("first-time-sign-in-button", "disabled"),
              Input("setup-google-drive-button-2", "children"), prevent_initial_call=True)
def enable_workspace_login_button(button_text):

    """
    Enables "Sign in to workspace" button upon form completion in Welcome > Sign In To Workspace page
    """

    if button_text is not None:
        if button_text == "Signed in to Google Drive":
            return False
        else:
            return True
    else:
        return True


@app.callback(Output("first-time-sign-in-button", "children"),
              Input("first-time-sign-in-button", "n_clicks"), prevent_initial_call=True)
def ui_feedback_for_workspace_login_button(button_click):

    """
    UI feedback for workspace sign in button in Setup > Login To Workspace
    """

    return [dbc.Spinner(size="sm"), " Signing in, please wait..."]


@app.callback(Output("workspace-has-been-setup-2", "data"),
              Input("first-time-sign-in-button", "children"),
              State("gdrive-folder-id-2", "data"), prevent_initial_call=True)
def ui_feedback_for_workspace_login_button(button_click, gdrive_folder_id):

    """
    Dismisses setup window and signs in to MS-AutoQC workspace
    """

    if button_click:

        drive = GoogleDrive(gauth_holder[0])
        methods_folder_id = None

        # Find methods directory in Google Drive
        if gdrive_folder_id is not None:
            for file in drive.ListFile({"q": "'" + gdrive_folder_id + "' in parents and trashed=false"}).GetList():
                if file["title"] == "methods":
                    methods_folder_id = file["id"]

        # Use ID to download contents of methods directory
        if methods_folder_id is not None:

            # Create methods directory if it does not exist
            methods_directory = os.path.join(current_directory, "methods")
            if not os.path.exists(methods_directory):
                os.makedirs(methods_directory)

            # Change to methods directory
            os.chdir(methods_directory)

            # Download files
            for file in drive.ListFile({"q": "'" + methods_folder_id + "' in parents and trashed=false"}).GetList():
                file.GetContentFile(file["title"])

            # Change back to root directory
            os.chdir(current_directory)

        # Save Google Drive credentials
        gauth_holder[0].SaveCredentialsFile(credentials_file)

        return True

    else:
        raise PreventUpdate


@app.callback(Output("workspace-setup-modal", "is_open"),
              Output("on-page-load", "data"),
              Input("workspace-has-been-setup-1", "data"),
              Input("workspace-has-been-setup-2", "data"))
def dismiss_setup_window(workspace_has_been_setup_1, workspace_has_been_setup_2):

    """
    Checks for a valid database on every start and dismisses setup window if found
    """

    # Check if setup is complete
    is_valid = db.is_valid()

    return not is_valid, is_valid


@app.callback(Output("google-drive-sync-button", "color"),
              Output("google-drive-sync-button", "children"),
              Output("google-drive-sync-form-text", "children"),
              Output("google-drive-sign-in-from-settings-alert", "is_open"),
              Input("google-drive-authenticated-3", "data"),
              Input("google-drive-authenticated", "data"),
              State("google-drive-sync-form-text", "children"), prevent_initial_call=True)
def update_google_drive_sync_status_in_settings(google_drive_authenticated, google_drive_authenticated_on_start, form_text):

    """
    Updates Google Drive sync status in user settings on user authentication
    """

    auth_source = ctx.triggered_id

    # Authenticated on app startup
    if auth_source == "google-drive-authenticated":
        form_text = "Cloud sync is enabled! You can now sign in to this MS-AutoQC workspace from any device."
        return "success", "Signed in to Google Drive", form_text, False

    # Authenticated from "Sign in to Google Drive" button in Settings > General
    elif auth_source == "google-drive-authenticated-3" and google_drive_authenticated_on_start is None:

        drive = GoogleDrive(gauth_holder[0])
        gdrive_folder_id = None
        gdrive_file_id = None

        # Check for MS-AutoQC folder in Google Drive root directory
        for file in drive.ListFile({"q": "'root' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"}).GetList():
            if file["title"] == "MS-AutoQC":
                gdrive_folder_id = file["id"]
                break

        # If it's not there, check "Shared With Me" and copy it over to root directory
        if gdrive_folder_id is None:
            for file in drive.ListFile({"q": "sharedWithMe and mimeType='application/vnd.google-apps.folder' and trashed=false"}).GetList():
                if file["title"] == "MS-AutoQC":
                    gdrive_folder_id = file["id"]
                    break

        # If Google Drive folder is found, alert user that they need to sign in with a different Google account
        if gdrive_folder_id is not None:
            os.remove(credentials_file)
            return "danger", "Sign in to Google Drive", form_text, True

        # If no workspace found, all good to create one
        else:
            # Create MS-AutoQC folder
            folder_metadata = {
                "title": "MS-AutoQC",
                "mimeType": "application/vnd.google-apps.folder"
            }
            folder = drive.CreateFile(folder_metadata)
            folder.Upload()

            # Get Google Drive ID of folder
            for file in drive.ListFile({"q": "'root' in parents and trashed=false"}).GetList():
                if file["title"] == "MS-AutoQC":
                    gdrive_folder_id = file["id"]
                    break

            # Create methods folder inside of MS-AutoQC folder
            folder_metadata = {
                "title": "methods",
                "parents": [{"id": gdrive_folder_id}],
                "mimeType": "application/vnd.google-apps.folder"
            }
            folder = drive.CreateFile(folder_metadata)
            folder.Upload()

            # Update database in Google Drive folder
            metadata = {
                "title": "QC Database.db",
                "parents": [{"id": gdrive_folder_id}],
            }
            file = drive.CreateFile(metadata=metadata)
            file.SetContentFile("QC Database.db")
            file.Upload()

            # Get Google Drive ID of database file
            for file in drive.ListFile({"q": "'" + gdrive_folder_id + "' in parents and trashed=false"}).GetList():
                if file["title"] == "QC Database.db":
                    gdrive_file_id = file["id"]
                    break

            # Put Google Drive ID's into database
            db.insert_google_drive_ids(gdrive_folder_id, gdrive_file_id)

            # Sync database
            db.sync_to_google_drive(drive=drive, sync_settings=True)

            # Save user credentials
            gauth_holder[0].SaveCredentialsFile(credentials_file)

        form_text = "Cloud sync is enabled! You can now sign in to this MS-AutoQC workspace from any device."
        return "success", "Signed in to Google Drive", form_text, False

    else:
        raise PreventUpdate


@app.callback(Output("tabs", "children"),
              Output("tabs", "value"),
              Input("instruments", "data"),
              Input("workspace-setup-modal", "is_open"))
def get_instrument_tabs(instruments, check_workspace_setup):

    """
    Retrieves all instruments on a user installation of MS-AutoQC
    """

    if db.is_valid():

        # Get list of instruments from database
        instrument_list = db.get_instruments_list()

        # Create tabs for each instrument
        instrument_tabs = []
        for instrument in instrument_list:
            instrument_tabs.append(
                dcc.Tab(label=instrument, value=instrument))

        return instrument_tabs, instrument_list[0]

    else:
        raise PreventUpdate


@app.callback(Output("instrument-run-table", "active_cell"),
              Output("instrument-run-table", "selected_cells"),
              Input("tabs", "value"), prevent_initial_call=True)
def reset_instrument_table(instrument):

    """
    Removes selected cell highlight upon tab switch to different instrument
    (A case study in insane side missions during frontend development)
    """

    return None, []


@app.callback(Output("instrument-run-table", "data"),
              Output("table-container", "style"),
              Output("plot-container", "style"),
              Input("tabs", "value"))
def populate_instrument_runs_table(instrument):

    """
    Dash callback for populating tables with list of past/active instrument runs
    """

    if instrument != "tab-1":
        # Get instrument runs from database
        df_instrument_runs = db.get_instrument_runs(instrument)

        if len(df_instrument_runs) == 0:
            empty_table = [{"Run ID": "N/A", "Chromatography": "N/A", "Status": "N/A"}]
            return empty_table, {"display": "block"}, {"display": "none"}

        # DataFrame refactoring
        df_instrument_runs = df_instrument_runs[["run_id", "chromatography", "status"]]
        df_instrument_runs = df_instrument_runs.rename(
            columns={"run_id": "Run ID",
                     "chromatography": "Chromatography",
                     "status": "Status"})

        # Convert DataFrame into a dictionary
        instrument_runs = df_instrument_runs.to_dict("records")
        return instrument_runs, {"display": "block"}, {"display": "block"}

    else:
        raise PreventUpdate


@app.callback(Output("istd-rt-pos", "data"),
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
              Output("samples", "data"),
              Output("pos-internal-standards", "data"),
              Output("neg-internal-standards", "data"),
              Output("istd-delta-rt-pos", "data"),
              Output("istd-delta-rt-neg", "data"),
              Output("istd-in-run-delta-rt-pos", "data"),
              Output("istd-in-run-delta-rt-neg", "data"),
              Output("istd-delta-mz-pos", "data"),
              Output("istd-delta-mz-neg", "data"),
              Input("instrument-run-table", "active_cell"),
              State("instrument-run-table", "data"), prevent_initial_call=True, suppress_callback_exceptions=True)
def load_data(active_cell, table_data):

    """
    Stores QC results in dcc.Store objects (user's browser session)
    """

    if active_cell:
        study_id = table_data[active_cell["row"]]["Run ID"]
        return get_qc_results(study_id)
    else:
        raise PreventUpdate


@app.callback(Output("loading-modal", "is_open"),
              Output("loading-modal-title", "children"),
              Output("loading-modal-body", "children"),
              Input("instrument-run-table", "active_cell"),
              State("instrument-run-table", "data"),
              Input("sample-table", "data"),
              State("loading-modal", "is_open"),
              State("study-resources", "data"), prevent_initial_call=True)
def loading_data_feedback(active_cell, table_data, placeholder_input, modal_is_open, study_resources):

    """
    Dash callback for providing user feedback when retrieving data from Google Drive
    """

    if active_cell:
        # status = table_data[active_cell["row"]]["Status"]

        if study_resources:
            study_name = json.loads(study_resources)["run_id"]
            if table_data[active_cell["row"]][active_cell["column_id"]] != study_name:
                study_name = table_data[active_cell["row"]]["Run ID"]
            else:
                return False, None, None
        else:
            study_name = table_data[active_cell["row"]]["Run ID"]

        if modal_is_open:
            return False, None, None

        title = html.Div([
            html.Div(children=[dbc.Spinner(color="primary"), " Loading QC results for " + study_name])
        ])

        body = "This may take a few seconds..."

        # if status == "Active":
        #    return True, title, body
        # else:
        #    return False, None, None

        return True, title, body

    else:
        return False, None, None


@app.callback(Output("sample-table", "data"),
              Input("samples", "data"), prevent_initial_call=True)
def populate_sample_tables(samples):

    """
    Populates table with list of samples for selected run from instrument runs table
    """

    if samples is not None:
        df_samples = pd.read_json(samples, orient="split")
        return df_samples.to_dict("records")
    else:
        return None


@app.callback(Output("istd-rt-dropdown", "options"),
              Output("istd-mz-dropdown", "options"),
              Output("istd-intensity-dropdown", "options"),
              Output("bio-standard-benchmark-dropdown", "options"),
              Output("rt-plot-sample-dropdown", "options"),
              Output("mz-plot-sample-dropdown", "options"),
              Output("intensity-plot-sample-dropdown", "options"),
              Input("polarity-options", "value"),
              State("sample-table", "data"),
              Input("samples", "data"),
              State("bio-intensity-pos", "data"),
              State("bio-intensity-neg", "data"),
              State("pos-internal-standards", "data"),
              State("neg-internal-standards", "data"))
def update_dropdowns_on_polarity_change(polarity, table_data, samples, bio_intensity_pos, bio_intensity_neg,
    pos_internal_standards, neg_internal_standards):

    """
    Updates dropdown lists with correct items for user-selected polarity
    """

    if samples is not None:
        df_samples = pd.read_json(samples, orient="split")

        if polarity == "Neg":
            istd_dropdown = json.loads(neg_internal_standards)

            if bio_intensity_neg is not None:
                df = pd.read_json(bio_intensity_neg, orient="split")
                bio_dropdown = df["Name"].astype(str).unique().tolist()

            df_samples = df_samples.loc[df_samples["Sample"].str.contains("Neg")]
            sample_dropdown = df_samples["Sample"].tolist()

        elif polarity == "Pos":
            istd_dropdown = json.loads(pos_internal_standards)

            if bio_intensity_pos is not None:
                df = pd.read_json(bio_intensity_pos, orient="split")
                bio_dropdown = df["Name"].astype(str).unique().tolist()

            df_samples = df_samples.loc[df_samples["Sample"].str.contains("Pos")]
            sample_dropdown = df_samples["Sample"].tolist()

        return istd_dropdown, istd_dropdown, istd_dropdown, bio_dropdown, sample_dropdown, sample_dropdown, sample_dropdown

    else:
        return [], [], [], [], [], [], []


@app.callback(Output("rt-plot-sample-dropdown", "value"),
              Output("mz-plot-sample-dropdown", "value"),
              Output("intensity-plot-sample-dropdown", "value"),
              Input("sample-filtering-options", "value"),
              Input("polarity-options", "value"),
              Input("samples", "data"),
              State("metadata", "data"), prevent_initial_call=True)
def apply_sample_filter_to_plots(filter, polarity, samples, metadata):

    """
    Apply sample filter to internal standard plots, options are:
    1. All samples
    2. Filter by samples only
    3. Filter by treatments / classes
    4. Filter by pools
    5. Filter by blanks
    """

    # Get complete list of samples (including blanks + pools) in polarity
    if samples is not None:
        df_samples = pd.read_json(samples, orient="split")
        df_samples = df_samples.loc[df_samples["Sample"].str.contains(polarity)]
        sample_list = df_samples["Sample"].tolist()
    else:
        raise PreventUpdate

    if filter is not None:
        # Return all samples, blanks, and pools
        if filter == "all":
            return [], [], []

        # Return samples only
        elif filter == "samples":
            df_metadata = pd.read_json(metadata, orient="split")
            df_metadata = df_metadata.loc[df_metadata["Filename"].isin(sample_list)]
            samples_only = df_metadata["Filename"].tolist()
            return samples_only, samples_only, samples_only

        # Return pools only
        elif filter == "pools":
            pools = [sample for sample in sample_list if "QC" in sample]
            return pools, pools, pools

        # Return blanks only
        elif filter == "blanks":
            blanks = [sample for sample in sample_list if "BK" in sample]
            return blanks, blanks, blanks

    else:
        return [], [], []


@app.callback(Output("istd-rt-plot", "figure"),
              Output("rt-prev-button", "n_clicks"),
              Output("rt-next-button", "n_clicks"),
              Output("istd-rt-dropdown", "value"),
              Input("polarity-options", "value"),
              Input("istd-rt-dropdown", "value"),
              Input("rt-plot-sample-dropdown", "value"),
              Input("istd-rt-pos", "data"),
              Input("istd-rt-neg", "data"),
              State("samples", "data"),
              State("study-resources", "data"),
              State("pos-internal-standards", "data"),
              State("neg-internal-standards", "data"),
              Input("rt-prev-button", "n_clicks"),
              Input("rt-next-button", "n_clicks"), prevent_initial_call=True)
def populate_istd_rt_plot(polarity, internal_standard, selected_samples, rt_pos, rt_neg, samples, resources,
    pos_internal_standards, neg_internal_standards, previous, next):

    """
    Populates internal standard retention time vs. sample plot
    """

    if rt_pos is None and rt_neg is None:
        return {}, None, None, None

    trigger = ctx.triggered_id

    # Get internal standard RT data
    df_istd_rt_pos = pd.DataFrame()
    df_istd_rt_neg = pd.DataFrame()

    if rt_pos is not None:
        df_istd_rt_pos = pd.read_json(rt_pos, orient="split")

    if rt_neg is not None:
        df_istd_rt_neg = pd.read_json(rt_neg, orient="split")

    # Get samples
    df_samples = pd.read_json(samples, orient="split")
    samples = df_samples["Sample"].astype(str).tolist()
    identifiers = db.get_biological_standard_identifiers()
    for identifier in identifiers:
        samples = [x for x in samples if identifier not in x]

    # Filter samples and internal standards by polarity
    if polarity == "Pos":
        samples = [x for x in samples if "Pos" in x]
        internal_standards = json.loads(pos_internal_standards)
        df_istd_rt = df_istd_rt_pos
    elif polarity == "Neg":
        samples = [x for x in samples if "Neg" in x]
        internal_standards = json.loads(neg_internal_standards)
        df_istd_rt = df_istd_rt_neg

    # Get retention times
    retention_times = json.loads(resources)["retention_times_dict"]

    # Set initial dropdown values when none are selected
    if not internal_standard:
        internal_standard = internal_standards[0]

    if not selected_samples:
        selected_samples = samples

    # Calculate index of internal standard from button clicks
    if trigger == "rt-prev-button" or trigger == "rt-next-button":
        index = get_internal_standard_index(previous, next, len(internal_standards))
        internal_standard = internal_standards[index]
    else:
        index = next

    try:
        # Generate internal standard RT vs. sample plot
        return load_istd_rt_plot(dataframe=df_istd_rt, samples=selected_samples,
            internal_standard=internal_standard, retention_times=retention_times), \
                None, index, internal_standard

    except Exception as error:
        print("Error in loading RT vs. sample plot:", error)
        return {}, None, None, None


@app.callback(Output("istd-intensity-plot", "figure"),
              Output("intensity-prev-button", "n_clicks"),
              Output("intensity-next-button", "n_clicks"),
              Output("istd-intensity-dropdown", "value"),
              Input("polarity-options", "value"),
              Input("istd-intensity-dropdown", "value"),
              Input("intensity-plot-sample-dropdown", "value"),
              Input("istd-intensity-pos", "data"),
              Input("istd-intensity-neg", "data"),
              State("samples", "data"),
              State("metadata", "data"),
              State("pos-internal-standards", "data"),
              State("neg-internal-standards", "data"),
              Input("intensity-prev-button", "n_clicks"),
              Input("intensity-next-button", "n_clicks"), prevent_initial_call=True)
def populate_istd_intensity_plot(polarity, internal_standard, selected_samples, intensity_pos, intensity_neg, samples, metadata,
    pos_internal_standards, neg_internal_standards, previous, next):

    """
    Populates internal standard intensity vs. sample plot
    """

    if intensity_pos is None and intensity_neg is None:
        return {}, None, None, None

    trigger = ctx.triggered_id

    # Get internal standard intensity data
    df_istd_intensity_pos = pd.DataFrame()
    df_istd_intensity_neg = pd.DataFrame()

    if intensity_pos is not None:
        df_istd_intensity_pos = pd.read_json(intensity_pos, orient="split")

    if intensity_neg is not None:
        df_istd_intensity_neg = pd.read_json(intensity_neg, orient="split")

    # Get samples
    df_samples = pd.read_json(samples, orient="split")
    samples = df_samples["Sample"].astype(str).tolist()
    identifiers = db.get_biological_standard_identifiers()
    for identifier in identifiers:
        samples = [x for x in samples if identifier not in x]

    # Get sample metadata
    df_metadata = pd.read_json(metadata, orient="split")

    # Filter samples and internal standards by polarity
    if polarity == "Pos":
        samples = [x for x in samples if "Pos" in x]
        internal_standards = json.loads(pos_internal_standards)
        df_istd_intensity = df_istd_intensity_pos
    elif polarity == "Neg":
        samples = [x for x in samples if "Neg" in x]
        internal_standards = json.loads(neg_internal_standards)
        df_istd_intensity = df_istd_intensity_neg

    # Set initial internal standard dropdown value when none are selected
    if not internal_standard:
        internal_standard = internal_standards[0]

    # Set initial sample dropdown value when none are selected
    if not selected_samples:
        selected_samples = samples
        treatments = []
    else:
        df_metadata = df_metadata.loc[df_metadata["Filename"].isin(selected_samples)]
        df_metadata = df_metadata.sort_values(by=["Treatment"])
        treatments = df_metadata["Treatment"].tolist()
        if len(df_metadata) == len(selected_samples):
            selected_samples = df_metadata["Filename"].tolist()

    # Calculate index of internal standard from button clicks
    if trigger == "intensity-prev-button" or trigger == "intensity-next-button":
        index = get_internal_standard_index(previous, next, len(internal_standards))
        internal_standard = internal_standards[index]
    else:
        index = next

    try:
        # Generate internal standard intensity vs. sample plot
        return load_istd_intensity_plot(dataframe=df_istd_intensity, samples=selected_samples,
        internal_standard=internal_standard, text=selected_samples, treatments=treatments), \
               None, index, internal_standard

    except Exception as error:
        print("Error in loading intensity vs. sample plot:", error)
        return {}, None, None, None


@app.callback(Output("istd-mz-plot", "figure"),
              Output("mz-prev-button", "n_clicks"),
              Output("mz-next-button", "n_clicks"),
              Output("istd-mz-dropdown", "value"),
              Input("polarity-options", "value"),
              Input("istd-mz-dropdown", "value"),
              Input("mz-plot-sample-dropdown", "value"),
              Input("istd-delta-mz-pos", "data"),
              Input("istd-delta-mz-neg", "data"),
              State("samples", "data"),
              State("pos-internal-standards", "data"),
              State("neg-internal-standards", "data"),
              State("study-resources", "data"),
              Input("mz-prev-button", "n_clicks"),
              Input("mz-next-button", "n_clicks"), prevent_initial_call=True)
def populate_istd_mz_plot(polarity, internal_standard, selected_samples, delta_mz_pos, delta_mz_neg, samples,
    pos_internal_standards, neg_internal_standards, resources, previous, next):

    """
    Populates internal standard delta m/z vs. sample plot
    """

    if delta_mz_pos is None and delta_mz_neg is None:
        return {}, None, None, None

    trigger = ctx.triggered_id

    # Get chromatography
    chromatography = json.loads(resources)["chromatography"]

    # Get internal standard RT data
    df_istd_mz_pos = pd.DataFrame()
    df_istd_mz_neg = pd.DataFrame()

    if delta_mz_pos is not None:
        df_istd_mz_pos = pd.read_json(delta_mz_pos, orient="split")

    if delta_mz_neg is not None:
        df_istd_mz_neg = pd.read_json(delta_mz_neg, orient="split")

    # Get samples (and filter out biological standards)
    df_samples = pd.read_json(samples, orient="split")
    samples = df_samples["Sample"].astype(str).tolist()
    identifiers = db.get_biological_standard_identifiers()
    for identifier in identifiers:
        samples = [x for x in samples if identifier not in x]

    # Filter samples and internal standards by polarity
    if polarity == "Pos":
        samples = [x for x in samples if "Pos" in x]
        internal_standards = json.loads(pos_internal_standards)
        df_istd_mz = df_istd_mz_pos
        pol = "Positive Mode"
    elif polarity == "Neg":
        samples = [x for x in samples if "Neg" in x]
        internal_standards = json.loads(neg_internal_standards)
        df_istd_mz = df_istd_mz_neg
        pol = "Negative Mode"

    # Set initial dropdown values when none are selected
    if not internal_standard:
        internal_standard = internal_standards[0]
    if not selected_samples:
        selected_samples = samples

    # Calculate index of internal standard from button clicks
    if trigger == "mz-prev-button" or trigger == "mz-next-button":
        index = get_internal_standard_index(previous, next, len(internal_standards))
        internal_standard = internal_standards[index]
    else:
        index = next

    try:
        # Generate internal standard delta m/z vs. sample plot
        return load_istd_delta_mz_plot(dataframe=df_istd_mz, samples=selected_samples,
            internal_standard=internal_standard, chromatography=chromatography, polarity=pol), \
               None, index, internal_standard

    except Exception as error:
        print("Error in loading delta m/z vs. sample plot:", error)
        return {}, None, None, None


@app.callback(Output("bio-standard-mz-rt-plot", "figure"),
              Output("bio-standard-benchmark-dropdown", "value"),
              Output("bio-standard-mz-rt-plot", "clickData"),
              Input("polarity-options", "value"),
              Input("bio-rt-pos", "data"),
              Input("bio-rt-neg", "data"),
              State("bio-intensity-pos", "data"),
              State("bio-intensity-neg", "data"),
              State("bio-mz-pos", "data"),
              State("bio-mz-neg", "data"),
              State("study-resources", "data"),
              Input("bio-standard-mz-rt-plot", "clickData"), prevent_initial_call=True)
def populate_bio_standard_mz_rt_plot(polarity, rt_pos, rt_neg, intensity_pos, intensity_neg, mz_pos, mz_neg, resources, click_data):

    """
    Populates biological standard m/z vs. RT plot
    """

    if rt_pos is None and rt_neg is None:
        if mz_pos is None and mz_neg is None:
            return {}, None, None

    # Get run ID and chromatography method
    run_id = json.loads(resources)["run_id"]

    # Get biological standard m/z, RT, and intensity data
    if polarity == "Pos":
        if rt_pos is not None and intensity_pos is not None and mz_pos is not None:
            df_bio_rt = pd.read_json(rt_pos, orient="split")
            df_bio_intensity = pd.read_json(intensity_pos, orient="split")
            df_bio_mz = pd.read_json(mz_pos, orient="split")

    elif polarity == "Neg":
        if rt_neg is not None and intensity_neg is not None and mz_neg is not None:
            df_bio_rt = pd.read_json(rt_neg, orient="split")
            df_bio_intensity = pd.read_json(intensity_neg, orient="split")
            df_bio_mz = pd.read_json(mz_neg, orient="split")

    if click_data is not None:
        selected_feature = click_data["points"][0]["hovertext"]
    else:
        selected_feature = None

    try:
        # Biological standard metabolites – m/z vs. retention time
        return load_bio_feature_plot(run_id=run_id, df_rt=df_bio_rt, df_mz=df_bio_mz, df_intensity=df_bio_intensity), \
               selected_feature, None
    except Exception as error:
        print("Error in loading biological standard m/z-RT plot:", error)
        return {}, None, None


@app.callback(Output("bio-standard-benchmark-plot", "figure"),
              Input("polarity-options", "value"),
              Input("bio-standard-benchmark-dropdown", "value"),
              Input("bio-intensity-pos", "data"),
              Input("bio-intensity-neg", "data"), prevent_initial_call=True)
def populate_bio_standard_benchmark_plot(polarity, selected_feature, intensity_pos, intensity_neg):

    """
    Populates biological standard benchmark plot
    """

    if intensity_pos is None and intensity_neg is None:
        return {}, None, None, None

    # Get intensity data
    if polarity == "Pos":
        if intensity_pos is not None:
            df_bio_intensity = pd.read_json(intensity_pos, orient="split")

    elif polarity == "Neg":
        if intensity_neg is not None:
            df_bio_intensity = pd.read_json(intensity_neg, orient="split")

    # Get clicked or selected feature from biological standard m/z-RT plot
    if not selected_feature:
        selected_feature = df_bio_intensity["Name"].astype(str).tolist()[0]

    try:
        # Generate biological standard metabolite intensity vs. instrument run plot
        return load_bio_benchmark_plot(dataframe=df_bio_intensity, metabolite_name=selected_feature)
    except Exception as error:
        print("Error loading biological standard intensity plot:", error)
        return {}


@app.callback(Output("sample-info-modal", "is_open"),
              Output("sample-modal-title", "children"),
              Output("sample-modal-body", "children"),
              Output("sample-table", "selected_cells"),
              Output("sample-table", "active_cell"),
              Output("istd-rt-plot", "clickData"),
              Output("istd-intensity-plot", "clickData"),
              Output("istd-mz-plot", "clickData"),
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
              State("bio-rt-pos", "data"),
              State("bio-rt-neg", "data"),
              State("bio-intensity-pos", "data"),
              State("bio-intensity-neg", "data"),
              State("bio-mz-pos", "data"),
              State("bio-mz-neg", "data"),
              State("sequence", "data"),
              State("metadata", "data"),
              State("study-resources", "data"), prevent_initial_call=True)
def toggle_sample_card(is_open, active_cell, table_data, rt_click, intensity_click, mz_click, rt_pos, rt_neg, intensity_pos,
    intensity_neg, mz_pos, mz_neg, delta_rt_pos, delta_rt_neg, in_run_delta_rt_pos, in_run_delta_rt_neg, delta_mz_pos, delta_mz_neg,
    bio_rt_pos, bio_rt_neg, bio_intensity_pos, bio_intensity_neg, bio_mz_pos, bio_mz_neg, sequence, metadata, resources):

    """
    Opens information modal when a sample is clicked from the sample table
    """

    # Get selected sample from table
    if active_cell:
        clicked_sample = table_data[active_cell["row"]][active_cell["column_id"]]

    # Get selected sample from plots
    if rt_click:
        clicked_sample = rt_click["points"][0]["x"]
        clicked_sample = clicked_sample.replace(": RT Info", "")

    if intensity_click:
        clicked_sample = intensity_click["points"][0]["x"]
        clicked_sample = clicked_sample.replace(": Height", "")

    if mz_click:
        clicked_sample = mz_click["points"][0]["x"]
        clicked_sample = clicked_sample.replace(": Precursor m/z Info", "")

    # Get run ID and chromatography method
    run_id = json.loads(resources)["run_id"]

    # Get sequence and metadata
    df_sequence = pd.read_json(sequence, orient="split")
    df_metadata = pd.read_json(metadata, orient="split")

    # Get polarity from sample name
    if "Pos" in clicked_sample:
        polarity = "Pos"
    elif "Neg" in clicked_sample:
        polarity = "Neg"

    # Check whether sample is a biological standard or not
    is_bio_standard = False
    identifiers = db.get_biological_standard_identifiers()

    for identifier in identifiers.keys():
        if identifier in clicked_sample:
            is_bio_standard = True
            break

    # Generate DataFrames with quantified features and metadata for selected sample
    if not is_bio_standard:

        if polarity == "Pos":
            df_rt = pd.read_json(rt_pos, orient="split")
            df_intensity = pd.read_json(intensity_pos, orient="split")
            df_mz = pd.read_json(mz_pos, orient="split")
            df_delta_rt = pd.read_json(delta_rt_pos, orient="split")
            df_in_run_delta_rt = pd.read_json(in_run_delta_rt_pos, orient="split")
            df_delta_mz = pd.read_json(delta_mz_pos, orient="split")

        elif polarity == "Neg":
            df_rt = pd.read_json(rt_neg, orient="split")
            df_intensity = pd.read_json(intensity_neg, orient="split")
            df_mz = pd.read_json(mz_neg, orient="split")
            df_delta_rt = pd.read_json(delta_rt_neg, orient="split")
            df_in_run_delta_rt = pd.read_json(in_run_delta_rt_neg, orient="split")
            df_delta_mz = pd.read_json(delta_mz_neg, orient="split")

        df_sample_features, df_sample_info = generate_sample_metadata_dataframe(clicked_sample, df_rt, df_mz, df_intensity,
            df_delta_rt, df_in_run_delta_rt, df_delta_mz, df_sequence, df_metadata)

    elif is_bio_standard:

        if polarity == "Pos":
            df_rt = pd.read_json(bio_rt_pos, orient="split")
            df_intensity = pd.read_json(bio_intensity_pos, orient="split")
            df_mz = pd.read_json(bio_mz_pos, orient="split")

        elif polarity == "Neg":
            df_rt = pd.read_json(bio_rt_neg, orient="split")
            df_intensity = pd.read_json(bio_intensity_neg, orient="split")
            df_mz = pd.read_json(bio_mz_neg, orient="split")

        df_sample_features, df_sample_info = generate_bio_standard_dataframe(clicked_sample, run_id, df_rt, df_mz, df_intensity)

    # Create tables from DataFrames
    metadata_table = dbc.Table.from_dataframe(df_sample_info, striped=True, bordered=True, hover=True)
    feature_table = dbc.Table.from_dataframe(df_sample_features, striped=True, bordered=True, hover=True)

    # Add tables to sample information modal
    title = clicked_sample
    body = html.Div(children=[metadata_table, feature_table])

    # Toggle modal
    if is_open:
        return False, title, body, [], None, None, None, None
    else:
        return True, title, body, [], None, None, None, None


@app.callback(Output("settings-modal", "is_open"),
              Input("settings-button", "n_clicks"), prevent_initial_call=True)
def toggle_settings_modal(button_click):

    """
    Toggles global settings modal
    """

    return True


@app.callback(Output("google-drive-sync-modal", "is_open"),
              Output("database-md5", "data"),
              Input("settings-modal", "is_open"),
              State("google-drive-authenticated", "data"),
              State("google-drive-sync-modal", "is_open"),
              Input("close-sync-modal", "data"),
              State("database-md5", "data"), prevent_initial_call=True)
def show_sync_modal(settings_is_open, google_drive_authenticated, sync_modal_is_open, sync_finished, md5_checksum):
    
    """
    Launches progress modal, which syncs database and methods directory to Google Drive
    """

    # If sync modal is open
    if sync_modal_is_open:
        # If sync is finished
        if sync_finished:
            # Close the modal
            return False, None

    # Check if settings modal has been closed
    if settings_is_open:
        return False, db.get_md5_for_database()

    elif not settings_is_open:

        # Check if user is logged into Google Drive
        if google_drive_authenticated:

            # Get MD5 checksum after use closes settings
            new_md5_checksum = db.get_md5_for_database()

            # Compare new MD5 checksum to old MD5 checksum
            if md5_checksum != new_md5_checksum:
                return True, new_md5_checksum
            else:
                return False, new_md5_checksum

        else:
            return False, None


@app.callback(Output("google-drive-sync-finished", "data"),
              Input("settings-modal", "is_open"),
              State("google-drive-authenticated", "data"),
              State("google-drive-authenticated-3", "data"),
              State("database-md5", "data"), prevent_initial_call=True)
def sync_settings_to_google_drive(settings_modal_is_open, google_drive_authenticated, auth_in_app, md5_checksum):

    """
    Syncs settings and methods files to Google Drive
    """

    if not settings_modal_is_open:
        if google_drive_authenticated or auth_in_app:
            if db.was_modified(md5_checksum):
                db.sync_to_google_drive(drive=GoogleDrive(gauth_holder[0]), sync_settings=True)
                return True

    return False


@app.callback(Output("close-sync-modal", "data"),
              Input("google-drive-sync-finished", "data"), prevent_initial_call=True)
def close_sync_modal(sync_finished):

    # You've reached Dash callback purgatory :/
    if sync_finished:
        return True


@app.callback(Output("workspace-users-table", "children"),
              Input("on-page-load", "data"),
              Input("google-drive-user-added", "data"),
              Input("google-drive-user-deleted", "data"))
def get_users_with_workspace_access(on_page_load, user_added, user_deleted):

    """
    Returns table of users that have access to the MS-AutoQC workspace
    """

    # Get users from database
    if db.is_valid():
        df_gdrive_users = db.get_table("gdrive_users")
        df_gdrive_users = df_gdrive_users.rename(
            columns={"id": "User",
                     "name": "Name",
                     "email_address": "Google Account Email Address"})
        df_gdrive_users.drop(["permission_id"], inplace=True, axis=1)

        # Generate and return table
        if len(df_gdrive_users) > 0:
            table = dbc.Table.from_dataframe(df_gdrive_users, striped=True, hover=True)
            return table
        else:
            return None
    else:
        raise PreventUpdate


@app.callback(Output("google-drive-user-added", "data"),
              Input("add-user-button", "n_clicks"),
              State("add-user-text-field", "value"),
              State("google-drive-authenticated", "data"), prevent_initial_call=True)
def add_user_to_workspace(button_click, user_email_address, google_drive_is_authenticated):

    """
    Grants user permission to MS-AutoQC workspace in Google Drive
    """

    drive = GoogleDrive(gauth_holder[0])
    db.add_user_to_workspace(drive, user_email_address)

    if user_email_address in db.get_workspace_users_list():
        return user_email_address
    else:
        return "Error"


@app.callback(Output("google-drive-user-deleted", "data"),
              Input("delete-user-button", "n_clicks"),
              State("add-user-text-field", "value"),
              State("google-drive-authenticated", "data"), prevent_initial_call=True)
def delete_user_from_workspace(button_click, user_email_address, google_drive_is_authenticated):

    """
    Revokes user permission to MS-AutoQC workspace in Google Drive
    """

    drive = GoogleDrive(gauth_holder[0])
    db.delete_user_from_workspace(drive, user_email_address)

    if user_email_address in db.get_workspace_users_list():
        return "Error"
    else:
        return user_email_address


@app.callback(Output("user-addition-alert", "is_open"),
              Output("user-addition-alert", "children"),
              Output("user-addition-alert", "color"),
              Input("google-drive-user-added", "data"), prevent_initial_call=True)
def ui_feedback_for_adding_gdrive_user(user_added_result):

    """
    UI alert upon adding a new user to MS-AutoQC workspace
    """

    if user_added_result is not None:
        if user_added_result != "Error" and user_added_result != "User already exists":
            return True, user_added_result + " has been granted access to the workspace.", "success"
        elif user_added_result == "User already exists":
            return True, "Error: " + user_added_result + " already has access to the workspace.", "danger"
        else:
            return True, "Error: cannot grant access.", "danger"


@app.callback(Output("user-deletion-alert", "is_open"),
              Output("user-deletion-alert", "children"),
              Output("user-deletion-alert", "color"),
              Input("google-drive-user-deleted", "data"), prevent_initial_call=True)
def ui_feedback_for_deleting_gdrive_user(user_deleted_result):

    """
    UI alert upon adding a new user to MS-AutoQC workspace
    """

    if user_deleted_result is not None:
        if user_deleted_result != "Error" and user_deleted_result != "User does not exist":
            return True, "Revoked workspace access for " + user_deleted_result + ".", "primary"
        elif user_deleted_result == "User does not exist":
            return True, "Error: " + user_deleted_result + " cannot be deleted because they are not in the workspace.", "danger"
        else:
            return True, "Error: cannot revoke access.", "danger"


@app.callback(Output("chromatography-methods-table", "children"),
              Output("select-istd-chromatography-dropdown", "options"),
              Output("select-bio-chromatography-dropdown", "options"),
              Output("add-chromatography-text-field", "value"),
              Output("chromatography-added", "data"),
              Input("on-page-load", "data"),
              Input("add-chromatography-button", "n_clicks"),
              State("add-chromatography-text-field", "value"),
              Input("istd-msp-added", "data"),
              Input("chromatography-removed", "data"),
              Input("chromatography-msdial-config-added", "data"))
def add_chromatography_method(on_page_load, button_click, chromatography_method, msp_added, method_removed, config_added):

    """
    Add chromatography method to database
    """

    if db.is_valid():

        # Add chromatography method to database
        method_added = ""
        if chromatography_method is not None:
            db.insert_chromatography_method(chromatography_method)
            method_added = "Added"

        # Update table
        df_methods = db.get_chromatography_methods()

        df_methods = df_methods.rename(
            columns={"method_id": "Method ID",
            "num_pos_standards": "Pos (+) Standards",
            "num_neg_standards": "Neg (–) Standards",
            "msdial_config_id": "MS-DIAL Config"})

        df_methods = df_methods[["Method ID", "Pos (+) Standards", "Neg (–) Standards", "MS-DIAL Config"]]

        methods_table = dbc.Table.from_dataframe(df_methods, striped=True, hover=True)

        # Update dropdown
        dropdown_options = []
        for method in df_methods["Method ID"].astype(str).tolist():
            dropdown_options.append({"label": method, "value": method})

        return methods_table, dropdown_options, dropdown_options, None, method_added

    else:
        raise PreventUpdate


@app.callback(Output("chromatography-removed", "data"),
              Input("remove-chromatography-method-button", "n_clicks"),
              State("select-istd-chromatography-dropdown", "value"), prevent_initial_call=True)
def remove_chromatography_method(button_click, chromatography):

    """
    Remove chromatography method from database
    """

    if chromatography is not None:
        db.remove_chromatography_method(chromatography)
        return "Removed"
    else:
        return ""


@app.callback(Output("chromatography-addition-alert", "is_open"),
              Output("chromatography-addition-alert", "children"),
              Input("chromatography-added", "data"))
def show_alert_on_chromatography_addition(chromatography_added):

    """
    UI feedback for adding a chromatography method
    """

    if chromatography_added is not None:
        if chromatography_added == "Added":
            return True, "The chromatography method was added successfully."

    return False, None


@app.callback(Output("chromatography-removal-alert", "is_open"),
              Output("chromatography-removal-alert", "children"),
              Input("chromatography-removed", "data"))
def show_alert_on_chromatography_addition(chromatography_removed):

    """
    UI feedback for removing a chromatography method
    """

    if chromatography_removed is not None:
        if chromatography_removed == "Removed":
            return True, "The selected chromatography method was removed."

    return False, None


@app.callback(Output("msp-save-changes-button", "children"),
              Input("select-istd-chromatography-dropdown", "value"),
              Input("select-istd-polarity-dropdown", "value"))
def add_msp_to_chromatography_button_feedback(chromatography, polarity):

    """
    "Save changes" button UI feedback for Settings > Internal Standards
    """

    if chromatography is not None and polarity is not None:
        return "Add MSP to " + chromatography + " " + polarity
    else:
        return "Add MSP"


@app.callback(Output("add-istd-msp-text-field", "value"),
              Input("add-istd-msp-button", "filename"), prevent_intitial_call=True)
def bio_standard_msp_text_field_feedback(filename):

    """
    UI feedback for selecting an MSP to save for a chromatography method
    """

    return filename


@app.callback(Output("istd-msp-added", "data"),
              Input("msp-save-changes-button", "n_clicks"),
              State("add-istd-msp-button", "contents"),
              State("add-istd-msp-button", "filename"),
              State("select-istd-chromatography-dropdown", "value"),
              State("select-istd-polarity-dropdown", "value"), prevent_initial_call=True)
def capture_uploaded_istd_msp(button_click, contents, filename, chromatography, polarity):

    """
    In Settings > Internal Standards, captures contents of
    uploaded MSP file and calls add_msp_to_database().
    """

    if contents is not None and chromatography is not None and polarity is not None:

        # Decode file contents
        content_type, content_string = contents.split(",")
        decoded = base64.b64decode(content_string)
        file = io.StringIO(decoded.decode("utf-8"))

        # Add identification file to database
        if button_click is not None and chromatography is not None and polarity is not None:
            if filename.endswith(".msp"):
                db.add_msp_to_database(file, chromatography, polarity)  # Parse MSP files
            elif filename.endswith(".csv") or filename.endswith(".txt"):
                db.add_csv_to_database(file, chromatography, polarity)  # Parse CSV files
            return "Success! " + filename + " has been added to " + chromatography + " " + polarity + "."
        else:
            return "Error"

        return "Ready"

    # Update dummy dcc.Store object to update chromatography methods table
    return None


@app.callback(Output("chromatography-msp-success-alert", "is_open"),
              Output("chromatography-msp-success-alert", "children"),
              Output("chromatography-msp-error-alert", "is_open"),
              Output("chromatography-msp-error-alert", "children"),
              Input("istd-msp-added", "data"), prevent_initial_call=True)
def ui_feedback_for_adding_msp_to_chromatography(msp_added):

    """
    UI feedback for adding an MSP to a chromatography method
    """

    if msp_added is not None:
        if "Success" in msp_added:
            return True, msp_added, False, ""
        elif msp_added == "Error":
            return False, "", True, "Error: Please select a chromatography and polarity."
    else:
        return False, "", False, ""


@app.callback(Output("msdial-directory", "value"),
              Input("file-explorer-select-button", "n_clicks"),
              Input("settings-modal", "is_open"),
              State("selected-msdial-folder", "data"))
def get_msdial_directory(select_folder_button, settings_modal_is_open, selected_folder):

    """
    Returns (previously inputted by user) location of MS-DIAL directory
    """

    selected_component = ctx.triggered_id

    if selected_component == "file-explorer-select-button":
        return selected_folder

    if db.is_valid():
        return db.get_msdial_configuration_parameters("Default")[-1]
    else:
        raise PreventUpdate


@app.callback(Output("msdial-config-added", "data"),
              Output("add-msdial-configuration-text-field", "value"),
              Input("add-msdial-configuration-button", "n_clicks"),
              State("add-msdial-configuration-text-field", "value"),
              State("msdial-directory", "value"), prevent_initial_call=True)
def add_msdial_configuration(button_click, msdial_config_id, msdial_directory):

    """
    Adds new MS-DIAL configuration to the database
    """

    if msdial_config_id is not None:
        db.add_msdial_configuration(msdial_config_id, msdial_directory)
        return "Added", None
    else:
        return "", None


@app.callback(Output("msdial-config-removed", "data"),
              Input("remove-config-button", "n_clicks"),
              State("msdial-configs-dropdown", "value"), prevent_initial_call=True)
def delete_msdial_configuration(button_click, msdial_config_id):

    """
    Removes dropdown-selected MS-DIAL configuration from database
    """

    if msdial_config_id is not None:
        if msdial_config_id != "Default":
            db.remove_msdial_configuration(msdial_config_id)
            return "Removed"
        else:
            return "Cannot remove"
    else:
        return ""


@app.callback(Output("msdial-configs-dropdown", "options"),
              Output("msdial-configs-dropdown", "value"),
              Input("on-page-load", "data"),
              Input("msdial-config-added", "data"),
              Input("msdial-config-removed", "data"))
def get_msdial_configs_for_dropdown(on_page_load, on_config_added, on_config_removed):

    """
    Retrieves list of user-created configurations of MS-DIAL parameters from database
    """

    if db.is_valid():

        # Get MS-DIAL configurations from database
        msdial_configurations = db.get_msdial_configurations()

        # Create and return options for dropdown
        config_options = []

        for config in msdial_configurations:
            config_options.append({"label": config, "value": config})

        return config_options, "Default"

    else:
        raise PreventUpdate


@app.callback(Output("msdial-config-addition-alert", "is_open"),
              Output("msdial-config-addition-alert", "children"),
              Output("msdial-config-addition-alert", "color"),
              Input("msdial-config-added", "data"), prevent_initial_call=True)
def show_alert_on_msdial_config_addition(config_added):

    """
    UI feedback on MS-DIAL configuration addition
    """

    if config_added is not None:
        if config_added == "Added":
            return True, "Success! New MS-DIAL configuration added.", "success"

    return False, None, "success"


@app.callback(Output("msdial-config-removal-alert", "is_open"),
              Output("msdial-config-removal-alert", "children"),
              Output("msdial-config-removal-alert", "color"),
              Input("msdial-config-removed", "data"),
              State("msdial-configs-dropdown", "value"), prevent_initial_call=True)
def show_alert_on_msdial_config_removal(config_removed, selected_config):

    """
    UI feedback on MS-DIAL configuration removal
    """

    if config_removed is not None:
        if config_removed == "Removed":
            message = "The selected MS-DIAL configuration was deleted."
            color = "primary"
        if selected_config == "Default":
            message = "Error: The default configuration cannot be deleted."
            color = "danger"
        return True, message, color
    else:
        return False, "", "danger"


@app.callback(Output("retention-time-begin", "value"),
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
              Output("msdial-directory-data", "data"),
              Input("msdial-configs-dropdown", "value"),
              Input("msdial-parameters-saved", "data"),
              Input("msdial-parameters-reset", "data"), prevent_initial_call=True)
def get_msdial_parameters_for_config(msdial_config_id, on_parameters_saved, on_parameters_reset):

    """
    In Settings > MS-DIAL parameters, fills text fields with placeholders
    of current parameter values stored in the database.
    """

    return db.get_msdial_configuration_parameters(msdial_config_id)


@app.callback(Output("msdial-parameters-saved", "data"),
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
              State("msdial-directory", "value"), prevent_initial_call=True)
def write_msdial_parameters_to_database(button_clicks, config_name, rt_begin, rt_end, mz_begin, mz_end,
    ms1_centroid_tolerance, ms2_centroid_tolerance, smoothing_method, smoothing_level, mass_slice_width, min_peak_width,
    min_peak_height, post_id_rt_tolerance, post_id_mz_tolerance, post_id_score_cutoff, alignment_rt_tolerance,
    alignment_mz_tolerance, alignment_rt_factor, alignment_mz_factor, peak_count_filter, qc_at_least_filter, msdial_directory):

    """
    Saves MS-DIAL parameters to respective configuration in database
    """

    db.update_msdial_configuration(config_name, rt_begin, rt_end, mz_begin, mz_end, ms1_centroid_tolerance,
        ms2_centroid_tolerance, smoothing_method, smoothing_level, mass_slice_width, min_peak_width, min_peak_height,
        post_id_rt_tolerance, post_id_mz_tolerance, post_id_score_cutoff, alignment_rt_tolerance, alignment_mz_tolerance,
        alignment_rt_factor, alignment_mz_factor, peak_count_filter, qc_at_least_filter, msdial_directory)

    return "Saved"


@app.callback(Output("msdial-parameters-reset", "data"),
              Input("reset-default-msdial-parameters-button", "n_clicks"),
              State("msdial-configs-dropdown", "value"),
              State("msdial-directory", "value"), prevent_initial_call=True)
def reset_msdial_parameters_to_default(button_clicks, msdial_config_name, msdial_directory):

    """
    Resets parameters for selected MS-DIAL configuration to default settings
    """

    db.update_msdial_configuration(msdial_config_name, 0, 100, 0, 2000, 0.008, 0.01, "LinearWeightedMovingAverage",
        3, 3, 35000, 0.1, 0.3, 0.008, 85, 0.05, 0.008, 0.5, 0.5, 0, "True", msdial_directory)

    return "Reset"


@app.callback(Output("msdial-parameters-success-alert", "is_open"),
              Output("msdial-parameters-success-alert", "children"),
              Input("msdial-parameters-saved", "data"), prevent_initial_call=True)
def show_alert_on_parameter_save(parameters_saved):

    """
    UI feedback for saving changes to MS-DIAL parameters
    """

    if parameters_saved is not None:
        if parameters_saved == "Saved":
            return True, "Your changes were successfully saved."


@app.callback(Output("msdial-parameters-reset-alert", "is_open"),
              Output("msdial-parameters-reset-alert", "children"),
              Input("msdial-parameters-reset", "data"), prevent_initial_call=True)
def show_alert_on_parameter_reset(parameters_reset):

    """
    UI feedback for resetting MS-DIAL parameters in a configuration
    """

    if parameters_reset is not None:
        if parameters_reset == "Reset":
            return True, "Your configuration has been reset to its default settings."


@app.callback(Output("qc-config-added", "data"),
              Output("add-qc-configuration-text-field", "value"),
              Input("add-qc-configuration-button", "n_clicks"),
              State("add-qc-configuration-text-field", "value"), prevent_initial_call=True)
def add_qc_configuration(button_click, qc_config_id):

    """
    Adds new QC configuration to the database
    """

    if qc_config_id is not None:
        db.add_qc_configuration(qc_config_id)
        return "Added", None
    else:
        return "", None


@app.callback(Output("qc-config-removed", "data"),
              Input("remove-qc-config-button", "n_clicks"),
              State("qc-configs-dropdown", "value"), prevent_initial_call=True)
def delete_qc_configuration(button_click, qc_config_id):

    """
    Removes dropdown-selected QC configuration from database
    """

    if qc_config_id is not None:
        if qc_config_id != "Default":
            db.remove_qc_configuration(qc_config_id)
            return "Removed"
        else:
            return "Cannot remove"
    else:
        return ""


@app.callback(Output("qc-configs-dropdown", "options"),
              Output("qc-configs-dropdown", "value"),
              Input("on-page-load", "data"),
              Input("qc-config-added", "data"),
              Input("qc-config-removed", "data"))
def get_qc_configs_for_dropdown(on_page_load, qc_config_added, qc_config_removed):

    """
    Retrieves list of user-created configurations of QC parameters from database
    """

    if db.is_valid():

        # Get QC configurations from database
        qc_configurations = db.get_qc_configurations_list()

        # Create and return options for dropdown
        config_options = []

        for config in qc_configurations:
            config_options.append({"label": config, "value": config})

        return config_options, "Default"

    else:
        raise PreventUpdate


@app.callback(Output("qc-config-addition-alert", "is_open"),
              Output("qc-config-addition-alert", "children"),
              Output("qc-config-addition-alert", "color"),
              Input("qc-config-added", "data"), prevent_initial_call=True)
def show_alert_on_qc_config_addition(config_added):

    """
    UI feedback on QC configuration addition
    """

    if config_added is not None:
        if config_added == "Added":
            return True, "Success! New QC configuration added.", "success"

    return False, None, "success"


@app.callback(Output("qc-config-removal-alert", "is_open"),
              Output("qc-config-removal-alert", "children"),
              Output("qc-config-removal-alert", "color"),
              Input("qc-config-removed", "data"),
              State("qc-configs-dropdown", "value"), prevent_initial_call=True)
def show_alert_on_qc_config_removal(config_removed, selected_config):

    """
    UI feedback on QC configuration removal
    """

    if config_removed is not None:
        if config_removed == "Removed":
            message = "The selected QC configuration was deleted."
            color = "primary"
        if selected_config == "Default":
            message = "Error: The default configuration cannot be deleted."
            color = "danger"
        return True, message, color
    else:
        return False, "", "danger"


@app.callback(Output("intensity-dropouts-cutoff", "value"),
              Output("library-rt-shift-cutoff", "value"),
              Output("in-run-rt-shift-cutoff", "value"),
              Output("library-mz-shift-cutoff", "value"),
              Output("intensity-cutoff-enabled", "value"),
              Output("library-rt-shift-cutoff-enabled", "value"),
              Output("in-run-rt-shift-cutoff-enabled", "value"),
              Output("library-mz-shift-cutoff-enabled", "value"),
              Input("qc-configs-dropdown", "value"),
              Input("qc-parameters-saved", "data"),
              Input("qc-parameters-reset", "data"), prevent_initial_call=True)
def get_qc_parameters_for_config(qc_config_name, on_parameters_saved, on_parameters_reset):

    """
    In Settings > QC Configurations, fills text fields with placeholders
    of current parameter values stored in the database.
    """

    selected_config = db.get_qc_configuration_parameters(config_name=qc_config_name)
    return tuple(selected_config.to_records(index=False)[0])


@app.callback(Output("qc-parameters-saved", "data"),
              Input("save-changes-qc-parameters-button", "n_clicks"),
              State("qc-configs-dropdown", "value"),
              State("intensity-dropouts-cutoff", "value"),
              State("library-rt-shift-cutoff", "value"),
              State("in-run-rt-shift-cutoff", "value"),
              State("library-mz-shift-cutoff", "value"),
              State("intensity-cutoff-enabled", "value"),
              State("library-rt-shift-cutoff-enabled", "value"),
              State("in-run-rt-shift-cutoff-enabled", "value"),
              State("library-mz-shift-cutoff-enabled", "value"), prevent_initial_call=True)
def write_qc_parameters_to_database(button_clicks, qc_config_name, intensity_dropouts_cutoff, library_rt_shift_cutoff,
    in_run_rt_shift_cutoff, library_mz_shift_cutoff, intensity_enabled, library_rt_enabled, in_run_rt_enabled, library_mz_enabled):

    """
    Saves QC parameters to respective configuration in database
    """

    db.update_qc_configuration(qc_config_name, intensity_dropouts_cutoff, library_rt_shift_cutoff, in_run_rt_shift_cutoff,
        library_mz_shift_cutoff, intensity_enabled, library_rt_enabled, in_run_rt_enabled, library_mz_enabled)
    return "Saved"


@app.callback(Output("qc-parameters-reset", "data"),
              Input("reset-default-qc-parameters-button", "n_clicks"),
              State("qc-configs-dropdown", "value"), prevent_initial_call=True)
def reset_msdial_parameters_to_default(button_clicks, qc_config_name):

    """
    Resets parameters for selected QC configuration to default settings
    """

    db.update_qc_configuration(config_name=qc_config_name, intensity_dropouts_cutoff=4,
        library_rt_shift_cutoff=0.1, in_run_rt_shift_cutoff=0.05, library_mz_shift_cutoff=0.005,
        intensity_enabled=True, library_rt_enabled=True, in_run_rt_enabled=True, library_mz_enabled=True)
    return "Reset"


@app.callback(Output("qc-parameters-success-alert", "is_open"),
              Output("qc-parameters-success-alert", "children"),
              Input("qc-parameters-saved", "data"), prevent_initial_call=True)
def show_alert_on_qc_parameter_save(parameters_saved):

    """
    UI feedback for saving changes to QC parameters
    """

    if parameters_saved is not None:
        if parameters_saved == "Saved":
            return True, "Your changes were successfully saved."


@app.callback(Output("qc-parameters-reset-alert", "is_open"),
              Output("qc-parameters-reset-alert", "children"),
              Input("qc-parameters-reset", "data"), prevent_initial_call=True)
def show_alert_on_qc_parameter_reset(parameters_reset):

    """
    UI feedback for resetting QC parameters in a configuration
    """

    if parameters_reset is not None:
        if parameters_reset == "Reset":
            return True, "Your QC configuration has been reset to its default settings."


@app.callback(Output("select-bio-standard-dropdown", "options"),
              Output("biological-standards-table", "children"),
              Input("on-page-load", "data"),
              Input("bio-standard-added", "data"),
              Input("bio-standard-removed", "data"),
              Input("chromatography-added", "data"),
              Input("chromatography-removed", "data"),
              Input("bio-msp-added", "data"),
              Input("bio-standard-msdial-config-added", "data"))
def get_biological_standards(on_page_load, on_standard_added, on_standard_removed, on_method_added, on_method_removed,
    on_msp_added, on_bio_standard_msdial_config_added):

    """
    Populates dropdown and table of biological standards
    """

    if db.is_valid():

        # Populate dropdown
        dropdown_options = []
        for biological_standard in db.get_biological_standards_list():
            dropdown_options.append({"label": biological_standard, "value": biological_standard})

        # Populate table
        df_biological_standards = db.get_biological_standards()

        # DataFrame refactoring
        df_biological_standards = df_biological_standards.rename(
            columns={"name": "Name",
                "identifier": "Identifier",
                "chromatography": "Method ID",
                "num_pos_features": "Pos (+) Metabolites",
                "num_neg_features": "Neg (–) Metabolites",
                "msdial_config_id": "MS-DIAL Config"})

        df_biological_standards = df_biological_standards[
            ["Name", "Identifier", "Method ID", "Pos (+) Metabolites", "Neg (–) Metabolites", "MS-DIAL Config"]]

        biological_standards_table = dbc.Table.from_dataframe(df_biological_standards, striped=True, hover=True)

        return dropdown_options, biological_standards_table

    else:
        raise PreventUpdate


@app.callback(Output("bio-standard-added", "data"),
              Output("add-bio-standard-text-field", "value"),
              Output("add-bio-standard-identifier-text-field", "value"),
              Input("add-bio-standard-button", "n_clicks"),
              State("add-bio-standard-text-field", "value"),
              State("add-bio-standard-identifier-text-field", "value"), prevent_initial_call=True)
def add_biological_standard(button_click, name, identifier):

    """
    Adds biological standard to database
    """

    if name is not None and identifier is not None:

        if len(db.get_chromatography_methods()) == 0:
            return "Error 2", name, identifier
        else:
            db.add_biological_standard(name, identifier)

        return "Added", None, None

    else:
        return "Error 1", name, identifier


@app.callback(Output("bio-standard-removed", "data"),
              Input("remove-bio-standard-button", "n_clicks"),
              State("select-bio-standard-dropdown", "value"), prevent_initial_call=True)
def remove_biological_standard(button_click, biological_standard_name):

    """
    Removes biological standard (and all corresponding MSPs) in the database
    """

    if biological_standard_name is not None:
        db.remove_biological_standard(biological_standard_name)
        return "Deleted " + biological_standard_name + " and all corresponding MSP files."
    else:
        return "Error"


@app.callback(Output("add-bio-msp-text-field", "value"),
              Input("add-bio-msp-button", "filename"), prevent_intitial_call=True)
def bio_standard_msp_text_field_ui_callback(filename):

    """
    UI feedback for selecting an MSP to save for a biological standard
    """

    return filename


@app.callback(Output("bio-msp-added", "data"),
              Input("bio-standard-save-changes-button", "n_clicks"),
              State("add-bio-msp-button", "contents"),
              State("add-bio-msp-button", "filename"),
              State("select-bio-chromatography-dropdown", "value"),
              State("select-bio-polarity-dropdown", "value"),
              State("select-bio-standard-dropdown", "value"), prevent_initial_call=True)
def capture_uploaded_bio_msp(button_click, contents, filename, chromatography, polarity, bio_standard):

    """
    In Settings > Biological Standards, captures contents of uploaded MSP file and calls add_msp_to_database().
    """

    if contents is not None and chromatography is not None and polarity is not None:

        # Decode file contents
        content_type, content_string = contents.split(",")
        decoded = base64.b64decode(content_string)
        file = io.StringIO(decoded.decode("utf-8"))

        # Add MSP file to database
        if button_click is not None and chromatography is not None and polarity is not None and bio_standard is not None:
            if filename.endswith(".msp"):
                db.add_msp_to_database(file, chromatography, polarity, bio_standard=bio_standard)

            # Check whether MSP was added successfully
            if bio_standard in db.get_biological_standards_list():
                return "Success! Added " + filename + " to " + bio_standard + " in " + chromatography + " " + polarity + "."
            else:
                return "Error 1"
        else:
            return "Error 2"

        return "Ready"

    # Update dummy dcc.Store object to update chromatography methods table
    return ""


@app.callback(Output("bio-standard-addition-alert", "is_open"),
              Output("bio-standard-addition-alert", "children"),
              Output("bio-standard-addition-alert", "color"),
              Input("bio-standard-added", "data"), prevent_initial_call=True)
def show_alert_on_bio_standard_addition(bio_standard_added):

    """
    UI feedback for adding a biological standard
    """

    if bio_standard_added is not None:
        if bio_standard_added == "Added":
            return True, "Success! New biological standard added.", "success"
        elif bio_standard_added == "Error 2":
            return True, "Error: Please add a chromatography method first.", "danger"

    return False, None, None


@app.callback(Output("bio-standard-removal-alert", "is_open"),
              Output("bio-standard-removal-alert", "children"),
              Input("bio-standard-removed", "data"), prevent_initial_call=True)
def show_alert_on_bio_standard_removal(bio_standard_removed):

    """
    UI feedback for removing a biological standard
    """

    if bio_standard_removed is not None:
        if "Deleted" in bio_standard_removed:
            return True, bio_standard_removed

    return False, None


@app.callback(Output("bio-msp-success-alert", "is_open"),
              Output("bio-msp-success-alert", "children"),
              Output("bio-msp-error-alert", "is_open"),
              Output("bio-msp-error-alert", "children"),
              Input("bio-msp-added", "data"), prevent_initial_call=True)
def ui_feedback_for_adding_msp_to_bio_standard(bio_standard_msp_added):

    """
    UI feedback for adding an MSP to a biological standard
    """

    if bio_standard_msp_added is not None:
        if "Success" in bio_standard_msp_added:
            return True, bio_standard_msp_added, False, ""
        elif bio_standard_msp_added == "Error 1":
            return False, "", True, "Error: Unable to add MSP to biological standard."
        elif bio_standard_msp_added == "Error 2":
            return False, "", True, "Error: Please select a biological standard, chromatography, and polarity first."
    else:
        return False, "", False, ""


@app.callback(Output("bio-standard-save-changes-button", "children"),
              Input("select-bio-chromatography-dropdown", "value"),
              Input("select-bio-polarity-dropdown", "value"),
              Input("select-bio-standard-dropdown", "value"))
def add_msp_to_bio_standard_button_feedback(chromatography, polarity, bio_standard):

    """
    "Save changes" button UI feedback for Settings > Biological Standards
    """

    if bio_standard is not None and chromatography is not None and polarity is not None:
        return "Add MSP to " + bio_standard + " in " + chromatography + " " + polarity
    elif bio_standard is not None:
        return "Added MSP to " + bio_standard
    else:
        return "Add MSP"


@app.callback(Output("bio-standard-msdial-configs-dropdown", "options"),
              Output("istd-msdial-configs-dropdown", "options"),
              Input("msdial-config-added", "data"),
              Input("msdial-config-removed", "data"))
def populate_msdial_configs_for_biological_standard(msdial_config_added, msdial_config_removed):

    """
    In Settings > Biological Standards, populates the MS-DIAL configurations dropdown
    """

    if db.is_valid():

        options = []

        for config in db.get_msdial_configurations():
            options.append({"label": config, "value": config})

        return options, options

    else:
        raise PreventUpdate


@app.callback(Output("bio-standard-msdial-config-added", "data"),
              Input("bio-standard-msdial-configs-button", "n_clicks"),
              State("select-bio-standard-dropdown", "value"),
              State("select-bio-chromatography-dropdown", "value"),
              State("bio-standard-msdial-configs-dropdown", "value"), prevent_initial_call=True)
def add_msdial_config_for_bio_standard(button_click, biological_standard, chromatography, config_id):

    """
    In Settings > Biological Standards, sets the MS-DIAL configuration to be used for chromatography
    """

    if biological_standard is not None and chromatography is not None and config_id is not None:
        db.update_msdial_config_for_bio_standard(biological_standard, chromatography, config_id)
        return "Added"
    else:
        return ""


@app.callback(Output("bio-config-success-alert", "is_open"),
              Output("bio-config-success-alert", "children"),
              Input("bio-standard-msdial-config-added", "data"),
              State("select-bio-standard-dropdown", "value"),
              State("select-bio-chromatography-dropdown", "value"), prevent_initial_call=True)
def ui_feedback_for_setting_msdial_config_for_bio_standard(config_added, bio_standard, chromatography):

    """
    In Settings > Biological Standards, provides an alert when MS-DIAL config is successfully set for biological standard
    """

    if config_added is not None:
        if config_added == "Added":
            message = "MS-DIAL parameter configuration saved successfully for " + bio_standard + " (" + chromatography + " method)."
            return True, message

    return False, ""


@app.callback(Output("chromatography-msdial-config-added", "data"),
              Input("istd-msdial-configs-button", "n_clicks"),
              State("select-istd-chromatography-dropdown", "value"),
              State("istd-msdial-configs-dropdown", "value"), prevent_initial_call=True)
def add_msdial_config_for_chromatography(button_click, chromatography, config_id):

    """
    In Settings > Internal Standards, sets the MS-DIAL configuration to be used for processing samples
    """

    if chromatography is not None and config_id is not None:
        db.update_msdial_config_for_internal_standards(chromatography, config_id)
        return "Added"
    else:
        return ""


@app.callback(Output("istd-config-success-alert", "is_open"),
              Output("istd-config-success-alert", "children"),
              Input("chromatography-msdial-config-added", "data"),
              State("select-istd-chromatography-dropdown", "value"), prevent_initial_call=True)
def ui_feedback_for_setting_msdial_config_for_chromatography(config_added, chromatography):

    """
    In Settings > Internal Standards, provides an alert when MS-DIAL config is successfully set for a chromatography
    """

    if config_added is not None:
        if config_added == "Added":
            message = "MS-DIAL parameter configuration saved successfully for " + chromatography + "."
            return True, message

    return False, ""


@app.callback(Output("setup-new-run-modal", "is_open"),
              Output("setup-new-run-button", "n_clicks"),
              Output("setup-new-run-modal-title", "children"),
              Input("setup-new-run-button", "n_clicks"),
              Input("start-run-monitor-modal", "is_open"),
              Input("start-bulk-qc-modal", "is_open"),
              State("tabs", "value"),
              Input("data-acquisition-folder-button", "n_clicks"),
              Input("file-explorer-select-button", "n_clicks"),
              State("settings-modal", "is_open"), prevent_initial_call=True)
def toggle_new_run_modal(button_clicks, success, success_2, instrument_name, browse_folder_button, file_explorer_button,
                         settings_modal_is_open):

    """
    Toggles modal for setting up AutoQC monitoring for a new instrument run
    """

    button = ctx.triggered_id

    modal_title = "New AutoQC Job – " + instrument_name

    open_modal = True, 1, modal_title
    close_modal = False, 0, modal_title

    if button == "data-acquisition-folder-button":
        return close_modal

    elif button == "file-explorer-select-button":
        if settings_modal_is_open:
            return close_modal
        else:
            return open_modal

    if success or success_2:
        return close_modal
    elif (not success or not success_2) and button_clicks != 0:
        return open_modal
    else:
        return close_modal


@app.callback(Output("start-run-chromatography-dropdown", "options"),
              Output("start-run-bio-standards-dropdown", "options"),
              Output("start-run-qc-configs-dropdown", "options"),
              Input("setup-new-run-button", "n_clicks"), prevent_initial_call=True)
def populate_options_for_new_run(button_click):

    """
    Populates dropdowns and checklists for Setup New AutoQC Job page
    """

    chromatography_methods = []
    biological_standards = []
    qc_configurations = []

    for method in db.get_chromatography_methods_list():
        chromatography_methods.append({"value": method, "label": method})

    for bio_standard in db.get_biological_standards_list():
        biological_standards.append({"value": bio_standard, "label": bio_standard})

    for qc_configuration in db.get_qc_configurations_list():
        qc_configurations.append({"value": qc_configuration, "label": qc_configuration})

    return chromatography_methods, biological_standards, qc_configurations


@app.callback(Output("sequence-path", "value"),
              Output("new-sequence", "data"),
              Input("sequence-upload-button", "contents"),
              State("sequence-upload-button", "filename"), prevent_initial_call=True)
def capture_uploaded_sequence(contents, filename):

    """
    Converts sequence CSV file to JSON string and stores in dcc.Store object
    """

    # Decode sequence file contents
    content_type, content_string = contents.split(",")
    decoded = base64.b64decode(content_string)
    sequence_file_contents = io.StringIO(decoded.decode("utf-8"))

    # Get sequence file as JSON string
    sequence = qc.convert_sequence_to_json(sequence_file_contents)

    # Update UI and store sequence JSON string
    return filename, sequence


@app.callback(Output("metadata-path", "value"),
              Output("new-metadata", "data"),
              Input("metadata-upload-button", "contents"),
              State("metadata-upload-button", "filename"), prevent_initial_call=True)
def capture_uploaded_metadata(contents, filename):

    """
    Converts metadata CSV file to JSON string and stores in dcc.Store object
    """

    # Decode metadata file contents
    content_type, content_string = contents.split(",")
    decoded = base64.b64decode(content_string)
    metadata_file_contents = io.StringIO(decoded.decode("utf-8"))

    # Get metadata file as JSON string
    metadata = qc.convert_metadata_to_json(metadata_file_contents)

    # Update UI and store metadata JSON string
    return filename, metadata


@app.callback(Output("monitor-new-run-button", "children"),
              Output("data-acquisition-path-title", "children"),
              Output("data-acquisition-path-form-text", "children"),
              Input("autoqc-job-type", "value"), prevent_initial_call=True)
def update_new_job_button_text(job_type):

    """
    Updates New AutoQC Job form submit button based on job type
    """

    if job_type == "active":
        button_text = "Start monitoring instrument run"
        text_field_title = "Data acquisition path"
        form_text = "Please enter the folder path to which incoming raw data files will be saved."
    elif job_type == "completed":
        button_text = "Start QC processing data files"
        text_field_title = "Data file path"
        form_text = "Please enter the folder path where your data files are saved."

    return button_text, text_field_title, form_text


@app.callback(Output("instrument-run-id", "valid"),
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
              State("tabs", "value"), prevent_initial_call=True)
def validation_feedback_for_new_run_setup_form(run_id, chromatography, qc_config, sequence_contents, sequence_filename,
    metadata_contents, metadata_filename, data_acquisition_path, run_id_valid, run_id_invalid, chromatography_valid,
    chromatography_invalid, qc_config_valid, qc_config_invalid, sequence_valid, sequence_invalid, metadata_valid,
    metadata_invalid, path_valid, path_invalid, instrument):

    """
    Extensive form validation and feedback for setting up a new MS-AutoQC job
    """

    # Instrument run ID validation
    if run_id is not None:

        # Get run ID's for instrument
        run_ids = db.get_instrument_runs(instrument)["run_id"].astype(str).tolist()

        # Check if run ID is unique
        if run_id not in run_ids:
            run_id_valid, run_id_invalid = True, False
        else:
            run_id_valid, run_id_invalid = False, True

    # Chromatography validation
    if chromatography is not None:
        if qc.chromatography_is_valid(chromatography):
            chromatography_valid, chromatography_invalid = True, False
        else:
            chromatography_valid, chromatography_invalid = False, True

    # QC configuration validation
    if qc_config is not None:
        qc_config_valid = True

    # Instrument sequence file validation
    if sequence_contents is not None:

        content_type, content_string = sequence_contents.split(",")
        decoded = base64.b64decode(content_string)
        sequence_contents = io.StringIO(decoded.decode("utf-8"))

        if qc.sequence_is_valid(sequence_filename, sequence_contents):
            sequence_valid, sequence_invalid = True, False
        else:
            sequence_valid, sequence_invalid = False, True

    # Metadata file validation
    if metadata_contents is not None:

        content_type, content_string = metadata_contents.split(",")
        decoded = base64.b64decode(content_string)
        metadata_contents = io.StringIO(decoded.decode("utf-8"))

        if qc.metadata_is_valid(metadata_filename, metadata_contents):
            metadata_valid, metadata_invalid = True, False
        else:
            metadata_valid, metadata_invalid = False, True

    # Validate that data acquisition path exists
    if data_acquisition_path is not None:
        if os.path.exists(data_acquisition_path):
            path_valid, path_invalid = True, False
        else:
            path_valid, path_invalid = False, True

    return run_id_valid, run_id_invalid, chromatography_valid, chromatography_invalid, qc_config_valid, qc_config_invalid, \
           sequence_valid, sequence_invalid, metadata_valid, metadata_invalid, path_valid, path_invalid


@app.callback(Output("monitor-new-run-button", "disabled"),
              Input("instrument-run-id", "valid"),
              Input("start-run-chromatography-dropdown", "valid"),
              Input("start-run-qc-configs-dropdown", "valid"),
              Input("sequence-path", "valid"),
              Input("metadata-path", "valid"),
              Input("data-acquisition-folder-path", "valid"), prevent_initial_call=True)
def enable_new_autoqc_job_button(run_id_valid, chromatography_valid, qc_config_valid, sequence_valid, metadata_valid, path_valid):

    """
    Enables "submit" button for New AutoQC Job form
    """

    if run_id_valid and chromatography_valid and qc_config_valid and sequence_valid and metadata_valid and path_valid:
        return False
    else:
        return True


@app.callback(Output("start-run-monitor-modal", "is_open"),
              Output("start-bulk-qc-modal", "is_open"),
              Output("new-job-error-modal", "is_open"),
              Output("filenames-for-bulk-qc", "data"),
              Input("monitor-new-run-button", "n_clicks"),
              State("instrument-run-id", "value"),
              State("tabs", "value"),
              State("start-run-chromatography-dropdown", "value"),
              State("start-run-bio-standards-dropdown", "value"),
              State("new-sequence", "data"),
              State("new-metadata", "data"),
              State("data-acquisition-folder-path", "value"),
              State("start-run-qc-configs-dropdown", "value"),
              State("autoqc-job-type", "value"), prevent_initial_call=True)
def new_autoqc_job_setup(button_clicks, run_id, instrument_id, chromatography, bio_standards, sequence, metadata,
    acquisition_path, qc_config_id, job_type):

    """
    This callback initiates the following:
    1. Writing a new instrument run to the database
    2. Generate parameters files for MS-DIAL processing
    3a. Initializing run monitoring at the given directory for an active run, or
    3b. Iterating through and processing data files for a completed run
    """

    # Write a new instrument run to the database
    db.insert_new_run(run_id, instrument_id, chromatography, bio_standards, sequence, metadata, qc_config_id)

    # Get MSPs and generate parameters files for MS-DIAL processing
    for polarity in ["Positive", "Negative"]:

        # Generate parameters files for processing samples
        msp_file_path = db.get_msp_file_path(chromatography, polarity)
        db.generate_msdial_parameters_file(chromatography, polarity, msp_file_path)

        # Generate parameters files for processing each biological standard
        if bio_standards is not None:
            for bio_standard in bio_standards:
                msp_file_path = db.get_msp_file_path(chromatography, polarity, bio_standard)
                db.generate_msdial_parameters_file(chromatography, polarity, msp_file_path, bio_standard)

    # Get filenames from sequence and filter out preblanks, wash, shutdown, etc.
    filenames = db.get_filenames_from_sequence(sequence)["File Name"].astype(str).tolist()

    # If this is for an active run, initialize run monitoring at the given directory
    if job_type == "active":
        listener = subprocess.Popen(["py", "AcquisitionListener.py", acquisition_path, str(filenames), run_id])
        return True, False, False, ""

    # If this is for a completed run, begin iterating through the files and process them
    elif job_type == "completed":
        return False, True, False, json.dumps(filenames)

    # Handle form validation errors
    else:
        return False, False, True, ""


@app.callback(Output("bulk-qc-progress-bar", "value"),
              Output("bulk-qc-progress-bar", "label"),
              Output("start-bulk-qc-modal-title", "children"),
              Output("progress-interval", "disabled"),
              Input("start-bulk-qc-modal", "is_open"),
              Input("progress-interval", "n_intervals"),
              State("data-acquisition-folder-path", "value"),
              State("filenames-for-bulk-qc", "data"),
              State("instrument-run-id", "value"),
              State("start-bulk-qc-modal-title", "children"), prevent_initial_call=True)
def start_bulk_qc_processing(modal_open, progress_intervals, data_file_directory, filenames_as_json, run_id, title):

    """
    Initiates bulk QC processing:
    1. Iterates through data files in a given directory
    2. Processes them and writes results to database
    3. Updates progress bar
    """

    if modal_open:
        # Get filenames as list from JSON string
        filenames = json.loads(filenames_as_json)

        # Replace any backwards slashes in path
        path = data_file_directory.replace("\\", "/")
        if not path.endswith("/"):
            path = path + "/"

        # Iterate through files using index from progress bar callback loop
        if title != "Processing data files...":
            index = int(title.split(" out of ")[0])
        else:
            index = 0

        # Once the last file has been processed, terminate the job
        if index == len(filenames):
            if db.sync_is_enabled():
                db.sync_to_google_drive(drive=GoogleDrive(gauth_holder[0]), sync_settings=False)
            return 100, "100%", "Processing complete!", True

        filename = filenames[index]

        # Prepare update of progress bar
        progress = int(min(((index + 1) / len(filenames)) * 100, 100))
        progress_label = str(progress) + "%"
        new_title = str(index + 1) + " out of " + str(len(filenames)) + " samples processed"

        # If the file has already been processed, restart the loop
        df_samples = db.get_samples_in_run(run_id)
        df_sample = df_samples.loc[df_samples["sample_id"] == filename]
        if df_sample["qc_result"].astype(str).values[0] != "None":
            return progress, progress_label, new_title, False

        # Otherwise, process the data file
        if os.path.exists(path + filename + ".raw"):
            qc.process_data_file(path=path, filename=filename, extension="raw", run_id=run_id)
        return progress, progress_label, new_title, False

    else:
        return 0, "", "", False


@app.callback(Output("file-explorer-modal", "is_open"),
              Input("data-acquisition-folder-button", "n_clicks"),
              Input("file-explorer-select-button", "n_clicks"),
              State("setup-new-run-modal", "is_open"),
              Input("msdial-folder-button", "n_clicks"), prevent_initial_call=True)
def open_file_explorer(new_job_browse_folder_button, select_folder_button, new_run_modal_is_open, msdial_select_folder_button):

    """
    Opens custom file explorer modal
    """

    button = ctx.triggered_id

    if button == "msdial-folder-button" or button == "data-acquisition-folder-button":
        return True
    elif button == "file-explorer-select-button":
        return False
    else:
        raise PreventUpdate


@app.callback(Output("file-explorer-modal-body", "children"),
              Input("file-explorer-modal", "is_open"),
              Input("selected-data-folder", "data"),
              Input("selected-msdial-folder", "data"),
              State("settings-modal", "is_open"), prevent_initial_call=True)
def list_directories_in_file_explorer(file_explorer_is_open, selected_data_folder, selected_msdial_folder, settings_is_open):

    """
    Lists directories for a user to select in the file explorer modal
    """

    if file_explorer_is_open:

        link_components = []
        start_folder = None

        if not settings_is_open and selected_data_folder is not None:
            start_folder = selected_data_folder
        elif settings_is_open and selected_msdial_folder is not None:
            start_folder = selected_msdial_folder

        if start_folder is None:
            if sys.platform == "win32":
                start_folder = "C:/Users/"
            elif sys.platform == "darwin":
                start_folder = "/Users/"

        folders = [f.path for f in os.scandir(start_folder) if f.is_dir()]

        if len(folders) > 0:
            for index, folder in enumerate(folders):
                link = html.A(folder, href="#", id="dir-" + str(index + 1))
                link_components.append(link)
                link_components.append(html.Br())

            for index in range(len(folders), 30):
                link_components.append(html.A("", id="dir-" + str(index + 1)))
        else:
            link_components = []

        return link_components

    else:
        raise PreventUpdate


@app.callback(Output("selected-data-folder", "data"),
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
              State("settings-modal", "is_open"), prevent_initial_call=True)
def the_most_inefficient_callback_in_history(com_1, com_2, com_3, com_4, com_5, com_6, com_7, com_8, com_9, com_10,
    com_11, com_12, com_13, com_14, com_15, com_16, com_17, com_18, com_19, com_20, com_21, com_22, com_23, com_24,
    com_25, com_26, com_27, com_28, com_29, com_30, dir_1, dir_2, dir_3, dir_4, dir_5, dir_6, dir_7, dir_8, dir_9, dir_10,
    dir_11, dir_12, dir_13, dir_14, dir_15, dir_16, dir_17, dir_18, dir_19, dir_20, dir_21, dir_22, dir_23, dir_24,
    dir_25, dir_26, dir_27, dir_28, dir_29, dir_30, selected_data_folder, selected_msdial_folder, back_button, settings_is_open):

    """
    Handles user selection of folder in the file explorer modal (I'm sorry)
    """

    if settings_is_open:
        selected_folder = selected_msdial_folder
    else:
        selected_folder = selected_data_folder

    if selected_folder is None:

        if sys.platform == "win32":
            start = "C:/Users/"
        elif sys.platform == "darwin":
            start = "/Users/"

        if settings_is_open:
            return None, start
        else:
            return start, None

    # Get <a> component that triggered callback
    selected_component = ctx.triggered_id

    if selected_component == "file-explorer-back-button":
        last_folder = "/" + selected_folder.split("/")[-1]
        previous = selected_folder.replace(last_folder, "")

        if settings_is_open:
            return None, previous
        else:
            return previous, None

    # Create a dictionary with all link components and their values
    components = ("dir-1", "dir-2", "dir-3", "dir-4", "dir-5", "dir-6", "dir-7", "dir-8", "dir-9", "dir-10", "dir-11", "dir-12",
        "dir-13", "dir-14", "dir-15", "dir-16", "dir-17", "dir-18", "dir-19", "dir-20", "dir-21", "dir-22", "dir-23", "dir-24",
        "dir-25", "dir-26", "dir-27", "dir-28", "dir-29", "dir-30")
    values = (dir_1, dir_2, dir_3, dir_4, dir_5, dir_6, dir_7, dir_8, dir_9, dir_10, dir_11, dir_12, dir_13, dir_14, dir_15,
        dir_16, dir_17, dir_18, dir_19, dir_20, dir_21, dir_22, dir_23, dir_24, dir_25, dir_26, dir_27, dir_28, dir_29, dir_30)
    folders = {components[i]: values[i] for i in range(len(components))}

    # Append to selected folder path by indexing set
    selected_folder = folders[selected_component]

    # Return selected folder and all folder values
    if settings_is_open:
        return None, selected_folder
    else:
        return selected_folder, None


@app.callback(Output("file-explorer-modal-title", "children"),
              Input("selected-data-folder", "data"),
              Input("selected-msdial-folder", "data"),
              State("settings-modal", "is_open"), prevent_initial_call=True)
def update_file_explorer_title(selected_data_folder, selected_msdial_folder, settings_is_open):

    """
    Populates data acquisition path text field with user selection
    """

    if not settings_is_open:
        return selected_data_folder
    elif settings_is_open:
        return selected_msdial_folder
    else:
        raise PreventUpdate


@app.callback(Output("data-acquisition-folder-path", "value"),
              Input("file-explorer-select-button", "n_clicks"),
              State("selected-data-folder", "data"),
              State("settings-modal", "is_open"), prevent_initial_call=True)
def update_folder_path_text_field(select_folder_button, selected_folder, settings_is_open):

    """
    Populates data acquisition path text field with user selection
    """

    if not settings_is_open:
        return selected_folder


if __name__ == "__main__":

    # if sys.platform == "win32":
    #     chrome_path = "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe"
    #     webbrowser.register("chrome", None, webbrowser.BackgroundBrowser(chrome_path))
    #     webbrowser.get("chrome").open("http://127.0.0.1:8050/")
    # elif sys.platform == "darwin":
    #     webbrowser.get("chrome").open("http://127.0.0.1:8050/", new=1)

    # Start Dash app
    app.run_server(threaded=False, debug=False, port=8050)