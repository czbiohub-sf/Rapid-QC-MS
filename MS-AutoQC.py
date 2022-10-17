import io, sys, subprocess, time
import base64, webbrowser, json
import pandas as pd
import sqlalchemy as sa
from dash import dash, dcc, html, dash_table, Input, Output, State
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

bootstrap_colors = {
    "blue": "rgb(0, 123, 255)",
    "red": "rgb(220, 53, 69)",
    "green": "rgb(40, 167, 69)",
    "yellow": "rgb(255, 193, 7)",
    "blue-low-opacity": "rgba(0, 123, 255, 0.4)",
    "red-low-opacity": "rgba(220, 53, 69, 0.4)",
    "green-low-opacity": "rgba(40, 167, 69, 0.4)",
    "yellow-low-opacity": "rgba(255, 193, 7, 0.4)"
}

# Initialize Dash app
app = dash.Dash(__name__, external_stylesheets=[local_stylesheet, dbc.themes.BOOTSTRAP],
                meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}])
app.title = "MS-AutoQC"

# Create Dash app layout
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
                        dbc.NavItem(dbc.NavLink("About", href="#", id="about-button", className="navbar-button")),
                        dbc.NavItem(dbc.NavLink("Help", href="#", id="help-button", className="navbar-button")),
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
                                        {"if": {"column_id": "Study"},
                                        "width": "50%"},
                                        {"if": {"column_id": "Type"},
                                            "width": "25%"},
                                        {"if": {"column_id": "Status"},
                                        "width": "25%"}
                                    ]
                                ),

                                # Button to start monitoring a new run
                                html.Div(className="d-grid gap-2", children=[
                                    dbc.Button("Setup New AutoQC Job",
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
                                                {"label": "Positive Mode", "value": "pos"},
                                                {"label": "Negative Mode", "value": "neg"}],
                                            value="pos"
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
                                            options=[
                                                {"label": "All", "value": "all"},
                                                {"label": "Samples", "value": "samples"},
                                                {"label": "Pools", "value": "pools"},
                                                {"label": "Blanks", "value": "blanks"}],
                                            value="all"
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
                                        "width": "65%"},
                                        {"if": {"column_id": "Position"},
                                        "width": "20%"},
                                        {"if": {"column_id": "QC"},
                                        "width": "15%"},
                                    ]
                                )
                            ]),
                        ]),

                        dbc.Col(width=12, lg=8, children=[

                            # Container for all plots
                            html.Div(id="plot-container", className="all-plots-container", style={"display": "none"}, children=[

                                html.Div(className="istd-plot-div", children=[

                                    html.Div(className="plot-container", children=[

                                        # Dropdown for selecting an internal standard for the RT vs. sample plot
                                        dcc.Dropdown(
                                            id="istd-rt-dropdown",
                                            options=standards_list,
                                            placeholder="Select internal standards...",
                                            style={"text-align": "left",
                                                   "height": "35px",
                                                   "width": "100%",
                                                   "display": "inline-block"}
                                        ),

                                        # Dropdown for filtering by sample for the RT vs. sample plot
                                        dcc.Dropdown(
                                            id="rt-plot-sample-dropdown",
                                            options=[],
                                            placeholder="Select samples...",
                                            style={"text-align": "left",
                                                   "height": "35px",
                                                   "width": "100%",
                                                   "display": "inline-block"},
                                            multi=True),

                                        # Scatter plot of internal standard retention times in QE 1 samples
                                        dcc.Graph(id="istd-rt-plot")
                                    ]),

                                    html.Div(className="plot-container", children=[

                                        # Dropdown for internal standard intensity plot
                                        dcc.Dropdown(
                                            id="istd-intensity-dropdown",
                                            options=standards_list,
                                            placeholder="Select internal standard...",
                                            style={"text-align": "left",
                                                   "height": "35px",
                                                   "width": "100%",
                                                   "display": "inline-block"}),

                                        # Dropdown for filtering by sample for the intensity vs. sample plot
                                        dcc.Dropdown(
                                            id="intensity-plot-sample-dropdown",
                                            options=[],
                                            placeholder="Select samples...",
                                            style={"text-align": "left",
                                                   "height": "35px",
                                                   "width": "100%",
                                                   "display": "inline-block"},
                                            multi=True),

                                        # Bar plot of internal standard intensity in QE 1 samples
                                        dcc.Graph(id="istd-intensity-plot")
                                    ]),

                                    html.Div(className="plot-container", children=[

                                        # Dropdown for internal standard delta m/z plot
                                        dcc.Dropdown(
                                            id="istd-mz-dropdown",
                                            options=standards_list,
                                            placeholder="Select internal standards...",
                                            style={"text-align": "left",
                                                   "height": "35px",
                                                   "width": "100%",
                                                   "display": "inline-block"},
                                        ),

                                        # Dropdown for filtering by sample for the delta m/z vs. sample plot
                                        dcc.Dropdown(
                                            id="mz-plot-sample-dropdown",
                                            options=[],
                                            placeholder="Select samples...",
                                            style={"text-align": "left",
                                                   "height": "35px",
                                                   "width": "100%",
                                                   "display": "inline-block"},
                                            multi=True),

                                        # Scatter plot of internal standard delta m/z vs. samples
                                        dcc.Graph(id="istd-mz-plot")
                                    ]),

                                ]),

                                html.Div(className="urine-plot-div", children=[

                                    # Scatter plot of QC urine feature retention times from QE 1
                                    html.Div(className="plot-container", children=[
                                        dcc.Graph(id="urine-rt-plot")
                                    ]),

                                    # Bar plot of QC urine feature peak heights from QE 1
                                    html.Div(className="plot-container", children=[

                                        # Dropdown for urine feature intensity plot
                                        dcc.Dropdown(
                                            id="urine-intensity-dropdown",
                                            options=list(get_pos_urine_features_dict().keys()),
                                            placeholder="Select urine feature...",
                                            style={"text-align": "left",
                                                   "height": "35px",
                                                   "width": "100%",
                                                   "display": "inline-block"}
                                        ),

                                        dcc.Graph(id="urine-intensity-plot", animate=False)
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

                        # Modal for setting up a new user or new instrument
                        dbc.Modal(id="setup-user-modal", size="lg", centered=True, is_open=False, scrollable=True,
                                  keyboard=False, backdrop="static", children=[
                            dbc.ModalHeader(dbc.ModalTitle("Welcome", id="setup-user-modal-title"), close_button=False),
                            dbc.ModalBody(id="setup-user-modal-body", className="modal-styles-2", children=[

                                html.Div([
                                    dbc.Label(
                                        "Looks like this is your first time using MS-AutoQC on this device. Let's help you get started."),
                                    dbc.Accordion(start_collapsed=True, children=[

                                        # First time setup screen
                                        dbc.AccordionItem(title="I'm setting up MS-AutoQC for the first time", children=[
                                            html.Div([
                                                dbc.Label("Instrument name"),
                                                dbc.Input(placeholder="Ex: Thermo Q-Exactive HF", type="text"),
                                                dbc.FormText("Please enter a unique name for this instrument."),
                                            ]), html.Br(),

                                            html.Div([
                                                dbc.Label("Add chromatography"),
                                                dbc.Input(placeholder="Ex: HILIC, RP (12 mins), RP (30 mins), etc.", type="text"),
                                                dbc.FormText(
                                                    "Please enter the name of one chromatography method you would like to " +
                                                    "add internal standards for. You can specify internal standards for more " +
                                                    "chromatographies later."),
                                            ]), html.Br(),

                                            html.Div([
                                                dbc.Label("Positive (+) mode internal standards"),
                                                dbc.InputGroup([
                                                    dbc.Input(placeholder="No MSP file selected",
                                                        id="setup-add-pos-istd-msp-text-field"),
                                                    dbc.Button("Browse Files", color="secondary",
                                                        id="setup-add-pos-istd-msp-button", n_clicks=0),
                                                ]),
                                                dbc.FormText(
                                                    "Please ensure that each internal standard has a name, m/z, RT, and MS/MS spectrum."),
                                            ]), html.Br(),

                                            html.Div([
                                                dbc.Label("Add negative (–) mode internal standards"),
                                                dbc.InputGroup([
                                                    dbc.Input(placeholder="No MSP file selected",
                                                        id="setup-add-neg-istd-msp-text-field"),
                                                    dbc.Button("Browse Files", color="secondary",
                                                        id="setup-add-neg-istd-msp-button", n_clicks=0),
                                                ]),
                                                dbc.FormText(
                                                    "Please ensure that each internal standard has a name, m/z, RT, and MS/MS spectrum."),
                                            ]), html.Br(),

                                            html.Div([
                                                dbc.Label("Add biological standard (optional)"),
                                                dbc.Input(placeholder="Ex: Human urine", type="text"),
                                                dbc.FormText(
                                                    "Please enter the name of one biological standard you would like to track " +
                                                    "instrument performance with. You can add more later."),
                                            ]), html.Br(),

                                            html.Div([
                                                dbc.Label(
                                                    "Positive (+) mode targeted features (for biological standard)"),
                                                dbc.InputGroup([
                                                    dbc.Input(placeholder="No MSP file selected",
                                                        id="setup-add-pos-istd-msp-text-field-2"),
                                                    dbc.Button("Browse Files", color="secondary", outline=False,
                                                        id="setup-add-pos-istd-msp-button-2", n_clicks=0),
                                                ]),
                                                dbc.FormText(
                                                    "If you are adding a biological standard, please ensure that " +
                                                    "each targeted feature has a name, m/z, RT, and MS/MS spectrum."),
                                            ]), html.Br(),

                                            html.Div([
                                                dbc.Label(
                                                    "Negative (–) mode targeted features (for biological standard)"),
                                                dbc.InputGroup([
                                                    dbc.Input(placeholder="No MSP file selected",
                                                        id="setup-add-neg-istd-msp-text-field-2"),
                                                    dbc.Button("Browse Files", color="secondary", outline=False,
                                                        id="setup-add-neg-istd-msp-button-2", n_clicks=0),
                                                ]),
                                                dbc.FormText(
                                                    "If you are adding a biological standard, please ensure that " +
                                                    "each targeted feature has a name, m/z, RT, and MS/MS spectrum."),
                                            ]), html.Br(),

                                            html.Div([
                                                dbc.Label("Enable cloud sync (recommended)"), html.Br(),
                                                dbc.Button("Sign in to Google Drive", id="setup-google-drive-sync-button",
                                                    color="primary", outline=False), html.Br(),
                                                dbc.FormText(
                                                    "This will allow you to monitor your instrument runs on other devices."),
                                            ]), html.Br(),

                                            html.Div([
                                                html.Div([
                                                    dbc.Button("Complete setup", style={"line-height": "1.75"}, color="success"),
                                                ], className="d-grid gap-2 col-12 mx-auto"),
                                            ])
                                        ]),

                                        # Adding an instrument to the workspace
                                        dbc.AccordionItem(title="I'm adding a new instrument to my workspace", children=[
                                            html.Div([
                                                dbc.Label("Instrument name"),
                                                dbc.Input(placeholder="Ex: Thermo Q-Exactive HF", type="text"),
                                                dbc.FormText("Please enter a unique name for this instrument."),
                                            ]), html.Br(),

                                            html.Div([
                                                dbc.Label("Sync with Google Drive"), html.Br(),
                                                dbc.Button("Sign in to Google Drive", id="setup-google-drive-sync-button-2",
                                                   color="primary", outline=False), html.Br(),
                                                dbc.FormText(
                                                    "Please ensure that your Google account has been registered to access " +
                                                    "your MS-AutoQC workspace by visiting Settings > General on the instrument computer."),
                                            ]), html.Br(),

                                            html.Div([
                                                html.Div([
                                                    dbc.Button("Add instrument to workspace",
                                                        style={"line-height": "1.75"}, color="success"),
                                                ], className="d-grid gap-2 col-12 mx-auto"),
                                            ])
                                        ]),

                                        # Signing in from another device
                                        dbc.AccordionItem(title="I'm trying to view my run from another computer", children=[
                                            html.Div([
                                                dbc.Label("Sign in to access MS-AutoQC"), html.Br(),
                                                dbc.Button("Sign in to Google Drive", id="setup-google-drive-sync-button-3",
                                                    color="primary", outline=False), html.Br(),
                                                dbc.FormText(
                                                    "Please ensure that your Google account has been registered to access " +
                                                    "your MS-AutoQC workspace by visiting Settings > General on the instrument computer."),
                                            ]), html.Br(),

                                            html.Div([
                                                html.Div([
                                                    dbc.Button("Sign in to MS-AutoQC workspace",
                                                        style={"line-height": "1.75"}, color="success"),
                                                ], className="d-grid gap-2 col-12 mx-auto"),
                                            ])
                                        ]),
                                    ]),
                                ]), html.Br(),
                            ])
                        ]),

                        # Modal for starting an instrument run listener
                        dbc.Modal(id="setup-new-run-modal", size="lg", centered=True, is_open=False, scrollable=True, children=[
                            dbc.ModalHeader(dbc.ModalTitle(id="setup-new-run-modal-title", children="Setup New AutoQC Job"), close_button=True),
                            dbc.ModalBody(id="setup-new-run-modal-body", className="modal-styles-2", children=[

                                # Text field for entering your run ID
                                html.Div([
                                    dbc.Label("Instrument run ID"),
                                    dbc.Input(id="instrument-run-id", placeholder="NEW_RUN_001", type="text"),
                                    dbc.FormText("Please enter a unique ID for this run."),
                                ]),

                                html.Br(),

                                # Select chromatography
                                html.Div([
                                    dbc.Label("Select chromatography"),
                                    dbc.Select(id="start-run-chromatography-dropdown",
                                               placeholder="No chromatography selected"),
                                ]),

                                html.Br(),

                                # Select biological standard used in this study
                                html.Div(children=[
                                    dbc.Label("Select biological standards (optional)"),
                                    dbc.Checklist(id="start-run-bio-standards-checklist"),
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
                                        # dbc.DropdownMenu([
                                        #     dbc.DropdownMenuItem("Thermo Xcalibur"),
                                        #     dbc.DropdownMenuItem("Agilent MassHunter")],
                                        #     id="vendor-software", label="Vendor", color="secondary"),
                                        dbc.Input(id="sequence-path",
                                                  placeholder="No file selected"),
                                        dbc.Button(dcc.Upload(
                                            id="sequence-upload-button",
                                            children=[html.A("Browse Files")]),
                                            color="secondary"),
                                    ]),
                                    dbc.FormText(
                                        "Please ensure you have selected the correct vendor software for this sequence."),
                                    dbc.FormFeedback("Looks good!", type="valid"),
                                    dbc.FormFeedback(
                                        "Please ensure you have selected the correct vendor software for this sequence.", type="invalid"),
                                ]),

                                html.Br(),

                                # Button and field for selecting a sample metadata file
                                html.Div([
                                    dbc.Label("Sample metadata (.csv)"),
                                    dbc.InputGroup([
                                        dbc.Input(id="metadata-path",
                                                  placeholder="No file selected"),
                                        dbc.Button(dcc.Upload(
                                            id="metadata-upload-button",
                                            children=[html.A("Browse Files")]),
                                            color="secondary"),
                                    ]),
                                    dbc.FormText("Please ensure you have the following columns: " +
                                                 "Sample Name, Species, Matrix, Treatment, and Conditions"),
                                    dbc.FormFeedback("Looks good!", type="valid"),
                                    dbc.FormFeedback("Please make sure your metadata file has the required columns.", type="invalid"),
                                ]),

                                html.Br(),

                                # Button and field for selecting the data acquisition directory
                                html.Div([
                                    dbc.Label("Data file directory"),
                                    dbc.InputGroup([
                                        dbc.Input(placeholder="C:/Users/Data/NEW_RUN_001",
                                                  id="data-acquisition-folder-path"),
                                    ]),
                                    dbc.FormText("Please type the folder path to which incoming data files will be saved."),
                                ]),

                                html.Br(),

                                # Switch between running AutoQC on a live run vs. past completed run
                                html.Div(children=[
                                    dbc.Label("Is this an active or completed instrument run?"),
                                    dbc.RadioItems(id="autoqc-job-type", value="active", options=[
                                        {"label": "I want to monitor and QC an active instrument run",
                                         "value": "active"},
                                        {"label": "I want to QC a completed instrument run",
                                         "value": "completed"}],
                                    ),
                                ]),

                                html.Br(),

                                html.Div([
                                    dbc.Button("Start monitoring this run", id="monitor-new-run-button",
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

                                        # Google Drive cloud storage
                                        html.Br(),
                                        dbc.Label("Cloud sync"),
                                        html.Br(),
                                        dbc.Button("Sync with Google Drive", id="google-drive-sync-button",
                                                   color="primary", outline=False),
                                        html.Br(),
                                        dbc.FormText("This will allow you to monitor your instrument runs on other devices."),
                                        html.Br(), html.Br(),

                                        # Google Drive sharing
                                        dbc.Label("Add new users"),
                                        html.Br(),
                                        dbc.InputGroup([
                                            dbc.Input(placeholder="example@gmail.com",
                                                      id="add-user-text-field"),
                                            dbc.Button("Add user", color="primary", outline=True,
                                                       id="add-user-button", n_clicks=0),
                                        ]),
                                        dbc.FormText(
                                            "Adding new users grants full read-and-write access to MS-AutoQC."),
                                        html.Br(),

                                        # Table of registered instruments
                                        dbc.Table(),

                                        # Slack notifications
                                        html.Br(),
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
                                        html.Br(),

                                        # Table of registered users
                                        dbc.Table()
                                    ]),

                                    # Internal standards
                                    dbc.Tab(label="Internal standards", className="modal-styles", children=[

                                        html.Br(),

                                        # Alerts for user feedback on biological standard addition/removal
                                        dbc.Alert(id="chromatography-addition-alert", color="success", is_open=False, duration=4000),
                                        dbc.Alert(id="chromatography-removal-alert", color="primary", is_open=False, duration=4000),

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

                                        dbc.Alert(id="istd-config-success-alert", color="success", is_open=False, duration=4000),

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

                                        # UI feedback on adding MSP to chromatography method
                                        dbc.Alert(id="chromatography-msp-success-alert", color="success", is_open=False, duration=4000),
                                        dbc.Alert(id="chromatography-msp-error-alert", color="danger", is_open=False, duration=4000),

                                        html.Div(id="chromatography-methods-table"),

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
                                        dbc.Alert(id="bio-standard-addition-alert", color="success", is_open=False, duration=4000),
                                        dbc.Alert(id="bio-standard-removal-alert", color="primary", is_open=False, duration=4000),

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

                                        html.Div([
                                            dbc.Label("Select biological standard"),
                                            dbc.InputGroup([
                                                dbc.Select(id="select-bio-standard-dropdown",
                                                           placeholder="No biological standard selected"),
                                                dbc.Button("Remove", color="danger", outline=True,
                                                           id="remove-bio-standard-button", n_clicks=0),
                                                dbc.Popover("You are about to delete this biological standard and "
                                                            "all of its corresponding MSP files. Are you sure?",
                                                            target="remove-bio-standard-button", trigger="hover", body=True)
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

                                        dbc.Alert(id="bio-config-success-alert", color="success", is_open=False, duration=4000),

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

                                        html.Div([
                                            dbc.Label("Edit targeted feature list (MSP format)"),
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

                                        # UI feedback on adding MSP to biological standard
                                        dbc.Alert(id="bio-msp-success-alert", color="success", is_open=False, duration=4000),
                                        dbc.Alert(id="bio-msp-error-alert", color="danger", is_open=False, duration=4000),

                                        html.Div(id="biological-standards-table"),

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
                                        dbc.Alert(id="qc-config-addition-alert", is_open=False, duration=4000),
                                        dbc.Alert(id="qc-config-removal-alert", is_open=False, duration=4000),

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

                                        html.Div([
                                            dbc.Label("Cutoff for intensity dropouts"),
                                            dbc.Input(id="intensity-dropout-text-field", type="number",
                                                      placeholder="4"),
                                            dbc.FormText("The minimum number of missing internal standards required for a QC fail."),
                                        ]),

                                        html.Br(),

                                        html.Div([
                                            dbc.Label("Cutoff for RT shift from run average (min)"),
                                            dbc.Input(id="run-rt-shift-text-field",
                                                      placeholder="0.1"),
                                            dbc.FormText(
                                                "The minimum retention time shift from the run average (in minutes) required for a QC fail."),
                                        ]),

                                        html.Br(),

                                        html.Div([
                                            dbc.Label("Allowed number of samples where delta RT is increasing"),
                                            dbc.Input(id="allowed-delta-rt-trends-text-field", type="number",
                                                      placeholder="3"),
                                            dbc.FormText(
                                                "If the delta RT is growing in X consecutive samples, you will be sent a warning."),
                                        ]),

                                        html.Br(),

                                        # UI feedback on saving changes to MS-DIAL parameters
                                        dbc.Alert(id="qc-parameters-success-alert",
                                                  color="success", is_open=False, duration=4000),
                                        dbc.Alert(id="qc-parameters-reset-alert",
                                                  color="primary", is_open=False, duration=4000),
                                        dbc.Alert(id="qc-parameters-error-alert",
                                                  color="danger", is_open=False, duration=4000),

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
                                        dbc.Alert(id="msdial-config-addition-alert", is_open=False, duration=4000),
                                        dbc.Alert(id="msdial-config-removal-alert", is_open=False, duration=4000),

                                        # Button and field for selecting the data acquisition directory
                                        html.Div([
                                            dbc.Label("MS-DIAL folder"),
                                            dbc.InputGroup([
                                                dbc.Input(placeholder="C:/Users/Me/Downloads/MS-DIAL",
                                                          id="msdial-directory"),
                                            ]),
                                            dbc.FormText(
                                                "Please enter the full path of your downloaded MS-DIAL folder."),
                                        ]),

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
                                                      color="success", is_open=False, duration=4000),
                                            dbc.Alert(id="msdial-parameters-reset-alert",
                                                      color="primary", is_open=False, duration=4000),
                                            dbc.Alert(id="msdial-parameters-error-alert",
                                                      color="danger", is_open=False, duration=4000),
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

            # Storage of all DataFrames necessary for QC plot generation
            dcc.Store(id="rt-pos"),
            dcc.Store(id="rt-neg"),
            dcc.Store(id="intensity-pos"),
            dcc.Store(id="intensity-neg"),
            dcc.Store(id="mz-pos"),
            dcc.Store(id="mz-neg"),
            dcc.Store(id="sequence"),
            dcc.Store(id="metadata"),
            dcc.Store(id="urine-rt-pos"),
            dcc.Store(id="urine-rt-neg"),
            dcc.Store(id="urine-intensity-pos"),
            dcc.Store(id="urine-intensity-neg"),
            dcc.Store(id="urine-mz-pos"),
            dcc.Store(id="urine-mz-neg"),
            dcc.Store(id="study-resources"),
            dcc.Store(id="samples"),
            dcc.Store(id="instruments"),
            dcc.Store(id="first-study"),

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
        ])
    ])

app.layout = serve_layout

@app.callback(Output("tabs", "children"),
              Output("tabs", "value"),
              Input("instruments", "data"))
def get_instrument_tabs(instruments):

    """
    Retrieves all instruments on a user installation of MS-AutoQC
    """

    instrument_list = db.get_instruments_list()

    # Create tabs for each instrument
    instrument_tabs = []
    for instrument in instrument_list:
        instrument_tabs.append(dcc.Tab(label=instrument, value=instrument))

    return instrument_tabs, instrument_list[0]


@app.callback(Output("instrument-run-table", "active_cell"),
              Output("instrument-run-table", "selected_cells"),
              Input("instrument-run-table", "data"),
              State("instrument-run-table", "active_cell"),
              State("first-study", "data"), prevent_initial_call=True)
def reset_instrument_table(table_data, active_cell, first_study):

    """
    Removes selected cell highlight upon tab switch to different instrument
    (A case study in insane side missions during frontend development)
    """

    if active_cell:
        if first_study != table_data[active_cell["row"]][active_cell["column_id"]]:
            return None, []
    else:
        return None, []


@app.callback(Output("instrument-run-table", "data"),
              Output("first-study", "data"),
              Output("table-container", "style"),
              Output("plot-container", "style"),
              Input("header", "children"),
              Input("tabs", "value"), suppress_callback_exceptions=True)
def populate_study_table(placeholder_input, instrument):

    """
    Dash callback for populating tables with list of past/active instrument runs
    """

    df_studies = pd.DataFrame()
    df_metadata = pd.DataFrame()

    studies = {
        "Study": [],
        "Type": [],
        "Status": []
    }

    files = drive.ListFile({"q": "'" + drive_ids[instrument] + "' in parents and trashed=false"}).GetList()

    # Get study name and chromatography
    for file in files:
        if "RT" in file["title"] and ("Pos" in file["title"] or "Neg" in file["title"]) and "urine" not in file["title"]:
            if file["title"].split("_")[0] not in studies["Study"]:
                studies["Study"].append(file["title"].split("_")[0])
                studies["Type"].append(file["title"].split("_")[2])
                studies["Status"].append("Complete")

    df_studies["Study"] = studies["Study"]
    df_studies["Type"] = studies["Type"]
    df_studies["Status"] = studies["Status"]
    studies = df_studies.to_dict("records")

    display_div = {"display": "block"}

    return studies, df_studies["Study"].tolist()[0], display_div, display_div


@app.callback(Output("rt-pos", "data"),
              Output("rt-neg", "data"),
              Output("intensity-pos", "data"),
              Output("intensity-neg", "data"),
              Output("mz-pos", "data"),
              Output("mz-neg", "data"),
              Output("sequence", "data"),
              Output("metadata", "data"),
              Output("urine-rt-pos", "data"),
              Output("urine-rt-neg", "data"),
              Output("urine-intensity-pos", "data"),
              Output("urine-intensity-neg", "data"),
              Output("urine-mz-pos", "data"),
              Output("urine-mz-neg", "data"),
              Output("study-resources", "data"),
              Output("samples", "data"),
              Input("instrument-run-table", "active_cell"),
              State("instrument-run-table", "data"),
              State("tabs", "value"), prevent_initial_call=True, suppress_callback_exceptions=True)
def load_data(active_cell, table_data, instrument):

    """
    Stores QC results for QE 1 in dcc.Store objects (user's browser session)
    """

    no_data = (None, None, None, None, None, None, None, None,
               None, None, None, None, None, None, None, None)

    if active_cell:
        study_id = table_data[active_cell["row"]][active_cell["column_id"]]
        return get_data(instrument, study_id)
    else:
        return no_data


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

    study_name = ""

    if active_cell:
        if study_resources:
            study_name = json.loads(study_resources)["study_name"]
            if table_data[active_cell["row"]][active_cell["column_id"]] != study_name:
                study_name = table_data[active_cell["row"]][active_cell["column_id"]]
        else:
            study_name = table_data[active_cell["row"]][active_cell["column_id"]]

        if modal_is_open:
            return False, None, None

        title = html.Div([
            html.Div(children=[dbc.Spinner(color="primary"), " Loading QC results for " + study_name])
        ])

        body = "This may take a few seconds..."

        return True, title, body

    else:
        return False, None, None


@app.callback(Output("sample-table", "data"),
              Input("samples", "data"), prevent_initial_call=True)
def populate_sample_tables(samples):

    """
    Populates table with list of samples for selected study from QE 1 instrument table
    """

    if samples is not None:
        df_samples = pd.read_json(samples, orient="split")
        return df_samples.to_dict("records")
    else:
        return None


@app.callback(Output("istd-rt-dropdown", "options"),
              Output("istd-mz-dropdown", "options"),
              Output("istd-intensity-dropdown", "options"),
              Output("urine-intensity-dropdown", "options"),
              Output("rt-plot-sample-dropdown", "options"),
              Output("mz-plot-sample-dropdown", "options"),
              Output("intensity-plot-sample-dropdown", "options"),
              Input("polarity-options", "value"),
              Input("sample-table", "data"),
              State("study-resources", "data"),
              State("samples", "data"), prevent_initial_call=True)
def update_dropdowns_on_polarity_change(polarity, table_data, study_resources, samples):

    """
    Updates QE 1 dropdown lists with correct items for user-selected polarity
    """

    if samples is not None:
        df_samples = pd.read_json(samples, orient="split")
        study_resources = json.loads(study_resources)

        if polarity == "neg":
            istd_dropdown = study_resources["neg_internal_standards"]
            urine_dropdown = list(neg_urine_features_dict.keys())
            df_samples = df_samples.loc[df_samples["Sample"].str.contains("Neg")]
            sample_dropdown = df_samples["Sample"].tolist()

        elif polarity == "pos":
            istd_dropdown = study_resources["pos_internal_standards"]
            urine_dropdown = list(get_pos_urine_features_dict().keys())
            df_samples = df_samples.loc[df_samples["Sample"].str.contains("Pos")]
            sample_dropdown = df_samples["Sample"].tolist()

        return istd_dropdown, istd_dropdown, istd_dropdown, urine_dropdown, sample_dropdown, sample_dropdown, sample_dropdown

    else:
        return [], [], [], [], [], [], []


@app.callback(Output("rt-plot-sample-dropdown", "value"),
              Output("mz-plot-sample-dropdown", "value"),
              Output("intensity-plot-sample-dropdown", "value"),
              Input("sample-filtering-options", "value"),
              Input("polarity-options", "value"),
              State("samples", "data"),
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

    # Hmmm...
    if polarity == "pos":
        polarity = "Pos"
    elif polarity == "neg":
        polarity = "Neg"

    # Get complete list of samples (including blanks + pools) in polarity
    df_samples = pd.read_json(samples, orient="split")
    df_samples = df_samples.loc[df_samples["Sample"].str.contains(polarity)]
    sample_list = df_samples["Sample"].tolist()

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


@app.callback(Output("istd-rt-plot", "figure"),
              Output("istd-intensity-plot", "figure"),
              Output("istd-mz-plot", "figure"),
              Output("urine-rt-plot", "figure"),
              Output("urine-intensity-plot", "figure"),
              Output("urine-intensity-dropdown", "value"),
              Output("urine-rt-plot", "clickData"),
              Input("polarity-options", "value"),
              Input("istd-rt-dropdown", "value"),
              Input("istd-intensity-dropdown", "value"),
              Input("istd-mz-dropdown", "value"),
              Input("urine-intensity-dropdown", "value"),
              Input("urine-rt-plot", "clickData"),
              Input("rt-plot-sample-dropdown", "value"),
              Input("intensity-plot-sample-dropdown", "value"),
              Input("mz-plot-sample-dropdown", "value"),
              State("rt-pos", "data"),
              State("rt-neg", "data"),
              State("intensity-pos", "data"),
              State("intensity-neg", "data"),
              State("mz-pos", "data"),
              State("mz-neg", "data"),
              State("sequence", "data"),
              State("metadata", "data"),
              State("urine-rt-pos", "data"),
              State("urine-rt-neg", "data"),
              State("urine-intensity-pos", "data"),
              State("urine-intensity-neg", "data"),
              State("urine-mz-pos", "data"),
              State("urine-mz-neg", "data"),
              Input("study-resources", "data"),
              State("samples", "data"),
              Input("tabs", "value"), prevent_initial_call=True)
def populate_plots(polarity, rt_plot_standard, intensity_plot_standard, mz_plot_standard,
                   urine_plot_feature, click_data, rt_plot_samples, intensity_plot_samples, mz_plot_samples,
                   rt_pos, rt_neg, intensity_pos, intensity_neg, mz_pos, mz_neg, sequence,
                   metadata, urine_rt_pos, urine_rt_neg, urine_intensity_pos, urine_intensity_neg,
                   urine_mz_pos, urine_mz_neg, study_resources, samples, instrument):

    """
    Dash callback for loading QE 1 instrument data into scatter and bar plots
    """

    if rt_pos is None:
        return dash.no_update, dash.no_update, dash.no_update, \
               dash.no_update, dash.no_update, None, None

    # Retrieve data for clicked study and store as a dictionary
    files = {
        "rt_pos": pd.read_json(rt_pos, orient="split"),
        "rt_neg": pd.read_json(rt_neg, orient="split"),
        "intensity_pos": pd.read_json(intensity_pos, orient="split"),
        "intensity_neg": pd.read_json(intensity_neg, orient="split"),
        "mz_pos": pd.read_json(mz_pos, orient="split"),
        "mz_neg": pd.read_json(mz_neg, orient="split"),
        "sequence": pd.read_json(sequence, orient="split"),
        "metadata": pd.read_json(metadata, orient="split"),
        "urine_rt_pos": pd.read_json(urine_rt_pos, orient="split"),
        "urine_rt_neg": pd.read_json(urine_rt_neg, orient="split"),
        "urine_intensity_pos": pd.read_json(urine_intensity_pos, orient="split"),
        "urine_intensity_neg": pd.read_json(urine_intensity_neg, orient="split"),
        "urine_mz_pos": pd.read_json(urine_mz_pos, orient="split"),
        "urine_mz_neg": pd.read_json(urine_mz_neg, orient="split"),
        "study_resources": json.loads(study_resources),
        "samples": pd.read_json(samples, orient="split")
    }

    if files["study_resources"]["instrument"] is not None:
        if files["study_resources"]["instrument"] != instrument:
            return {}, {}, {}, {}, {}, None, None

    # Get study name
    study_name = files["study_resources"]["study_name"]

    # Get retention times
    retention_times_dict = files["study_resources"]["retention_times_dict"]

    # Get metadata DataFrame
    df_metadata = files["metadata"]

    # Get internal standards from QC DataFrames for RT scatter plot
    if polarity == "pos":
        internal_standards = files["rt_pos"]["Title"].astype(str).tolist()
        urine_features_dict = get_pos_urine_features_dict()
    elif polarity == "neg":
        internal_standards = files["rt_neg"]["Title"].astype(str).tolist()
        urine_features_dict = get_neg_urine_features_dict()

    # Set initial dropdown values when none are selected
    if not rt_plot_standard:
        rt_plot_standard = internal_standards[0]

    if not intensity_plot_standard:
        intensity_plot_standard = internal_standards[0]

    if not mz_plot_standard:
        mz_plot_standard = internal_standards[0]

    # Get clicked or selected feature from QC urine m/z-RT plot
    if not urine_plot_feature:
        urine_plot_feature = list(urine_features_dict.keys())[0]

    if click_data:
        urine_plot_feature = click_data["points"][0]["hovertext"]

    # Prepare DataFrames for plotting
    df_istd_rt = files["rt_" + polarity]
    df_istd_intensity = files["intensity_" + polarity]
    df_istd_mz = files["mz_" + polarity]
    df_urine_rt = files["urine_rt_" + polarity]
    df_urine_intensity = files["urine_intensity_" + polarity]
    df_urine_mz = files["urine_mz_" + polarity]

    # Transpose DataFrames
    df_istd_rt = df_istd_rt.transpose()
    df_istd_intensity = df_istd_intensity.transpose()
    df_istd_mz = df_istd_mz.transpose()
    df_urine_intensity = df_urine_intensity.transpose()

    for dataframe in [df_istd_rt, df_istd_intensity, df_istd_mz, df_urine_intensity]:
        dataframe.columns = dataframe.iloc[0]
        dataframe.drop(dataframe.index[0], inplace=True)

    # Split text in internal standard dataframes
    for istd in internal_standards:

        # Splitting text for RT data
        rt = df_istd_rt[istd].str.split(": ").str[0]
        rt_diff = df_istd_rt[istd].str.split(": ").str[1]
        df_istd_rt[istd] = rt.astype(float)

        # Splitting text for m/z data
        mz = df_istd_mz[istd].str.split(": ").str[0]
        delta_mz = df_istd_mz[istd].str.split(": ").str[1]
        df_istd_mz[istd] = delta_mz.astype(float)

    # Get list of samples from transposed DataFrames
    samples = df_istd_rt.index.values.tolist()
    samples = [sample.replace(": RT Info", "") for sample in samples]

    if not rt_plot_samples:
        rt_plot_samples = samples

    if not intensity_plot_samples:
        intensity_plot_samples = samples
        treatments = []
    else:
        df_metadata = df_metadata.loc[df_metadata["Filename"].isin(intensity_plot_samples)]
        df_metadata = df_metadata.sort_values(by=["Treatment"])
        treatments = df_metadata["Treatment"].tolist()
        if len(df_metadata) == len(intensity_plot_samples):
            intensity_plot_samples = df_metadata["Filename"].tolist()

    if not mz_plot_samples:
        mz_plot_samples = samples

    try:
        # Internal standards – retention time vs. sample
        istd_rt_plot = load_istd_rt_plot(dataframe=df_istd_rt,
                                         samples=rt_plot_samples,
                                         internal_standard=rt_plot_standard,
                                         retention_times_dict=retention_times_dict)

        # Internal standards – intensity vs. sample
        istd_intensity_plot = load_istd_intensity_plot(dataframe=df_istd_intensity,
                                                      samples=intensity_plot_samples,
                                                      internal_standard=intensity_plot_standard,
                                                      text=intensity_plot_samples,
                                                      treatments=treatments)

        # Internal standards – delta m/z vs. sample
        istd_delta_mz_plot = load_istd_delta_mz_plot(dataframe=df_istd_mz,
                                                     samples=mz_plot_samples,
                                                     internal_standard=mz_plot_standard)

        # Urine features – retention time vs. feature
        urine_feature_plot = load_urine_feature_plot(study_name=study_name,
                                                     df_rt=df_urine_rt,
                                                     df_mz=df_urine_mz,
                                                     df_intensity=files["urine_intensity_" + polarity],
                                                     urine_features_dict=urine_features_dict)

        # Urine features – intensity vs. feature
        urine_benchmark_plot = load_urine_benchmark_plot(dataframe=df_urine_intensity,
                                                         study=df_urine_intensity.index,
                                                         feature_name=urine_plot_feature,
                                                         polarity=polarity)

        return istd_rt_plot, istd_intensity_plot, istd_delta_mz_plot, \
               urine_feature_plot, urine_benchmark_plot, urine_plot_feature, None

    except Exception as error:
        print(error)
        return {}, {}, {}, {}, {}, None, None


@app.callback(Output("sample-info-modal", "is_open"),
              Output("sample-modal-title", "children"),
              Output("sample-modal-body", "children"),
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
              State("rt-pos", "data"),
              State("rt-neg", "data"),
              State("intensity-pos", "data"),
              State("intensity-neg", "data"),
              State("mz-pos", "data"),
              State("mz-neg", "data"),
              State("sequence", "data"),
              State("metadata", "data"), prevent_initial_call=True)
def toggle_sample_card(is_open, active_cell, table_data, rt_click, intensity_click, mz_click,
                       rt_pos, rt_neg, intensity_pos, intensity_neg, mz_pos, mz_neg, sequence, metadata):

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

    df_sequence = pd.read_json(sequence, orient="split")
    df_metadata = pd.read_json(metadata, orient="split")

    if "pos" in clicked_sample.lower():
        polarity = "pos"
    elif "neg" in clicked_sample.lower():
        polarity = "neg"

    # Generate DataFrames with iSTD and metadata info for selected sample
    if polarity == "pos":

        df_rt_pos = pd.read_json(rt_pos, orient="split")
        df_intensity_pos = pd.read_json(intensity_pos, orient="split")
        df_mz_pos = pd.read_json(mz_pos, orient="split")

        df_sample_istd, df_sample_info = generate_sample_metadata_dataframe(clicked_sample, df_rt_pos,
                                                df_mz_pos, df_intensity_pos, df_sequence, df_metadata)

    elif polarity == "neg":

        df_rt_neg = pd.read_json(rt_neg, orient="split")
        df_intensity_neg = pd.read_json(intensity_neg, orient="split")
        df_mz_neg = pd.read_json(mz_neg, orient="split")

        df_sample_istd, df_sample_info = generate_sample_metadata_dataframe(clicked_sample, df_rt_neg,
                                                df_mz_neg, df_intensity_neg, df_sequence, df_metadata)

    # Create tables from DataFrames
    metadata_table = dbc.Table.from_dataframe(df_sample_info, striped=True, bordered=True, hover=True)
    istd_table = dbc.Table.from_dataframe(df_sample_istd, striped=True, bordered=True, hover=True)

    # Add tables to sample information modal
    title = clicked_sample
    body = html.Div(children=[metadata_table, istd_table])

    # Toggle modal
    if is_open:
        return False, title, body, None, None, None, None
    else:
        return True, title, body, None, None, None, None


@app.callback(Output("setup-new-run-modal", "is_open"),
              Output("setup-new-run-button", "n_clicks"),
              Input("setup-new-run-button", "n_clicks"),
              Input("start-run-monitor-modal", "is_open"),
              Input("start-bulk-qc-modal", "is_open"), prevent_initial_call=True)
def toggle_new_run_modal(button_clicks, success, success_2):

    """
    Toggles modal for setting up AutoQC monitoring for a new instrument run
    """

    if success or success_2:
        return False, 0
    elif (not success or not success_2) and button_clicks != 0:
        return True, 1
    else:
        return False, 0


@app.callback(Output("settings-modal", "is_open"),
              Input("settings-button", "n_clicks"), prevent_initial_call=True)
def toggle_settings_modal(button_click):

    """
    Toggles global settings modal
    """

    return True


@app.callback(Output("chromatography-methods-table", "children"),
              Output("select-istd-chromatography-dropdown", "options"),
              Output("select-bio-chromatography-dropdown", "options"),
              Output("add-chromatography-text-field", "value"),
              Output("chromatography-added", "data"),
              Input("on-page-load", "data"),
              Input("add-chromatography-button", "n_clicks"),
              State("add-chromatography-text-field", "value"),
              Input("istd-msp-added", "data"),
              Input("chromatography-removed", "data"))
def add_chromatography_method(on_page_load, button_click, chromatography_method, msp_added, method_removed):

    """
    Add chromatography method to database
    """

    # Add chromatography method to database
    method_added = ""
    if chromatography_method is not None:
        db.insert_chromatography_method(chromatography_method)
        method_added = "Added"

    # Update table
    df_methods = db.get_chromatography_methods()

    df_methods = df_methods.rename(
        columns={"method_id": "Method ID",
        "num_pos_standards": "Positive (+) Mode Standards",
        "num_neg_standards": "Negative (–) Mode Standards"})

    df_methods = df_methods[["Method ID", "Positive (+) Mode Standards", "Negative (–) Mode Standards"]]

    methods_table = dbc.Table.from_dataframe(df_methods, striped=True, hover=True)

    # Update dropdown
    dropdown_options = []
    for method in df_methods["Method ID"].astype(str).tolist():
        dropdown_options.append({"label": method, "value": method})

    return methods_table, dropdown_options, dropdown_options, None, method_added


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
        if msp_added == "Added":
            return True, "Success! Your MSP was added to the selected chromatography.", False, ""
        elif msp_added == "Error":
            return False, "", True, "Error: Unable to add MSP to chromatography."
    else:
        return False, "", False, ""


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
        if button_click is not None:
            if filename.endswith(".msp"):
                db.add_msp_to_database(file, chromatography, polarity)  # Parse MSP files
            elif filename.endswith(".csv") or filename.endswith(".txt"):
                db.add_csv_to_database(file, chromatography, polarity)  # Parse CSV files
            return "Added"

        return "Ready"

    # Update dummy dcc.Store object to update chromatography methods table
    return ""


@app.callback(Output("msdial-directory", "value"),
              Input("on-page-load", "data"))
def get_msdial_directory(on_page_load):

    """
    Returns (previously inputted by user) location of MS-DIAL directory
    """

    return db.get_msdial_configuration_parameters("Default configuration")[-1]


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
        if msdial_config_id != "Default configuration":
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

    # Get MS-DIAL configurations from database
    msdial_configurations = db.get_msdial_configurations()

    # Create and return options for dropdown
    config_options = []

    for config in msdial_configurations:
        config_options.append({"label": config, "value": config})

    return config_options, "Default configuration"


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
            message = "The selected MS-DIAL configuration has been deleted."
            color = "primary"
        if selected_config == "Default configuration":
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
        if qc_config_id != "Default configuration":
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

    # Get QC configurations from database
    qc_configurations = db.get_qc_configurations_list()

    # Create and return options for dropdown
    config_options = []

    for config in qc_configurations:
        config_options.append({"label": config, "value": config})

    return config_options, "Default configuration"


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
            message = "The selected QC configuration has been deleted."
            color = "primary"
        if selected_config == "Default configuration":
            message = "Error: The default configuration cannot be deleted."
            color = "danger"
        return True, message, color
    else:
        return False, "", "danger"


@app.callback(Output("intensity-dropout-text-field", "value"),
              Output("run-rt-shift-text-field", "value"),
              Output("allowed-delta-rt-trends-text-field", "value"),
              Input("qc-configs-dropdown", "value"),
              Input("qc-parameters-saved", "data"),
              Input("qc-parameters-reset", "data"), prevent_initial_call=True)
def get_qc_parameters_for_config(qc_config_id, on_parameters_saved, on_parameters_reset):

    """
    In Settings > QC Configurations, fills text fields with placeholders
    of current parameter values stored in the database.
    """

    return db.get_qc_configuration_parameters(qc_config_id)


@app.callback(Output("qc-parameters-saved", "data"),
              Input("save-changes-qc-parameters-button", "n_clicks"),
              State("qc-configs-dropdown", "value"),
              State("intensity-dropout-text-field", "value"),
              State("run-rt-shift-text-field", "value"),
              State("allowed-delta-rt-trends-text-field", "value"), prevent_initial_call=True)
def write_qc_parameters_to_database(button_clicks, config_name, intensity_dropouts_cutoff, max_rt_shift, allowed_delta_rt_trends):

    """
    Saves QC parameters to respective configuration in database
    """

    db.update_qc_configuration(config_name, intensity_dropouts_cutoff, max_rt_shift, allowed_delta_rt_trends)
    return "Saved"


@app.callback(Output("qc-parameters-reset", "data"),
              Input("reset-default-qc-parameters-button", "n_clicks"),
              State("qc-configs-dropdown", "value"), prevent_initial_call=True)
def reset_msdial_parameters_to_default(button_clicks, config_name):

    """
    Resets parameters for selected QC configuration to default settings
    """

    db.update_qc_configuration(config_name, 4, 0.1, 3)
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
              Input("bio-msp-added", "data"))
def get_biological_standards(on_page_load, on_standard_added, on_standard_removed, on_method_added, on_method_removed, on_msp_added):

    """
    Populates dropdown and table of biological standards
    """

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
            "chromatography": "Chromatography",
            "num_pos_features": "Pos (+) Features",
            "num_neg_features": "Neg (–) Features"})

    df_biological_standards = df_biological_standards[
        ["Name", "Identifier", "Chromatography", "Pos (+) Features", "Neg (–) Features"]]

    biological_standards_table = dbc.Table.from_dataframe(df_biological_standards, striped=True, hover=True)

    return dropdown_options, biological_standards_table


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
        db.add_biological_standard(name, identifier)
        return "Added", None, None
    else:
        return "Error", name, identifier


@app.callback(Output("bio-standard-removed", "data"),
              Input("remove-bio-standard-button", "n_clicks"),
              State("select-bio-standard-dropdown", "value"), prevent_initial_call=True)
def remove_biological_standard(button_click, biological_standard_name):

    """
    Removes biological standard (and all corresponding MSPs) in the database
    """

    if biological_standard_name is not None:
        db.remove_biological_standard(biological_standard_name)
        return "Removed"
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
        if button_click is not None:
            if filename.endswith(".msp"):
                db.add_msp_to_database(file, chromatography, polarity, bio_standard=bio_standard)

            # Check whether MSP was added successfully
            if bio_standard in db.get_biological_standards_list():
                return "Added"
            else:
                return "Error"

        return "Ready"

    # Update dummy dcc.Store object to update chromatography methods table
    return ""


@app.callback(Output("bio-standard-addition-alert", "is_open"),
              Output("bio-standard-addition-alert", "children"),
              Input("bio-standard-added", "data"), prevent_initial_call=True)
def show_alert_on_bio_standard_addition(bio_standard_added):

    """
    UI feedback for adding a biological standard
    """

    if bio_standard_added is not None:
        if bio_standard_added == "Added":
            return True, "Success! New biological standard added."

    return False, None


@app.callback(Output("bio-standard-removal-alert", "is_open"),
              Output("bio-standard-removal-alert", "children"),
              Input("bio-standard-removed", "data"), prevent_initial_call=True)
def show_alert_on_bio_standard_removal(bio_standard_removed):

    """
    UI feedback for removing a biological standard
    """

    if bio_standard_removed is not None:
        if bio_standard_removed == "Removed":
            return True, "The selected biological standard has been deleted."

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
        if bio_standard_msp_added == "Added":
            return True, "Success! Your MSP was added to the biological standard.", False, ""
        elif bio_standard_msp_added == "Error":
            return False, "", True, "Error: Unable to add MSP to biological standard."
    else:
        return False, "", False, ""


@app.callback(Output("bio-standard-save-changes-button", "children"),
              Input("select-bio-chromatography-dropdown", "value"),
              Input("select-bio-polarity-dropdown", "value"))
def add_msp_to_bio_standard_button_feedback(chromatography, polarity):

    """
    "Save changes" button UI feedback for Settings > Biological Standards
    """

    if chromatography is not None and polarity is not None:
        return "Add MSP to " + chromatography + " " + polarity
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

    options = []

    for config in db.get_msdial_configurations():
        options.append({"label": config, "value": config})

    return options, options


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


@app.callback(Output("start-run-chromatography-dropdown", "options"),
              Output("start-run-bio-standards-checklist", "options"),
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


@app.callback(Output("instrument-run-id", "valid"),
              Output("start-run-chromatography-dropdown", "valid"),
              Output("start-run-qc-configs-dropdown", "valid"),
              Output("sequence-path", "valid"),
              Output("metadata-path", "valid"),
              Output("data-acquisition-folder-path", "valid"),
              Input("instrument-run-id", "value"),
              Input("start-run-chromatography-dropdown", "value"),
              Input("start-run-qc-configs-dropdown", "value"),
              Input("sequence-path", "value"),
              Input("metadata-path", "value"),
              Input("data-acquisition-folder-path", "value"),
              State("instrument-run-id", "valid"),
              State("start-run-chromatography-dropdown", "valid"),
              State("start-run-qc-configs-dropdown", "valid"),
              State("sequence-path", "valid"),
              State("metadata-path", "valid"),
              State("data-acquisition-folder-path", "valid"), prevent_initial_call=True)
def validation_feedback_for_new_run_setup_form(run_id, chromatography, qc_config, sequence, metadata, data_acquisition_path,
    run_id_valid, chromatography_valid, qc_config_valid, sequence_valid, metadata_valid, data_acquisition_path_valid):

    """
    Form validation feedback for the Setup New AutoQC Job page
    """

    if run_id is not None:
        run_id_valid = True

    if chromatography is not None:
        chromatography_valid = True

    if qc_config is not None:
        qc_config_valid = True

    if sequence is not None:
        sequence_valid = True

    if metadata is not None:
        metadata_valid = True

    if data_acquisition_path is not None:
        data_acquisition_path_valid = True

    return run_id_valid, chromatography_valid, qc_config_valid, sequence_valid, metadata_valid, data_acquisition_path_valid


@app.callback(Output("start-run-monitor-modal", "is_open"),
              Output("start-bulk-qc-modal", "is_open"),
              Output("new-job-error-modal", "is_open"),
              Output("filenames-for-bulk-qc", "data"),
              Input("monitor-new-run-button", "n_clicks"),
              State("instrument-run-id", "value"),
              State("tabs", "value"),
              State("start-run-chromatography-dropdown", "value"),
              State("start-run-bio-standards-checklist", "value"),
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
        msp_file_path = db.get_msp_file_paths(chromatography, polarity)
        db.generate_msdial_parameters_file(chromatography, polarity, msp_file_path)

        # Generate parameters files for processing each biological standard
        for bio_standard in bio_standards:
            msp_file_path = db.get_msp_file_paths(chromatography, polarity, bio_standard)
            db.generate_msdial_parameters_file(chromatography, polarity, msp_file_path, bio_standard)

    # Get filenames from sequence and filter out preblanks, wash, shutdown, etc.
    filenames = qc.get_filenames_from_sequence(sequence)

    for filename in filenames.copy():
        if "_BK_" and "_pre_" in filename:
            filenames.remove(filename)
        elif "wash" in filename:
            filenames.remove(filename)
        elif "shutdown" in filename:
            filenames.remove(filename)

    # If this is for an active run, initialize run monitoring at the given directory
    if job_type == "active":
        listener = subprocess.Popen(["python", "AcquisitionListener.py", acquisition_path, str(filenames), run_id])
        return True, False, False, ""

    # If this is for a completed run, begin iterating through the files and process them
    elif job_type == "completed":
        return False, True, False, json.dumps(filenames)

    # TODO: Handle form validation errors
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

        filename = filenames[index]

        # Once the last file has been processed, terminate the job
        if index == len(filenames):
            return 100, "100%", "Processing complete!", True

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
        qc.process_data_file(path=path, filename=filename, extension="raw", run_id=run_id)
        return progress, progress_label, new_title, False

    else:
        return 0, "", "", False


if __name__ == "__main__":

    # if sys.platform == "win32":
    #     chrome_path = "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe"
    #     webbrowser.register("chrome", None, webbrowser.BackgroundBrowser(chrome_path))
    #     webbrowser.get("chrome").open("http://127.0.0.1:8050/")
    # elif sys.platform == "darwin":
    #     webbrowser.get("chrome").open("http://127.0.0.1:8050/", new=1)

    # Start Dash app
    app.run_server(threaded=False, debug=False)