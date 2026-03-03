"""Dash layout for Rapid-QC-MS.

serve_layout() is called by app.py and returns the full page structure.
Extracted verbatim from DashWebApp.py — no logic changes.
"""

import os

from dash import dcc, html, dash_table
import dash_bootstrap_components as dbc

from rapidqcms.dashboard.plots import bootstrap_colors

try:
    from flask import session as flask_session
except ImportError:
    flask_session = None

# Initialize directories (same as original DashWebApp.py)
src_folder = os.path.dirname(os.path.realpath(__file__))
root_directory = src_folder
data_directory = os.path.join(root_directory, "data")
methods_directory = os.path.join(data_directory, "methods")
auth_directory = os.path.join(root_directory, "auth")

for directory in [data_directory, auth_directory, methods_directory]:
    if not os.path.exists(directory):
        os.makedirs(directory)

def serve_layout():

    biohub_logo = "https://raw.githubusercontent.com/czbiohub-sf/Rapid-QC-MS/77a5b4908dc331ac94d186b4b85d804543b7df14/docs/CZ-Biohub-Mark-SF-Color-RGB.png"

    # Read user from session if Okta auth is active
    try:
        user = flask_session.get("user") if flask_session is not None else None
    except RuntimeError:
        user = None

    nav_items = [
        dbc.NavItem(dbc.NavLink("About", href="https://github.com/czbiohub-sf/Rapid-QC-MS", className="navbar-button", target="_blank")),
        dbc.NavItem(dbc.NavLink("Support", href="https://github.com/czbiohub-sf/Rapid-QC-MS/wiki", className="navbar-button", target="_blank")),
        dbc.NavItem(dbc.NavLink("Settings", href="#", id="settings-button", className="navbar-button")),
    ]
    if user:
        display_name = user.get("name") or user.get("email", "")
        nav_items.append(
            dbc.NavItem(dbc.NavLink(display_name, disabled=True, className="navbar-button"))
        )
        nav_items.append(
            dbc.NavItem(dbc.NavLink("Logout", href="/logout", className="navbar-button"))
        )

    return html.Div(className="app-layout", children=[

        # Navigation bar
        dbc.Navbar(
            dbc.Container(style={"height": "50px"}, children=[
                # Logo and title
                html.A(
                    dbc.Row([
                        dbc.Col(html.Img(src=biohub_logo, height="30px")),
                        dbc.Col(dbc.NavbarBrand(id="header", children="Rapid-QC-MS", className="ms-2")),
                        ], align="center", className="g-0",
                    ), href="https://www.czbiohub.org/", style={"textDecoration": "none"},
                ),
                # Nav links (About, Support, Settings, [user, Logout])
                dbc.Row([
                    dbc.Nav(nav_items, className="me-auto")
                ], className="g-0 ms-auto flex-nowrap mt-3 mt-md-0")
            ]), color="dark", dark=True
        ),

        # App layout
        html.Div(className="page", children=[

            dbc.Tabs(id="main-tabs", active_tab="run-browser", className="main-tabs mt-2", children=[

                dbc.Tab(label="Run Browser", tab_id="run-browser", children=[

            dbc.Row(justify="center", children=[

                dbc.Col(width=11, children=[

                    dbc.Row(justify="center", children=[

                        dbc.Col(width=12, lg=4, children=[

                            # Filter bar for the run browser
                            dbc.Row(className="mb-2", children=[
                                dbc.Col(width=3, children=[
                                    dcc.Dropdown(id="filter-instrument", placeholder="All instruments",
                                                 multi=True, clearable=True),
                                ]),
                                dbc.Col(width=3, children=[
                                    dcc.Dropdown(id="filter-type", placeholder="All types", clearable=True,
                                        options=[
                                            {"label": "Metabolomics", "value": "metabolomics"},
                                            {"label": "Proteomics",   "value": "proteomics"},
                                        ]),
                                ]),
                                dbc.Col(width=3, children=[
                                    dcc.Dropdown(id="filter-status", placeholder="All statuses", clearable=True,
                                        options=[
                                            {"label": "Active",    "value": "active"},
                                            {"label": "Completed", "value": "completed"},
                                        ]),
                                ]),
                                dbc.Col(width=3, children=[
                                    dcc.Dropdown(id="filter-date-range", value="2w", clearable=False,
                                        options=[
                                            {"label": "Last 2 weeks", "value": "2w"},
                                            {"label": "Last month",   "value": "1m"},
                                            {"label": "Last 3 months","value": "3m"},
                                            {"label": "All time",     "value": "all"},
                                        ]),
                                ]),
                            ]),

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
                                        "max-height": "420px",
                                        "overflowY": "auto"},
                                    style_data_conditional=[
                                        {"if": {"state": "active"},
                                        "backgroundColor": bootstrap_colors[
                                        "blue-low-opacity"],
                                        "border": "1px solid " + bootstrap_colors["blue"]
                                        }],
                                    style_cell_conditional=[
                                        {"if": {"column_id": "Run ID"},
                                            "width": "35%"},
                                        {"if": {"column_id": "Instrument"},
                                            "width": "20%"},
                                        {"if": {"column_id": "Date"},
                                            "width": "25%"},
                                        {"if": {"column_id": "Status"},
                                            "width": "20%"},
                                    ]
                                ),

                                # Progress bar for instrument run
                                dbc.Card(id="active-run-progress-card", style={"display": "none"},
                                    className="margin-top-15", children=[
                                        dbc.CardHeader(id="active-run-progress-header", style={"padding": "0.75rem"}),
                                        dbc.CardBody([

                                            # Instrument run progress
                                            dcc.Interval(id="refresh-interval", n_intervals=0, interval=30000, disabled=True),
                                            dbc.Progress(id="active-run-progress-bar", animated=False),

                                            # Buttons for managing Rapid-QC-MS jobs
                                            html.Div(id="job-controller-panel", children=[
                                                html.Div(className="d-flex justify-content-center btn-toolbar", children=[
                                                    # Button to mark current job as complete
                                                    html.Div(className="me-1", children=[
                                                        dbc.Button("Mark as Completed",
                                                            id="mark-as-completed-button",
                                                            className="run-button",
                                                            outline=True,
                                                            color="success"),
                                                    ]),

                                                    # Button to restart job
                                                    html.Div(className="me-1", children=[
                                                        dbc.Button("Restart Job",
                                                            id="restart-job-button",
                                                            className="run-button",
                                                            outline=True,
                                                            color="warning"),
                                                    ]),

                                                    # Button to delete job
                                                    html.Div(className="me-1", children=[
                                                        dbc.Button("Delete Job",
                                                            id="delete-job-button",
                                                            className="run-button",
                                                            outline=True,
                                                            color="danger"),
                                                    ]),
                                                ]),
                                            ]),
                                        ])
                                ]),

                                # Button to start new Rapid-QC-MS job
                                html.Div(className="d-grid gap-2", children=[
                                    dbc.Button("Setup New QC Job",
                                        id="setup-new-run-button",
                                        style={"margin-top": "15px",
                                            "line-height": "1.75"},
                                        outline=True,
                                        color="primary"),
                                ]),

                                # Polarity filtering options
                                html.Div(className="margin-top-15", children=[
                                    dcc.Dropdown(
                                        id="polarity-options",
                                        placeholder="All polarities",
                                        clearable=True,
                                        value=None,
                                        options=[
                                            {"label": "Positive", "value": "Pos"},
                                            {"label": "Negative", "value": "Neg"},
                                        ],
                                    ),
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
                                                {"label": "Specimens", "value": "specimens"},
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
                                        {"if": {"column_id": "Specimen"},
                                        "width": "55%"},
                                        {"if": {"column_id": "QC"},
                                        "width": "20%"},
                                        {"if": {"column_id": "Polarity"},
                                        "width": "25%"},
                                    ]
                                )
                            ]),
                        ]),

                        dbc.Col(width=12, lg=8, children=[

                            # Container for all plots
                            html.Div(id="plot-container", className="all-plots-container", style={"display": "none"}, children=[

                                html.Div(className="istd-plot-div", children=[

                                    html.Div(id="istd-rt-div", className="plot-container", children=[

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
                                            placeholder="Select specimens...",
                                            style={"text-align": "left",
                                                   "height": "1.5",
                                                   "width": "100%",
                                                   "display": "inline-block"},
                                            multi=True),

                                        # Scatter plot of internal standard retention times vs. samples
                                        dcc.Graph(id="istd-rt-plot"),
                                    ]),

                                    html.Div(id="istd-intensity-div", className="plot-container", children=[

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
                                            placeholder="Select specimens...",
                                            style={"text-align": "left",
                                                   "height": "1.5",
                                                   "width": "100%",
                                                   "display": "inline-block"},
                                            multi=True,
                                        ),

                                        # Bar plot of internal standard intensity vs. samples
                                        dcc.Graph(id="istd-intensity-plot")
                                    ]),

                                    html.Div(id="istd-mz-div", className="plot-container", children=[

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
                                            placeholder="Select specimens...",
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
                                    html.Div(id="bio-standard-mz-rt-div", className="plot-container", children=[

                                        # Dropdown for selecting a biological standard to view
                                        dcc.Dropdown(id="bio-standards-plot-dropdown",
                                            options=[], placeholder="Select biological standard...",
                                            style={"text-align": "left", "height": "1.5", "font-size": "1rem",
                                                "width": "100%", "display": "inline-block"}),
                                        html.Div(className="child-container", children=[
                                            dcc.Dropdown(id="bio-standards-plot-dropdown-compare-target",
                                                options=[], placeholder="Select target...",
                                                style={"text-align": "left", "height": "1.5", "font-size": "1rem",
                                                    "width": "100%", "display": "inline-block"})
                                        ]),

                                        html.Div(className="child-container", children=[
                                            dcc.Dropdown(id="bio-standards-plot-dropdown-compare-source",
                                                options=[], placeholder="Select target...",
                                                style={"text-align": "left", "height": "1.5", "font-size": "1rem",
                                                    "width": "100%", "display": "inline-block"})
                                        ]),
                                        dcc.Dropdown(id="bio-standards-plot-dropdown-jobid",
                                            options=[], placeholder="Pick target job for pool compares...",
                                            style={"text-align": "left", "height": "1.5", "font-size": "1rem",
                                                "width": "100%", "display": "inline-block"}),

                                        dcc.Graph(id="bio-standard-mz-rt-plot")
                                    ]),

                                    # Bar plot for biological standard feature intensity vs. run
                                    html.Div(id="bio-standard-benchmark-div", className="plot-container", children=[

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
                            dbc.Button("dump to .csv", id="dump-sample-modal-to-csv"),
                            dcc.Download(id="dumped-sample-info-card"),
                            dbc.ModalHeader(dbc.ModalTitle(id="sample-modal-title"), close_button=True),
                            dbc.ModalBody(id="sample-modal-body")
                        ]),

                        # Modal for alerting user that data is loading
                        dbc.Modal(id="loading-modal", size="md", centered=True, is_open=False, scrollable=True,
                                  keyboard=False, backdrop="static", children=[
                            dbc.ModalHeader(dbc.ModalTitle(id="loading-modal-title"), close_button=False),
                            dbc.ModalBody(id="loading-modal-body")
                        ]),

                        # Modal for job completion / restart / deletion confirmation
                        dbc.Modal(id="job-controller-modal", size="md", centered=True, is_open=False, children=[
                            dbc.ModalHeader(dbc.ModalTitle(id="job-controller-modal-title")),
                            dbc.ModalBody(id="job-controller-modal-body"),
                            dbc.ModalFooter(children=[
                                dbc.Button("Cancel", color="secondary", id="job-controller-cancel-button"),
                                dbc.Button(id="job-controller-confirm-button")
                            ]),
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
                            dbc.ModalHeader(dbc.ModalTitle("Welcome to Rapid-QC-MS", id="setup-user-modal-title"), close_button=False),
                            dbc.ModalBody(id="setup-user-modal-body", className="modal-styles-2", children=[

                                html.Div([
                                    html.H5("Let's help you get started."),
                                    html.P("Looks like this is a new installation. What would you like to do today?"),
                                    dbc.Accordion(start_collapsed=True, children=[

                                        # Setting up Rapid-QC-MS for the first time
                                        dbc.AccordionItem(title="I'm setting up Rapid-QC-MS on a new instrument", children=[
                                            html.Div(className="modal-styles-3", children=[

                                                # Instrument name text field
                                                html.Div([
                                                    dbc.Label("Instrument name"),
                                                    dbc.InputGroup([
                                                        dbc.Input(id="first-time-instrument-id", type="text",
                                                                  placeholder="Ex: Thermo Q-Exactive HF 1",
                                                                  pattern="^[A-Za-z0-9_ -]+$"),
                                                        dbc.DropdownMenu(id="first-time-instrument-vendor",
                                                            label="Choose Vendor", color="primary", children=[
                                                                dbc.DropdownMenuItem("Thermo Fisher", id="thermo-fisher-item")
                                                        ]),
                                                    ]),
                                                    dbc.FormText("Enter name (alphanumeric, underscores, dashes, spaces) 4 chars or longer and vendor for this instrument."),
                                                ]),

                                                html.Br(),

                                                # Google Drive authentication button
                                                html.Div([
                                                    dbc.Label("Sync with Google Drive (experimental)"),
                                                    html.Br(),
                                                    dbc.InputGroup([
                                                        dbc.Input(placeholder="Client ID", id="gdrive-client-id-1"),
                                                        dbc.Input(placeholder="Client secret", id="gdrive-client-secret-1"),
                                                        dbc.Button("Sign in to Google Drive", id="setup-google-drive-button-1",
                                                           color="primary", outline=True),
                                                    ]),
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
                                        dbc.AccordionItem(title="I'm signing in to an existing Rapid-QC-MS workspace", children=[
                                            html.Div(className="modal-styles-3", children=[

                                                # Google Drive authentication button
                                                html.Div([
                                                    dbc.Label("Sign in to access Rapid-QC-MS"), html.Br(),
                                                    dbc.InputGroup([
                                                        dbc.Input(placeholder="Client ID", id="gdrive-client-id-2"),
                                                        dbc.Input(placeholder="Client secret", id="gdrive-client-secret-2"),
                                                        dbc.Button("Sign in to Google Drive", id="setup-google-drive-button-2",
                                                            color="primary", outline=False),
                                                    ]),
                                                    dbc.FormText(
                                                        "Please ensure that your Google account has been registered to " +
                                                        "access your Rapid-QC-MS workspace by visiting Settings > General."),
                                                    dbc.Popover(id="google-drive-button-2-popover", is_open=False,
                                                                target="setup-google-drive-button-2", placement="right")
                                                ]),

                                                # Checkbox for logging in to instrument computer
                                                dbc.Checkbox(id="device-identity-checkbox", className="checkbox-margin",
                                                    label="I am signing in from an instrument computer", value=False),

                                                # Dropdown for selecting an instrument
                                                dbc.Select(id="device-identity-selection", value=None,
                                                    placeholder="Which instrument?", disabled=True),

                                                html.Br(),

                                                # Workspace sign-in button
                                                html.Div([
                                                    html.Div([
                                                        dbc.Button("Sign in to Rapid-QC-MS workspace", id="first-time-sign-in-button",
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
                            dbc.ModalHeader(dbc.ModalTitle(id="setup-new-run-modal-title", children="New QC Job"), close_button=True),
                            dbc.ModalBody(id="setup-new-run-modal-body", className="modal-styles-2", children=[

                                # Text field for entering your job ID
                                html.Div([
                                    dbc.Label("Job ID"),
                                    dbc.Input(id="instrument-run-id", placeholder="Give your job a unique ID", type="text"),
                                    dbc.FormFeedback("Looks good!", type="valid"),
                                    dbc.FormFeedback("Please enter a unique ID for this job.", type="invalid"),
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
                                        "Internal Standards and Settings > Biological Standards.", type="invalid")
                                ]),

                                html.Br(),

                                # Select biological standard used in this study
                                html.Div(children=[
                                    dbc.Label("Select biological standards (optional)"),
                                    dcc.Dropdown(id="start-run-bio-standards-dropdown",
                                        options=[], placeholder="Select biological standards...",
                                        style={"text-align": "left", "height": "1.5", "font-size": "1rem",
                                            "width": "100%", "display": "inline-block"},
                                        multi=True)
                                ]),

                                html.Br(),

                                # Select AutoQC configuration
                                html.Div(children=[
                                    dbc.Label("Select Rapid-QC-MS configuration"),
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
                                            accept="text/plain, application/vnd.ms-excel, .csv",
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
                                            accept="text/plain, application/vnd.ms-excel, .csv",
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
                                    dbc.RadioItems(id="ms_autoqc-job-type", value="active", options=[
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
                                dbc.Alert("Rapid-QC-MS will start monitoring your run. Please do not restart your computer.", color="success")
                            ]),
                        ]),

                        # Error modal for new AutoQC job setup
                        dbc.Modal(id="new-job-error-modal", size="md", centered=True, is_open=False, children=[
                            dbc.ModalHeader(dbc.ModalTitle(id="new-job-error-modal-title"), close_button=False),
                            dbc.ModalBody(id="new-job-error-modal-body", className="modal-styles"),
                        ]),

                        # Rapid-QC-MS settings
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
                                                "This Google account already has an Rapid-QC-MS workspace."),
                                            html.P(
                                                "Please sign in with a different Google account to enable cloud "
                                                "sync for this workspace."),
                                            html.P(
                                                "Or, if you'd like to add a new instrument to an existing Rapid-QC-MS "
                                                "workspace, please reinstall Rapid-QC-MS on this instrument and enable "
                                                "cloud sync during setup.")
                                        ]),

                                        dbc.Alert(id="gdrive-credentials-saved-alert", is_open=False, duration=5000),

                                        dbc.Label("Manage workspace access", style={"font-weight": "bold"}),
                                        html.Br(),

                                        # Google Drive cloud storage
                                        dbc.Label("Google API client credentials"),
                                        html.Br(),
                                        dbc.InputGroup([
                                            dbc.Input(placeholder="Client ID", id="gdrive-client-id"),
                                            dbc.Input(placeholder="Client secret", id="gdrive-client-secret"),
                                            dbc.Button("Set credentials",
                                                id="set-gdrive-credentials-button", color="primary", outline=True),
                                        ]),
                                        dbc.FormText(children=[
                                            "You can get these credentials from the ",
                                            html.A("Google Cloud console",
                                               href="https://console.cloud.google.com/apis/credentials", target="_blank"),
                                            " in Credentials > OAuth 2.0 Client ID's."]),
                                        html.Br(), html.Br(),

                                        dbc.Label("Enable cloud sync with Google Drive"),
                                        html.Br(),
                                        dbc.Button("Sync with Google Drive",
                                            id="google-drive-sync-button", color="primary", outline=False),
                                        html.Br(),
                                        dbc.FormText(id="google-drive-sync-form-text", children=
                                            "This will allow you to monitor your instrument runs on other devices."),
                                        html.Br(), html.Br(),

                                        # Alerts for modifying workspace access
                                        dbc.Alert(id="user-addition-alert", color="success", is_open=False, duration=5000),
                                        dbc.Alert(id="user-deletion-alert", color="primary", is_open=False, duration=5000),

                                        # Google Drive sharing
                                        dbc.Label("Add / remove workspace users"),
                                        html.Br(),
                                        dbc.InputGroup([
                                            dbc.Input(placeholder="example@gmail.com", id="add-user-text-field"),
                                            dbc.Button("Add user", color="primary", outline=True,
                                                id="add-user-button", n_clicks=0),
                                            dbc.Button("Delete user", color="danger", outline=True,
                                                id="delete-user-button", n_clicks=0),
                                            dbc.Popover("This will revoke user access to the Rapid-QC-MS workspace. "
                                                "Are you sure?", target="delete-user-button", trigger="hover", body=True)
                                        ]),
                                        dbc.FormText(
                                            "Adding new users grants full read-and-write access to this Rapid-QC-MS workspace."),
                                        html.Br(), html.Br(),

                                        # Table of users with workspace access
                                        html.Div(id="workspace-users-table"),
                                        html.Br(),

                                        dbc.Label("Slack notifications", style={"font-weight": "bold"}),
                                        html.Br(),

                                        # Alerts for modifying workspace access
                                        dbc.Alert(id="slack-token-save-alert", is_open=False, duration=5000),

                                        # Channel for Slack notifications
                                        dbc.Label("Slack API client credentials"),
                                        html.Br(),
                                        dbc.InputGroup([
                                            dbc.Input(placeholder="Slack bot user OAuth token", id="slack-bot-token"),
                                            dbc.Button("Save bot token", color="primary", outline=True,
                                                       id="save-slack-token-button", n_clicks=0),
                                        ]),
                                        dbc.FormText(children=[
                                            "You can get the Slack bot token from the ",
                                            html.A("Slack API website",
                                               href="https://api.slack.com/apps", target="_blank"),
                                            " in Your App > Settings > Install App."]),
                                        html.Br(), html.Br(),

                                        dbc.Alert(id="slack-notifications-toggle-alert", is_open=False, duration=5000),

                                        dbc.Label("Register Slack channel for notifications"),
                                        dbc.InputGroup(children=[
                                            dbc.Input(id="slack-channel", placeholder="#my-slack-channel"),
                                            dbc.InputGroupText(
                                                dbc.Switch(id="slack-notifications-enabled", label="Enable notifications")),
                                        ]),
                                        dbc.FormText(
                                            "Please enter the Slack channel you'd like to register for notifications."),
                                        html.Br(), html.Br(),

                                        dbc.Label("Email notifications", style={"font-weight": "bold"}),
                                        html.Br(),

                                        # Alerts for modifying email notification list
                                        dbc.Alert(id="email-addition-alert", is_open=False, duration=5000),
                                        dbc.Alert(id="email-deletion-alert", is_open=False, duration=5000),

                                        # Register recipients for email notifications
                                        dbc.Label("Register recipients for email notifications"),
                                        html.Br(),
                                        dbc.InputGroup([
                                            dbc.Input(placeholder="recipient@example.com",
                                                id="email-notifications-text-field"),
                                            dbc.Button("Register email", color="primary", outline=True,
                                                id="add-email-button", n_clicks=0),
                                            dbc.Button("Remove email", color="danger", outline=True,
                                                id="delete-email-button", n_clicks=0),
                                            dbc.Popover("This will un-register the email account from Rapid-QC-MS "
                                                "notifications. Are you sure?", target="delete-email-button",
                                                trigger="hover", body=True)
                                        ]),
                                        dbc.FormText(
                                            "Please enter a valid email address to register for email notifications."),
                                        html.Br(), html.Br(),

                                        # Table of users registered for email notifications
                                        html.Div(id="email-notifications-table")
                                    ]),

                                    # Internal standards
                                    dbc.Tab(label="Chromatography methods", className="modal-styles", children=[

                                        html.Br(),

                                        # Alerts for user feedback on biological standard addition/removal
                                        dbc.Alert(id="chromatography-addition-alert", color="success", is_open=False, duration=5000),
                                        dbc.Alert(id="chromatography-removal-alert", color="primary", is_open=False, duration=5000),

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

                                        dbc.Alert(id="istd-config-success-alert", color="success", is_open=False, duration=5000),

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
                                                  duration=5000),
                                        dbc.Alert(id="chromatography-msp-error-alert", color="danger", is_open=False,
                                                  duration=5000),

                                        dbc.Label("Add internal standard identification files", style={"font-weight": "bold"}),
                                        html.Br(),

                                        html.Div([
                                            dbc.Label("Add internal standards (MSP or CSV format)"),
                                            dbc.InputGroup([
                                                dbc.Input(placeholder="No file selected",
                                                          id="add-istd-msp-text-field"),
                                                dbc.Button(dcc.Upload(
                                                    id="add-istd-msp-button",
                                                    accept="text/plain, application/vnd.ms-excel, .msp, .csv",
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
                                        dbc.Alert(id="bio-standard-addition-alert", is_open=False, duration=5000),

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
                                                "The sequence identifier is a string that gets matched to specimen filenames and is case sensitive."),
                                        ]),

                                        html.Br(),

                                        # Table of biological standards
                                        dbc.Label("Biological standards", style={"font-weight": "bold"}),
                                        html.Br(),

                                        html.Div(id="biological-standards-table"),
                                        html.Br(),

                                        dbc.Alert(id="bio-standard-removal-alert", color="primary", is_open=False, duration=5000),

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

                                        dbc.Alert(id="bio-config-success-alert", color="success", is_open=False, duration=5000),

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
                                                  duration=5000),
                                        dbc.Alert(id="bio-msp-error-alert", color="danger", is_open=False,
                                                  duration=5000),

                                        html.Div([
                                            dbc.Label("Edit targeted metabolites list (MSP format)"),
                                            html.Br(),
                                            dbc.InputGroup([
                                                dbc.Input(placeholder="No MSP file selected",
                                                          id="add-bio-msp-text-field"),
                                                dbc.Button(dcc.Upload(
                                                    id="add-bio-msp-button",
                                                    accept=".msp",
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
                                    dbc.Tab(label="IS configurations", className="modal-styles", children=[

                                        html.Br(),

                                        # UI feedback on adding / removing QC configurations
                                        dbc.Alert(id="qc-config-addition-alert", is_open=False, duration=5000),
                                        dbc.Alert(id="qc-config-removal-alert", is_open=False, duration=5000),

                                        dbc.Label("Manage IS configurations", style={"font-weight": "bold"}),
                                        html.Br(),

                                        html.Div([
                                            dbc.Label("Add new IS configuration"),
                                            dbc.InputGroup([
                                                dbc.Input(id="add-is-configuration-text-field",
                                                          placeholder="Name of configuration to add"),
                                                dbc.Button("Add new config", color="primary", outline=True,
                                                           id="add-is-configuration-button", n_clicks=0),
                                            ]),
                                            dbc.FormText("Give your custom IS configuration a unique name"),
                                        ]),

                                        html.Br(),

                                        # Select configuration
                                        html.Div(children=[
                                            dbc.Label("Select IS configuration to edit"),
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

                                        dbc.Label("Edit IS configuration parameters", style={"font-weight": "bold"}),
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
                                                  color="success", is_open=False, duration=5000),
                                        dbc.Alert(id="qc-parameters-reset-alert",
                                                  color="primary", is_open=False, duration=5000),
                                        dbc.Alert(id="qc-parameters-error-alert",
                                                  color="danger", is_open=False, duration=5000),

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
                                        dbc.Alert(id="msdial-config-addition-alert", is_open=False, duration=5000),
                                        dbc.Alert(id="msdial-config-removal-alert", is_open=False, duration=5000),
                                        dbc.Alert(id="msdial-directory-saved-alert", is_open=False, duration=5000),

                                        dbc.Label("MS-DIAL installation", style={"font-weight": "bold"}),
                                        html.Br(),

                                        # Button and field for selecting the data acquisition directory
                                        html.Div([
                                            dbc.Label("MS-DIAL download location"),
                                            dbc.InputGroup([
                                                dbc.Input(placeholder="C:/Users/Me/Downloads/MS-DIAL",
                                                    id="msdial-directory"),
                                                dbc.Button("Browse Folders", id="msdial-folder-button",
                                                    color="secondary", outline=True),
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

                                        html.Div([
                                            # UI feedback on saving changes to MS-DIAL parameters
                                            dbc.Alert(id="msdial-parameters-success-alert",
                                                color="success", is_open=False, duration=5000),
                                            dbc.Alert(id="msdial-parameters-reset-alert",
                                                color="primary", is_open=False, duration=5000),
                                            dbc.Alert(id="msdial-parameters-error-alert",
                                                color="danger", is_open=False, duration=5000),
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

                ]),  # end Run Browser Tab

                dbc.Tab(label="Instrument Performance", tab_id="performance", children=[
                    dbc.Row(justify="center", style={"paddingTop": "20px"}, children=[
                        dbc.Col(width=11, children=[

                            # Filter row
                            dbc.Row(className="mb-3", children=[
                                dbc.Col(width=4, children=[
                                    dcc.Dropdown(id="perf-instrument", placeholder="All instruments",
                                                 multi=True, clearable=True),
                                ]),
                                dbc.Col(width=4, children=[
                                    dcc.Dropdown(id="perf-exp-type", placeholder="All types", clearable=True,
                                        options=[
                                            {"label": "Metabolomics", "value": "metabolomics"},
                                            {"label": "Proteomics",   "value": "proteomics"},
                                        ]),
                                ]),
                                dbc.Col(width=4, children=[
                                    dcc.Dropdown(id="perf-date-range", value="3m", clearable=False,
                                        options=[
                                            {"label": "Last month",    "value": "1m"},
                                            {"label": "Last 3 months", "value": "3m"},
                                            {"label": "Last 6 months", "value": "6m"},
                                            {"label": "All time",      "value": "all"},
                                        ]),
                                ]),
                            ]),

                            # Summary cards
                            dbc.Row(className="mb-3", children=[
                                dbc.Col(width=3, children=[dbc.Card(dbc.CardBody([
                                    html.H6("Total Runs", className="text-muted small"),
                                    html.H4(id="perf-card-runs", children="—"),
                                ]))]),
                                dbc.Col(width=3, children=[dbc.Card(dbc.CardBody([
                                    html.H6("Pass Rate", className="text-muted small"),
                                    html.H4(id="perf-card-passrate", children="—"),
                                ]))]),
                                dbc.Col(width=3, children=[dbc.Card(dbc.CardBody([
                                    html.H6("Avg IS Detection", className="text-muted small"),
                                    html.H4(id="perf-card-detection", children="—"),
                                ]))]),
                                dbc.Col(width=3, children=[dbc.Card(dbc.CardBody([
                                    html.H6("Last Run", className="text-muted small"),
                                    html.H4(id="perf-card-lastrun", children="—"),
                                ]))]),
                            ]),

                            # Status chart — always visible
                            dbc.Row(className="mb-3", children=[
                                dbc.Col(width=12, children=[dcc.Graph(id="perf-status-chart")]),
                            ]),

                            # Metabolomics-specific charts
                            html.Div(id="perf-metabolomics-section", children=[
                                dbc.Row(className="mb-3", children=[
                                    dbc.Col(width=6, children=[dcc.Graph(id="perf-detection-chart")]),
                                    dbc.Col(width=6, children=[dcc.Graph(id="perf-issues-chart")]),
                                ]),
                            ]),

                            # Proteomics-specific charts
                            html.Div(id="perf-proteomics-section", children=[
                                dbc.Row(className="mb-3", children=[
                                    dbc.Col(width=12, children=[dcc.Graph(id="perf-scan-chart")]),
                                ]),
                            ]),

                        ]),
                    ]),
                ]),  # end Instrument Performance Tab

            ]),  # end dbc.Tabs

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
            dcc.Store(id="qc-warnings-pos"),
            dcc.Store(id="qc-warnings-neg"),
            dcc.Store(id="qc-fails-pos"),
            dcc.Store(id="qc-fails-neg"),
            dcc.Store(id="sequence"),
            dcc.Store(id="metadata"),
            dcc.Store(id="bio-rt-pos"),
            dcc.Store(id="bio-rt-neg"),
            dcc.Store(id="bio-intensity-pos"),
            dcc.Store(id="bio-intensity-neg"),
            dcc.Store(id="bio-mz-pos"),
            dcc.Store(id="bio-mz-neg"),
            dcc.Store(id="study-resources"),
            dcc.Store(id="specimens"),
            dcc.Store(id="pos-internal-standards"),
            dcc.Store(id="neg-internal-standards"),
            dcc.Store(id="instruments"),
            dcc.Store(id="selected-instrument"),
            dcc.Store(id="load-finished"),
            dcc.Store(id="close-load-modal"),

            # Data for starting a new AutoQC job
            dcc.Store(id="new-sequence"),
            dcc.Store(id="new-metadata"),

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
            dcc.Store(id="msdial-directory-saved"),
            dcc.Store(id="google-drive-sync-finished"),
            dcc.Store(id="close-sync-modal"),
            dcc.Store(id="database-md5"),
            dcc.Store(id="selected-data-folder"),
            dcc.Store(id="selected-msdial-folder"),
            dcc.Store(id="google-drive-user-added"),
            dcc.Store(id="google-drive-user-deleted"),
            dcc.Store(id="email-added"),
            dcc.Store(id="email-deleted"),
            dcc.Store(id="gdrive-credentials-saved"),
            dcc.Store(id="slack-bot-token-saved"),
            dcc.Store(id="slack-channel-saved"),
            dcc.Store(id="google-drive-sync-update"),
            dcc.Store(id="job-marked-completed"),
            dcc.Store(id="job-restarted"),
            dcc.Store(id="job-deleted"),
            dcc.Store(id="job-action-failed"),
            dcc.Store(id="feature-table-for-csv", storage_type='local', data={}),
            dcc.Store(id="csv-filename"),
            dcc.Store(id="perf-data"),
            # Dummy inputs for Google Drive authentication
            dcc.Store(id="google-drive-download-database"),
            dcc.Store(id="workspace-has-been-setup-1"),
            dcc.Store(id="workspace-has-been-setup-2"),
            dcc.Store(id="google-drive-authenticated-1"),
            dcc.Store(id="gdrive-folder-id-1"),
            dcc.Store(id="gdrive-database-file-id-1"),
            dcc.Store(id="gdrive-methods-zip-id-1"),
            dcc.Store(id="google-drive-authenticated-2"),
            dcc.Store(id="gdrive-folder-id-2"),
            dcc.Store(id="gdrive-database-file-id-2"),
            dcc.Store(id="gdrive-methods-zip-id-2"),
            dcc.Store(id="google-drive-authenticated-3"),
            dcc.Store(id="gdrive-folder-id-3"),
            dcc.Store(id="gdrive-database-file-id-3"),
            dcc.Store(id="gdrive-methods-zip-id-3"),
        ])
    ])

