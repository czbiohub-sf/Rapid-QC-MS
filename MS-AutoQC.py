import sys, webbrowser, json
import pandas as pd
from dash import dash, dcc, html, dash_table, Input, Output, State
import dash_bootstrap_components as dbc
from QCFileProcessing import *

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
                html.A(
                    # Use row and col to control vertical alignment of logo / brand
                    dbc.Row([
                        dbc.Col(html.Img(src=biohub_logo, height="30px")),
                        dbc.Col(dbc.NavbarBrand(id="header", children="MS-AutoQC", className="ms-2")),
                    ], align="center", className="g-0",
                ),
                href="https://biohub.org",
                style={"textDecoration": "none"},
            )]), color="dark", dark=True,
        ),

        # App layout
        html.Div(className="page", children=[

            dbc.Row(justify="center", children=[

                dbc.Col(width=11, children=[

                    dcc.Tabs(id="tabs", className="instrument-tabs", children=[

                        # QC dashboard for QE 1
                        dcc.Tab(label="Thermo QE 1", children=[

                            dbc.Row(justify="center", children=[

                                dbc.Col(width=12, lg=4, children=[

                                    html.Div(id="QE1-table-container", className="table-container", style={"display": "none"}, children=[

                                        # Table of past/active studies that were run on QE 1
                                        dash_table.DataTable(id="QE1-table", page_action="none",
                                            fixed_rows={"headers": True},
                                            cell_selectable=True,
                                            style_cell={"textAlign": "left",
                                                        "fontSize": "15px",
                                                        "fontFamily": "sans-serif",
                                                        "lineHeight": "25px",
                                                        "padding": "10px",
                                                        "borderRadius": "5px"},
                                            style_data={"whiteSpace": "normal",
                                                        "textOverflow": "ellipsis",
                                                        "maxWidth": 0},
                                            style_table={"max-height": "285px",
                                                        "overflowY": "auto"},
                                            style_data_conditional=[
                                                {
                                                    "if": {
                                                        "state": "active"
                                                    },
                                                   "backgroundColor": bootstrap_colors["blue-low-opacity"],
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

                                        # Polarity filtering options
                                        html.Div(className="radio-group-container", children=[
                                            html.Div(className="radio-group", children=[
                                                dbc.RadioItems(
                                                    id="QE1-polarity-options",
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
                                            html.Div(className="radio-group", children=[
                                                dbc.RadioItems(
                                                    id="QE1-sample-filtering-options",
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
                                        dash_table.DataTable(id="QE1-sample-table", page_action="none",
                                            fixed_rows={"headers": True},
                                            # cell_selectable=True,
                                            style_cell={"textAlign": "left",
                                                        "fontSize": "15px",
                                                        "fontFamily": "sans-serif",
                                                        "lineHeight": "25px",
                                                        "whiteSpace": "normal",
                                                        "padding": "10px",
                                                        "borderRadius": "5px"},
                                            style_data={"whiteSpace": "normal",
                                                        "textOverflow": "ellipsis",
                                                        "maxWidth": 0},
                                            style_table={"height": "475px",
                                                         "overflowY": "auto"},
                                            style_data_conditional=[
                                                {"if": {"filter_query": "{QC} = 'Fail'"},
                                                    "backgroundColor": bootstrap_colors["red-low-opacity"],
                                                    "font-weight": "bold"
                                                },
                                                {"if": {"filter_query": "{QC} = 'Check'"},
                                                    "backgroundColor": bootstrap_colors["yellow-low-opacity"]
                                                },
                                                {"if": {"state": "active"},
                                                   "backgroundColor": bootstrap_colors["blue-low-opacity"],
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

                                    # Data container for QE 1 plots
                                    html.Div(id="QE1-plot-container", className="all-plots-container", style={"display": "none"}, children=[

                                        html.Div(className="istd-plot-div", children=[

                                            html.Div(className="plot-container", children=[

                                                # Dropdown for selecting an internal standard for the RT vs. sample plot
                                                dcc.Dropdown(
                                                    id="QE1-istd-rt-dropdown",
                                                    options=standards_list,
                                                    placeholder="Select internal standards...",
                                                    style={"text-align": "left",
                                                           "height": "35px",
                                                           "width": "100%",
                                                           "display": "inline-block"}
                                                ),

                                                # Dropdown for filtering by sample for the RT vs. sample plot
                                                dcc.Dropdown(
                                                    id="QE1-rt-plot-sample-dropdown",
                                                    options=[],
                                                    placeholder="Select samples...",
                                                    style={"text-align": "left",
                                                           "height": "35px",
                                                           "width": "100%",
                                                           "display": "inline-block"},
                                                    multi=True),

                                                # Scatter plot of internal standard retention times in QE 1 samples
                                                dcc.Graph(id="QE1-istd-rt-plot")
                                            ]),

                                            html.Div(className="plot-container", children=[

                                                # Dropdown for internal standard intensity plot
                                                dcc.Dropdown(
                                                    id="QE1-istd-intensity-dropdown",
                                                    options=standards_list,
                                                    placeholder="Select internal standard...",
                                                    style={"text-align": "left",
                                                           "height": "35px",
                                                           "width": "100%",
                                                           "display": "inline-block"}),

                                                # Dropdown for filtering by sample for the intensity vs. sample plot
                                                dcc.Dropdown(
                                                    id="QE1-intensity-plot-sample-dropdown",
                                                    options=[],
                                                    placeholder="Select samples...",
                                                    style={"text-align": "left",
                                                           "height": "35px",
                                                           "width": "100%",
                                                           "display": "inline-block"},
                                                multi=True),

                                                # Bar plot of internal standard intensity in QE 1 samples
                                                dcc.Graph(id="QE1-istd-intensity-plot")
                                            ]),

                                            html.Div(className="plot-container", children=[

                                                # Dropdown for internal standard delta m/z plot
                                                dcc.Dropdown(
                                                    id="QE1-istd-mz-dropdown",
                                                    options=standards_list,
                                                    placeholder="Select internal standards...",
                                                    style={"text-align": "left",
                                                           "height": "35px",
                                                           "width": "100%",
                                                           "display": "inline-block"},
                                                ),

                                                # Dropdown for filtering by sample for the delta m/z vs. sample plot
                                                dcc.Dropdown(
                                                    id="QE1-mz-plot-sample-dropdown",
                                                    options=[],
                                                    placeholder="Select samples...",
                                                    style={"text-align": "left",
                                                           "height": "35px",
                                                           "width": "100%",
                                                           "display": "inline-block"},
                                                    multi=True),

                                                # Scatter plot of internal standard delta m/z vs. samples
                                                dcc.Graph(id="QE1-istd-mz-plot")
                                            ]),

                                        ]),

                                        html.Div(className="urine-plot-div", children=[

                                            # Scatter plot of QC urine feature retention times from QE 1
                                            html.Div(className="plot-container", children=[
                                                dcc.Graph(id="QE1-urine-rt-plot")
                                            ]),

                                            # Bar plot of QC urine feature peak heights from QE 1
                                            html.Div(className="plot-container", children=[

                                                # Dropdown for urine feature intensity plot
                                                dcc.Dropdown(
                                                    id="QE1-urine-intensity-dropdown",
                                                    options=list(get_pos_urine_features_dict().keys()),
                                                    placeholder="Select urine feature...",
                                                    style={"text-align": "left",
                                                           "height": "35px",
                                                           "width": "100%",
                                                           "display": "inline-block"}
                                                ),

                                                dcc.Graph(id="QE1-urine-intensity-plot", animate=False)
                                            ])
                                        ])
                                    ]),
                                ]),

                                # Modal/dialog for sample information card
                                dbc.Modal(id="QE1-sample-info-modal", size="xl", centered=True, is_open=False,
                                    scrollable=True, children=[
                                        dbc.ModalHeader(dbc.ModalTitle(id="QE1-sample-modal-title"), close_button=True),
                                        dbc.ModalBody(id="QE1-sample-modal-body")
                                ]),

                                # Modal/dialog for alerting user that data is loading
                                dbc.Modal(id="QE1-loading-modal", size="md", centered=True, is_open=False, scrollable=True,
                                    keyboard=False, backdrop="static", children=[
                                        dbc.ModalHeader(dbc.ModalTitle(id="QE1-loading-modal-title"), close_button=False),
                                        dbc.ModalBody(id="QE1-loading-modal-body")
                                ]),
                            ]),
                        ]),

                        # QC dashboard for QE 2
                        dcc.Tab(label="Thermo QE 2", children=[

                            dbc.Row(justify="center", children=[

                                dbc.Col(width=12, lg=4, children=[

                                    html.Div(id="QE2-table-container", className="table-container", style={"display": "none"}, children=[

                                        # Table of past/active studies that were run on QE 2
                                        dash_table.DataTable(id="QE2-table", page_action="none",
                                             fixed_rows={"headers": True},
                                             cell_selectable=True,
                                             style_cell={"textAlign": "left",
                                                         "fontSize": "15px",
                                                         "fontFamily": "sans-serif",
                                                         "lineHeight": "25px",
                                                         "padding": "10px",
                                                         "borderRadius": "5px"},
                                             style_data={"whiteSpace": "normal",
                                                         "textOverflow": "ellipsis",
                                                         "maxWidth": 0},
                                             style_table={"max-height": "285px",
                                                          "overflowY": "auto"},
                                             style_data_conditional=[{
                                                    "if": {
                                                        "state": "active"
                                                    },
                                                   "backgroundColor": bootstrap_colors["blue-low-opacity"],
                                                   "border": "1px solid " + bootstrap_colors["blue"]
                                                }],
                                             style_cell_conditional=[
                                                {"if": {"column_id": "Study"},
                                                    "width": "50%"},
                                                {"if": {"column_id": "Type"},
                                                    "width": "25%"},
                                                {"if": {"column_id": "Type"},
                                                    "width": "25%"}
                                             ]
                                         ),

                                        # Polarity filtering options
                                        html.Div(className="radio-group-container", children=[
                                            html.Div(className="radio-group", children=[
                                                dbc.RadioItems(
                                                    id="QE2-polarity-options",
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
                                            html.Div(className="radio-group", children=[
                                                dbc.RadioItems(
                                                    id="QE2-sample-filtering-options",
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
                                        dash_table.DataTable(id="QE2-sample-table", page_action="none",
                                            fixed_rows={"headers": True},
                                            # cell_selectable=True,
                                            style_cell={"textAlign": "left",
                                                        "fontSize": "15px",
                                                        "fontFamily": "sans-serif",
                                                        # "whiteSpace": "normal",
                                                        "lineHeight": "25px",
                                                        "padding": "10px",
                                                        "borderRadius": "5px"},
                                            style_data={"whiteSpace": "normal",
                                                        "textOverflow": "ellipsis",
                                                        "maxWidth": 0},
                                            style_table={"height": "475px",
                                                         "overflowY": "auto"},
                                            style_data_conditional=[
                                                {"if": {"filter_query": "{QC} = 'Fail'"},
                                                    "backgroundColor": bootstrap_colors["red-low-opacity"],
                                                    "font-weight": "bold"
                                                },
                                                {"if": {"filter_query": "{QC} = 'Check'"},
                                                    "backgroundColor": bootstrap_colors["yellow-low-opacity"]
                                                },
                                                {"if": {"state": "active"},
                                                   "backgroundColor": bootstrap_colors["blue-low-opacity"],
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

                                    # Data container for QE 2 plots
                                    html.Div(id="QE2-plot-container", className="all-plots-container", style={"display": "none"}, children=[

                                        html.Div(className="istd-plot-div", children=[

                                            html.Div(className="plot-container", children=[

                                                # Dropdown for internal standard RT plot
                                                dcc.Dropdown(
                                                    id="QE2-istd-rt-dropdown",
                                                    options=standards_list,
                                                    placeholder="Select internal standards...",
                                                    style={"text-align": "left",
                                                           "height": "35px",
                                                           "width": "100%",
                                                           "display": "inline-block"},
                                                ),

                                                # Dropdown for filtering by sample for the RT vs. sample plot
                                                dcc.Dropdown(
                                                    id="QE2-rt-plot-sample-dropdown",
                                                    options=[],
                                                    placeholder="Select samples...",
                                                    style={"text-align": "left",
                                                           "height": "35px",
                                                           "width": "100%",
                                                           "display": "inline-block"},
                                                multi=True),

                                                # Scatter plot of internal standard retention times in QE 2 samples
                                                dcc.Graph(id="QE2-istd-rt-plot")
                                            ]),

                                            html.Div(className="plot-container", children=[

                                                # Dropdown for internal standard intensity plot
                                                dcc.Dropdown(
                                                    id="QE2-istd-intensity-dropdown",
                                                    options=standards_list,
                                                    placeholder="Select internal standard...",
                                                    style={"text-align": "left",
                                                           "height": "35px",
                                                           "width": "100%",
                                                           "display": "inline-block"}),

                                                # Dropdown for filtering by sample for the intensity vs. sample plot
                                                dcc.Dropdown(
                                                    id="QE2-intensity-plot-sample-dropdown",
                                                    options=[],
                                                    placeholder="Select samples...",
                                                    style={"text-align": "left",
                                                           "height": "35px",
                                                           "width": "100%",
                                                           "display": "inline-block"},
                                                multi=True),

                                                # Bar plot of internal standard intensity in QE 2 samples
                                                dcc.Graph(id="QE2-istd-intensity-plot")
                                            ]),

                                            html.Div(className="plot-container", children=[

                                                # Dropdown for internal standard delta m/z plot
                                                dcc.Dropdown(
                                                    id="QE2-istd-mz-dropdown",
                                                    options=standards_list,
                                                    placeholder="Select internal standards...",
                                                    style={"text-align": "left",
                                                           "height": "35px",
                                                           "width": "100%",
                                                           "display": "inline-block"},
                                                ),

                                                # Dropdown for filtering by sample for the delta m/z vs. sample plot
                                                dcc.Dropdown(
                                                    id="QE2-mz-plot-sample-dropdown",
                                                    options=[],
                                                    placeholder="Select samples...",
                                                    style={"text-align": "left",
                                                           "height": "35px",
                                                           "width": "100%",
                                                           "display": "inline-block"},
                                                multi=True),

                                                # Scatter plot of internal standard delta m/z vs. samples
                                                dcc.Graph(id="QE2-istd-mz-plot")
                                            ]),

                                        ]),

                                        html.Div(className="urine-plot-div", children=[

                                            # Scatter plot of QC urine feature retention times from QE 2
                                            html.Div(className="plot-container", children=[
                                                dcc.Graph(id="QE2-urine-rt-plot")
                                            ]),

                                            # Bar plot of QC urine feature peak heights from QE 2
                                            html.Div(className="plot-container", children=[

                                                # Dropdown for urine feature intensity plot
                                                dcc.Dropdown(
                                                    id="QE2-urine-intensity-dropdown",
                                                    options=list(get_pos_urine_features_dict().keys()),
                                                    placeholder="Select urine feature...",
                                                    style={"text-align": "left",
                                                           "height": "35px",
                                                           "width": "100%",
                                                           "display": "inline-block"}
                                                ),

                                                dcc.Graph(id="QE2-urine-intensity-plot", animate=False)
                                            ])
                                        ]),
                                    ]),
                                ]),

                                # Modal/dialog for sample information card
                                dbc.Modal(id="QE2-sample-info-modal", size="xl", centered=True, is_open=False,
                                    scrollable=True, children=[
                                        dbc.ModalHeader(dbc.ModalTitle(id="QE2-sample-modal-title"), close_button=True),
                                        dbc.ModalBody(id="QE2-sample-modal-body"),
                                ]),

                                # Modal/dialog for alerting user that data is loading
                                dbc.Modal(id="QE2-loading-modal", size="md", centered=True, is_open=False,
                                    scrollable=True, keyboard=False, backdrop="static", children=[
                                        dbc.ModalHeader(dbc.ModalTitle(id="QE2-loading-modal-title"), close_button=False),
                                        dbc.ModalBody(id="QE2-loading-modal-body")
                                ]),
                            ]),
                        ]),

                        dcc.Tab(label="Fusion Lumos 1", children=[]),

                        dcc.Tab(label="Fusion Lumos 2", children=[]),

                        dcc.Tab(label="Bruker timsTOF", children=[]),

                    ]),

                ]),
            ]),

            # Storage of all necessary DataFrames in dcc.Store objects
            dcc.Store(id="rt-pos-QE1"),
            dcc.Store(id="rt-neg-QE1"),
            dcc.Store(id="intensity-pos-QE1"),
            dcc.Store(id="intensity-neg-QE1"),
            dcc.Store(id="mz-pos-QE1"),
            dcc.Store(id="mz-neg-QE1"),
            dcc.Store(id="sequence-QE1"),
            dcc.Store(id="metadata-QE1"),
            dcc.Store(id="urine-rt-pos-QE1"),
            dcc.Store(id="urine-rt-neg-QE1"),
            dcc.Store(id="urine-intensity-pos-QE1"),
            dcc.Store(id="urine-intensity-neg-QE1"),
            dcc.Store(id="urine-mz-pos-QE1"),
            dcc.Store(id="urine-mz-neg-QE1"),
            dcc.Store(id="study-resources-QE1"),
            dcc.Store(id="samples-QE1"),

            dcc.Store(id="rt-pos-QE2"),
            dcc.Store(id="rt-neg-QE2"),
            dcc.Store(id="intensity-pos-QE2"),
            dcc.Store(id="intensity-neg-QE2"),
            dcc.Store(id="mz-pos-QE2"),
            dcc.Store(id="mz-neg-QE2"),
            dcc.Store(id="sequence-QE2"),
            dcc.Store(id="metadata-QE2"),
            dcc.Store(id="urine-rt-pos-QE2"),
            dcc.Store(id="urine-rt-neg-QE2"),
            dcc.Store(id="urine-intensity-pos-QE2"),
            dcc.Store(id="urine-intensity-neg-QE2"),
            dcc.Store(id="urine-mz-pos-QE2"),
            dcc.Store(id="urine-mz-neg-QE2"),
            dcc.Store(id="study-resources-QE2"),
            dcc.Store(id="samples-QE2")

        ])

    ])


app.layout = serve_layout


@app.callback(Output("QE1-table", "data"),
              Output("QE2-table", "data"),
              Output("QE1-table-container", "style"),
              Output("QE2-table-container", "style"),
              Output("QE1-plot-container", "style"),
              Output("QE2-plot-container", "style"),
              Input("header", "children"), suppress_callback_exceptions=True)
def populate_study_table(placeholder_input):

    """
    Dash callback for populating tables with list of past/active instrument runs
    """

    df_studies_QE1 = pd.DataFrame()
    df_studies_QE2 = pd.DataFrame()
    df_metadata = pd.DataFrame()

    QE1_studies = {
        "Study": [],
        "Type": [],
        "Status": []
    }

    QE2_studies = {
        "Study": [],
        "Type": [],
        "Status": []
    }

    QE1_files = drive.ListFile({"q": "'" + drive_ids["QE 1"] + "' in parents and trashed=false"}).GetList()
    QE2_files = drive.ListFile({"q": "'" + drive_ids["QE 2"] + "' in parents and trashed=false"}).GetList()

    # Get study name and chromatography
    for file in QE1_files:
        if "RT" in file["title"] and "Pos" in file["title"] and "urine" not in file["title"]:
            QE1_studies["Study"].append(file["title"].split("_")[0])
            QE1_studies["Type"].append(file["title"].split("_")[2])
            QE1_studies["Status"].append("Complete")

    for file in QE2_files:
        if "RT" in file["title"] and "Pos" in file["title"] and "urine" not in file["title"]:
            QE2_studies["Study"].append(file["title"].split("_")[0])
            QE2_studies["Type"].append(file["title"].split("_")[2])
            QE2_studies["Status"].append("Complete")

    df_studies_QE1["Study"] = QE1_studies["Study"]
    df_studies_QE1["Type"] = QE1_studies["Type"]
    df_studies_QE1["Status"] = QE1_studies["Status"]
    QE1_studies = df_studies_QE1.to_dict("records")

    df_studies_QE2["Study"] = QE2_studies["Study"]
    df_studies_QE2["Type"] = QE2_studies["Type"]
    df_studies_QE2["Status"] = QE2_studies["Status"]
    QE2_studies = df_studies_QE2.to_dict("records")

    display_div = {"display": "block"}

    return QE1_studies, QE2_studies, display_div, display_div, display_div, display_div


@app.callback(Output("rt-pos-QE1", "data"),
              Output("rt-neg-QE1", "data"),
              Output("intensity-pos-QE1", "data"),
              Output("intensity-neg-QE1", "data"),
              Output("mz-pos-QE1", "data"),
              Output("mz-neg-QE1", "data"),
              Output("sequence-QE1", "data"),
              Output("metadata-QE1", "data"),
              Output("urine-rt-pos-QE1", "data"),
              Output("urine-rt-neg-QE1", "data"),
              Output("urine-intensity-pos-QE1", "data"),
              Output("urine-intensity-neg-QE1", "data"),
              Output("urine-mz-pos-QE1", "data"),
              Output("urine-mz-neg-QE1", "data"),
              Output("study-resources-QE1", "data"),
              Output("samples-QE1", "data"),
              Input("QE1-table", "active_cell"),
              State("QE1-table", "data"), prevent_initial_call=True, suppress_callback_exceptions=True)
def load_data_for_QE1(active_cell, table_data):

    """
    Stores QC results for QE 1 in dcc.Store objects (user's browser session)
    """

    if active_cell:
        study_id = table_data[active_cell["row"]][active_cell["column_id"]]
        return get_data("QE 1", study_id)


@app.callback(Output("rt-pos-QE2", "data"),
              Output("rt-neg-QE2", "data"),
              Output("intensity-pos-QE2", "data"),
              Output("intensity-neg-QE2", "data"),
              Output("mz-pos-QE2", "data"),
              Output("mz-neg-QE2", "data"),
              Output("sequence-QE2", "data"),
              Output("metadata-QE2", "data"),
              Output("urine-rt-pos-QE2", "data"),
              Output("urine-rt-neg-QE2", "data"),
              Output("urine-intensity-pos-QE2", "data"),
              Output("urine-intensity-neg-QE2", "data"),
              Output("urine-mz-pos-QE2", "data"),
              Output("urine-mz-neg-QE2", "data"),
              Output("study-resources-QE2", "data"),
              Output("samples-QE2", "data"),
              Input("QE2-table", "active_cell"),
              State("QE2-table", "data"), prevent_initial_call=True, suppress_callback_exceptions=True)
def load_data_for_QE2(active_cell, table_data):

    """
    Stores QC results for QE 2 in dcc.Store objects (user's browser session)
    """

    if active_cell:
        study_id = table_data[active_cell["row"]][active_cell["column_id"]]
        return get_data("QE 2", study_id)


@app.callback(Output("QE1-loading-modal", "is_open"),
              Output("QE1-loading-modal-title", "children"),
              Output("QE1-loading-modal-body", "children"),
              Output("QE2-loading-modal", "is_open"),
              Output("QE2-loading-modal-title", "children"),
              Output("QE2-loading-modal-body", "children"),
              Input("QE1-table", "active_cell"),
              State("QE1-table", "data"),
              Input("QE2-table", "active_cell"),
              State("QE2-table", "data"),
              Input("QE1-sample-table", "data"),
              Input("QE2-sample-table", "data"),
              State("QE1-loading-modal", "is_open"),
              State("QE2-loading-modal", "is_open"),
              State("study-resources-QE1", "data"),
              State("study-resources-QE2", "data"), prevent_initial_call=True)
def loading_data_feedback(active_cell_QE1, table_data_QE1, active_cell_QE2, table_data_QE2,
                          placeholder_input_1, placeholder_input_2, modal_is_open_QE1, modal_is_open_QE2,
                          study_resources_QE1, study_resources_QE2):

    """
    Dash callback for providing user feedback when retrieving data from Google Drive
    """

    loading_on = ""
    study_name = ""

    if active_cell_QE1:

        if study_resources_QE1:

            study_name_QE1 = json.loads(study_resources_QE1)["study_name"]

            if table_data_QE1[active_cell_QE1["row"]][active_cell_QE1["column_id"]] != study_name_QE1:
                study_name = table_data_QE1[active_cell_QE1["row"]][active_cell_QE1["column_id"]]
                loading_on = "QE 1"

        else:
            study_name = table_data_QE1[active_cell_QE1["row"]][active_cell_QE1["column_id"]]
            loading_on = "QE 1"

        if modal_is_open_QE1:
            return False, None, None, False, None, None

    if active_cell_QE2:

        if study_resources_QE2:

            study_name_QE2 = json.loads(study_resources_QE2)["study_name"]

            if table_data_QE2[active_cell_QE2["row"]][active_cell_QE2["column_id"]] != study_name_QE2:
                study_name = table_data_QE2[active_cell_QE2["row"]][active_cell_QE2["column_id"]]
                loading_on = "QE 2"

        else:
            study_name = table_data_QE2[active_cell_QE2["row"]][active_cell_QE2["column_id"]]
            loading_on = "QE 2"

        if modal_is_open_QE2:
            return False, None, None, False, None, None

    title = html.Div([
        html.Div(children=[dbc.Spinner(color="primary"), " Loading QC results for " + study_name])
    ])

    body = "This may take a few seconds..."

    if loading_on == "QE 1":
        return True, title, body, False, None, None
    elif loading_on == "QE 2":
        return False, None, None, True, title, body


@app.callback(Output("QE1-sample-table", "data"),
              Input("samples-QE1", "data"), prevent_initial_call=True)
def populate_QE1_sample_tables(samples):

    """
    Populates table with list of samples for selected study from QE 1 instrument table
    """

    df_samples = pd.read_json(samples, orient="split")
    return df_samples.to_dict("records")


@app.callback(Output("QE2-sample-table", "data"),
              Input("samples-QE2", "data"), prevent_initial_call=True)
def populate_QE2_sample_tables(samples):

    """
    Populates table with list of samples for selected study from QE 2 instrument table
    """

    df_samples = pd.read_json(samples, orient="split")
    return df_samples.to_dict("records")


@app.callback(Output("QE1-istd-rt-dropdown", "options"),
              Output("QE1-istd-mz-dropdown", "options"),
              Output("QE1-istd-intensity-dropdown", "options"),
              Output("QE1-urine-intensity-dropdown", "options"),
              Output("QE1-rt-plot-sample-dropdown", "options"),
              Output("QE1-mz-plot-sample-dropdown", "options"),
              Output("QE1-intensity-plot-sample-dropdown", "options"),
              Input("QE1-polarity-options", "value"),
              Input("QE1-sample-table", "data"),
              State("study-resources-QE1", "data"),
              State("samples-QE1", "data"), prevent_initial_call=True)
def update_QE1_dropdowns_on_polarity_change(polarity, table_data, study_resources, samples):

    """
    Updates QE 1 dropdown lists with correct items for user-selected polarity
    """

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


@app.callback(Output("QE2-istd-rt-dropdown", "options"),
              Output("QE2-istd-intensity-dropdown", "options"),
              Output("QE2-istd-mz-dropdown", "options"),
              Output("QE2-urine-intensity-dropdown", "options"),
              Output("QE2-rt-plot-sample-dropdown", "options"),
              Output("QE2-mz-plot-sample-dropdown", "options"),
              Output("QE2-intensity-plot-sample-dropdown", "options"),
              Input("QE2-polarity-options", "value"),
              Input("QE2-sample-table", "data"),
              State("study-resources-QE2", "data"),
              State("samples-QE2", "data"), prevent_initial_call=True)
def update_QE2_dropdowns_on_polarity_change(polarity, table_data, study_resources, samples):

    """
    Updates QE 2 dropdown lists with correct items for user-selected polarity
    """

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


@app.callback(Output("QE1-rt-plot-sample-dropdown", "value"),
              Output("QE1-mz-plot-sample-dropdown", "value"),
              Output("QE1-intensity-plot-sample-dropdown", "value"),
              Input("QE1-sample-filtering-options", "value"),
              Input("QE1-polarity-options", "value"),
              Input("samples-QE1", "data"),
              Input("metadata-QE1", "data"), prevent_initial_call=True)
def apply_sample_filter_to_QE1_plots(filter, polarity, samples, metadata):

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


@app.callback(Output("QE2-rt-plot-sample-dropdown", "value"),
              Output("QE2-mz-plot-sample-dropdown", "value"),
              Output("QE2-intensity-plot-sample-dropdown", "value"),
              Input("QE2-sample-filtering-options", "value"),
              Input("QE2-polarity-options", "value"),
              Input("samples-QE2", "data"),
              Input("metadata-QE2", "data"), prevent_initial_call=True)
def apply_sample_filter_to_QE2_plots(filter, polarity, samples, metadata):

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


@app.callback(Output("QE1-istd-rt-plot", "figure"),
              Output("QE1-istd-intensity-plot", "figure"),
              Output("QE1-istd-mz-plot", "figure"),
              Output("QE1-urine-rt-plot", "figure"),
              Output("QE1-urine-intensity-plot", "figure"),
              Output("QE1-urine-intensity-dropdown", "value"),
              Output("QE1-urine-rt-plot", "clickData"),
              Input("QE1-polarity-options", "value"),
              Input("QE1-istd-rt-dropdown", "value"),
              Input("QE1-istd-intensity-dropdown", "value"),
              Input("QE1-istd-mz-dropdown", "value"),
              Input("QE1-urine-intensity-dropdown", "value"),
              Input("QE1-urine-rt-plot", "clickData"),
              Input("QE1-rt-plot-sample-dropdown", "value"),
              Input("QE1-intensity-plot-sample-dropdown", "value"),
              Input("QE1-mz-plot-sample-dropdown", "value"),
              State("rt-pos-QE1", "data"),
              State("rt-neg-QE1", "data"),
              State("intensity-pos-QE1", "data"),
              State("intensity-neg-QE1", "data"),
              State("mz-pos-QE1", "data"),
              State("mz-neg-QE1", "data"),
              State("sequence-QE1", "data"),
              State("metadata-QE1", "data"),
              State("urine-rt-pos-QE1", "data"),
              State("urine-rt-neg-QE1", "data"),
              State("urine-intensity-pos-QE1", "data"),
              State("urine-intensity-neg-QE1", "data"),
              State("urine-mz-pos-QE1", "data"),
              State("urine-mz-neg-QE1", "data"),
              Input("study-resources-QE1", "data"),
              State("samples-QE1", "data"), prevent_initial_call=True)
def populate_QE1_plots(polarity, rt_plot_standard, intensity_plot_standard, mz_plot_standard,
                       urine_plot_feature, click_data, rt_plot_samples, intensity_plot_samples, mz_plot_samples,
                       rt_pos, rt_neg, intensity_pos, intensity_neg, mz_pos, mz_neg, sequence,
                       metadata, urine_rt_pos, urine_rt_neg, urine_intensity_pos, urine_intensity_neg,
                       urine_mz_pos, urine_mz_neg, study_resources, samples):

    """
    Dash callback for loading QE 1 instrument data into scatter and bar plots
    """

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
        return dash.no_update


@app.callback(Output("QE2-istd-rt-plot", "figure"),
              Output("QE2-istd-intensity-plot", "figure"),
              Output("QE2-istd-mz-plot", "figure"),
              Output("QE2-urine-rt-plot", "figure"),
              Output("QE2-urine-intensity-plot", "figure"),
              Output("QE2-urine-intensity-dropdown", "value"),
              Output("QE2-urine-rt-plot", "clickData"),
              Input("QE2-polarity-options", "value"),
              Input("QE2-istd-rt-dropdown", "value"),
              Input("QE2-istd-intensity-dropdown", "value"),
              Input("QE2-istd-mz-dropdown", "value"),
              Input("QE2-urine-intensity-dropdown", "value"),
              Input("QE2-urine-rt-plot", "clickData"),
              Input("QE2-rt-plot-sample-dropdown", "value"),
              Input("QE2-intensity-plot-sample-dropdown", "value"),
              Input("QE2-mz-plot-sample-dropdown", "value"),
              State("rt-pos-QE2", "data"),
              State("rt-neg-QE2", "data"),
              State("intensity-pos-QE2", "data"),
              State("intensity-neg-QE2", "data"),
              State("mz-pos-QE2", "data"),
              State("mz-neg-QE2", "data"),
              State("sequence-QE2", "data"),
              State("metadata-QE2", "data"),
              State("urine-rt-pos-QE2", "data"),
              State("urine-rt-neg-QE2", "data"),
              State("urine-intensity-pos-QE2", "data"),
              State("urine-intensity-neg-QE2", "data"),
              State("urine-mz-pos-QE2", "data"),
              State("urine-mz-neg-QE2", "data"),
              Input("study-resources-QE2", "data"),
              State("samples-QE2", "data"), prevent_initial_call=True)
def populate_QE2_plots(polarity, rt_plot_standard, intensity_plot_standard, mz_plot_standard,
                       urine_plot_feature, click_data, rt_plot_samples, intensity_plot_samples, mz_plot_samples,
                       rt_pos, rt_neg, intensity_pos, intensity_neg, mz_pos, mz_neg, sequence,
                       metadata, urine_rt_pos, urine_rt_neg, urine_intensity_pos, urine_intensity_neg,
                       urine_mz_pos, urine_mz_neg, study_resources, samples):

    """
    Dash callback for loading QE 2 instrument data into scatter and bar plots
    """

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
        return dash.no_update


@app.callback(Output("QE1-sample-info-modal", "is_open"),
              Output("QE1-sample-modal-title", "children"),
              Output("QE1-sample-modal-body", "children"),
              Output("QE1-sample-table", "active_cell"),
              Output("QE1-istd-rt-plot", "clickData"),
              Output("QE1-istd-intensity-plot", "clickData"),
              Output("QE1-istd-mz-plot", "clickData"),
              State("QE1-sample-info-modal", "is_open"),
              Input("QE1-sample-table", "active_cell"),
              State("QE1-sample-table", "data"),
              Input("QE1-istd-rt-plot", "clickData"),
              Input("QE1-istd-intensity-plot", "clickData"),
              Input("QE1-istd-mz-plot", "clickData"),
              State("rt-pos-QE1", "data"),
              State("rt-neg-QE1", "data"),
              State("intensity-pos-QE1", "data"),
              State("intensity-neg-QE1", "data"),
              State("mz-pos-QE1", "data"),
              State("mz-neg-QE1", "data"),
              State("sequence-QE1", "data"),
              State("metadata-QE1", "data"), prevent_initial_call=True)
def toggle_sample_card_for_QE1(is_open, active_cell, table_data, rt_click, intensity_click, mz_click,
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


@app.callback(Output("QE2-sample-info-modal", "is_open"),
              Output("QE2-sample-modal-title", "children"),
              Output("QE2-sample-modal-body", "children"),
              Output("QE2-sample-table", "active_cell"),
              Output("QE2-istd-rt-plot", "clickData"),
              Output("QE2-istd-intensity-plot", "clickData"),
              Output("QE2-istd-mz-plot", "clickData"),
              State("QE2-sample-info-modal", "is_open"),
              Input("QE2-sample-table", "active_cell"),
              State("QE2-sample-table", "data"),
              Input("QE2-istd-rt-plot", "clickData"),
              Input("QE2-istd-intensity-plot", "clickData"),
              Input("QE2-istd-mz-plot", "clickData"),
              State("rt-pos-QE2", "data"),
              State("rt-neg-QE2", "data"),
              State("intensity-pos-QE2", "data"),
              State("intensity-neg-QE2", "data"),
              State("mz-pos-QE2", "data"),
              State("mz-neg-QE2", "data"),
              State("sequence-QE2", "data"),
              State("metadata-QE2", "data"), prevent_initial_call=True)
def toggle_sample_card_for_QE2(is_open, active_cell, table_data, rt_click, intensity_click, mz_click,
                               rt_pos, rt_neg, intensity_pos, intensity_neg, mz_pos, mz_neg, sequence, metadata):

    """
    Opens information modal when a sample is clicked from the sample table
    """

    # Get selected sample
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


if __name__ == "__main__":

    # if sys.platform == "win32":
    #     chrome_path = "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe"
    #     webbrowser.register("chrome", None, webbrowser.BackgroundBrowser(chrome_path))
    #     webbrowser.get("chrome").open("http://127.0.0.1:8050/")
    # elif sys.platform == "darwin":
    #     webbrowser.get("chrome").open("http://127.0.0.1:8050/", new=1)

    # Start Dash app
    app.run_server(debug=False)