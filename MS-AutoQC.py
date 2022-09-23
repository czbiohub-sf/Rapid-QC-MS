import sys, webbrowser, json
import pandas as pd
from dash import dash, dcc, html, dash_table, Input, Output, State
import dash_bootstrap_components as dbc
from QCPlotGeneration import *

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

                    dbc.Row(justify="center", children=[

                        # Tabs to switch between instruments
                        dcc.Tabs(id="tabs", className="instrument-tabs", children=[

                            dcc.Tab(label="Thermo QE 1", value="QE 1"),

                            dcc.Tab(label="Thermo QE 2", value="QE 2"),

                            dcc.Tab(label="Fusion Lumos 1", value="Lumos 1"),

                            dcc.Tab(label="Fusion Lumos 2", value="Lumos 2"),

                            dcc.Tab(label="Bruker timsTOF", value="timsTOF"),

                        ]),

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

                                # Polarity filtering options
                                html.Div(className="radio-group-container", children=[
                                    html.Div(className="radio-group", children=[
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
                                    html.Div(className="radio-group", children=[
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

                        # Modal/dialog for sample information card
                        dbc.Modal(id="sample-info-modal", size="xl", centered=True, is_open=False,
                                  scrollable=True, children=[
                            dbc.ModalHeader(dbc.ModalTitle(id="sample-modal-title"), close_button=True),
                            dbc.ModalBody(id="sample-modal-body")
                        ]),

                        # Modal/dialog for alerting user that data is loading
                        dbc.Modal(id="loading-modal", size="md", centered=True, is_open=False, scrollable=True,
                                  keyboard=False, backdrop="static", children=[
                            dbc.ModalHeader(dbc.ModalTitle(id="loading-modal-title"), close_button=False),
                            dbc.ModalBody(id="loading-modal-body")
                        ]),
                    ]),
                ]),
            ]),

            # Storage of all necessary DataFrames in dcc.Store objects
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
            dcc.Store(id="url"),
            dcc.Store(id="first-study")

        ])

    ])


app.layout = serve_layout


@app.callback(Output("tabs", "value"),
              Input("url", "data"))
def get_instrument_tabs(url):

    """
    TODO: Retrieves all instruments on a user installation of MS-AutoQC
    """

    return "QE 1"


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


if __name__ == "__main__":

    # if sys.platform == "win32":
    #     chrome_path = "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe"
    #     webbrowser.register("chrome", None, webbrowser.BackgroundBrowser(chrome_path))
    #     webbrowser.get("chrome").open("http://127.0.0.1:8050/")
    # elif sys.platform == "darwin":
    #     webbrowser.get("chrome").open("http://127.0.0.1:8050/", new=1)

    # Start Dash app
    app.run_server(debug=True)