import os, sys, webbrowser
import pandas as pd
import numpy as np
import plotly.express as px
from dash import dash, dcc, html, dash_table, Input, Output, State

study_loaded = {
    "QE 1": {
        "study_name": "",
        "study_file": ""
    },
    "QE 2": {
        "study_name": "",
        "study_file": ""
    },
}

standards_list = ["1_Methionine_d8", "1_1_Methylnicotinamide_d3", "1_Creatinine_d3", "1_Carnitine_d3",
             "1_Acetylcarnitine_d3", "1_TMAO_d9", "1_Choline_d9", "1_Glutamine_d5", "1_CUDA", "1_Glutamic Acid_d3",
             "1_Arginine_d7", "1_Alanine_d3", "1_Valine d8", "1_Tryptophan d5", "1_Serine d3", "1_Lysine d8",
             "1_Phenylalanine d8", "1_Hippuric acid d5"]

neg_urine_features_list = ["AEMOLEFTQBMNLQ-AQKNRBDQSA-N", "AKEUNCKRJATALU-UHFFFAOYSA-N", "ALRHLSYJTWAHJZ-UHFFFAOYSA-N",
                           "AQTYXAPIHMXAAV-UHFFFAOYSA-N", "AYFVYJQAPQTCCC-UHFFFAOYSA-N", "BTJIUGUIPKRLHP-UHFFFAOYSA-N",
                           "BXFFHSIDQOFMLE-UHFFFAOYSA-N", "BYXCFUMGEBZDDI-UHFFFAOYSA-N", "CBQJSKKFNMDLON-UHFFFAOYSA-N",
                           "CCVYRRGZDBSHFU-UHFFFAOYSA-N", "CGFRVKXGZRODPA-UHFFFAOYSA-N", "COLNVLDHVKWLRT-UHFFFAOYSA-N",
                           "CZMRCDWAGMRECN-UGDNZRGBSA-N", "CZWCKYRVOZZJNM-USOAJAOKSA-N", "DCICDMMXFIELDF-UHFFFAOYSA-N",
                           "DCXYFEDJOCDNAF-REOHCLBHSA-N", "DDRJAANPRJIHGJ-UHFFFAOYSA-N", "DRTQHJPVMGBUCF-XVFCMESISA-N",
                           "DTERQYGMUDWYAZ-ZETCQYMHSA-N", "DUUGKQCEGZLZNO-UHFFFAOYSA-N", "FDGQSTZJBFJUBT-UHFFFAOYSA-N",
                           "FJKROLUGYXJWQN-UHFFFAOYSA-N", "GHOKWGTUZJEAQD-UHFFFAOYSA-N", "HCZHHEIFKROPDY-UHFFFAOYSA-N",
                           "HEBKCHPVOIAQTA-QWWZWVQMSA-N", "HNDVDQJCIGZPNO-YFKPBYRVSA-N", "HNEGQIOMVPPMNR-IHWYPQMZSA-N",
                           "IGMNYECMUMZDDF-UHFFFAOYSA-N", "ILGMGHZPXRDCCS-UHFFFAOYSA-N", "JDHILDINMRGULE-UHFFFAOYSA-N",
                           "JFCQEDHGNNZCLN-UHFFFAOYSA-N", "JFLIEFSWGNOPJJ-JTQLQIEISA-N", "JVGVDSSUAVXRDY-UHFFFAOYSA-N",
                           "JVTAAEKCZFNVCJ-UHFFFAOYSA-N", "KBOJOGQFRVVWBH-ZETCQYMHSA-N", "KDYFGRWQOYBRFD-UHFFFAOYSA-N",
                           "KSPQDMRTZZYQLM-UHFFFAOYSA-N", "KTHDTJVBEPMMGL-UHFFFAOYSA-N", "LOIYMIARKYCTBW-OWOJBTEDSA-N",
                           "LXVSANCQXSSLPA-UHFFFAOYSA-N", "MTCFGRXMJLQNBG-UHFFFAOYSA-N", "MYYIAHXIVFADCU-QMMMGPOBSA-N",
                           "NBIIXXVUZAFLBC-UHFFFAOYSA-N", "NIDVTARKFBZMOT-PEBGCTIMSA-N", "NOFNCLGCUJJPKU-UHFFFAOYSA-N",
                           "NWGZOALPWZDXNG-UHFFFAOYSA-N", "ONPXCLZMBSJLSP-CSMHCCOUSA-N", "PMOWTIHVNWZYFI-AATRIKPKSA-N",
                           "POJWUDADGALRAB-UHFFFAOYSA-N", "PTJWIQPHWPFNBW-GBNDHIKLSA-N", "PXQPEWDEAKTCGB-UHFFFAOYSA-N",
                           "PYUSHNKNPOHWEZ-YFKPBYRVSA-N", "QFDRTQONISXGJA-UHFFFAOYSA-N", "QIVBCDIJIAJPQS-UHFFFAOYSA-N",
                           "QVWAEZJXDYOKEH-UHFFFAOYSA-N", "RFCQJGFZUQFYRF-UHFFFAOYSA-N", "RSPURTUNRHNVGF-IOSLPCCCSA-N",
                           "RWSXRVCMGQZWBV-UHFFFAOYSA-N", "SQVRNKJHWKZAKO-LUWBGTNYSA-N", "SXUXMRMBWZCMEN-ZOQUXTDFSA-N",
                           "TYFQFVWCELRYAO-UHFFFAOYSA-N", "UTAIYTHAJQNQDW-KQYNXXCUSA-N", "UZTFMUBKZQVKLK-UHFFFAOYSA-N",
                           "VBUYCZFBVCCYFD-NUNKFHFFSA-N", "VVHOUVWJCQOYGG-UHFFFAOYSA-N", "VZCYOOQTPOCHFL-UPHRSURJSA-N",
                           "WGNAKZGUSRVWRH-UHFFFAOYSA-N", "WHUUTDBJXJRKMK-UHFFFAOYSA-N", "WLJVNTCWHIRURA-UHFFFAOYSA-N",
                           "WNLRTRBMVRJNCN-UHFFFAOYSA-N", "WRUSVQOKJIDBLP-HWKANZROSA-N", "WXNXCEHXYPACJF-UHFFFAOYSA-N",
                           "WXTMDXOMEHJXQO-UHFFFAOYSA-N", "XGILAAMKEQUXLS-UHFFFAOYSA-N", "XLBVNMSMFQMKEY-BYPYZUCNSA-N",
                           "XOAAWQZATWQOTB-UHFFFAOYSA-N", "XUYPXLNMDZIRQH-UHFFFAOYSA-N", "YGSDEFSMJLZEOE-UHFFFAOYSA-N",
                           "ZDXPYRJPNDTMRX-UHFFFAOYSA-N", "ZFXYFBGIUFBOJW-UHFFFAOYSA-N", "ZMHLUFWWWPBTIU-UHFFFAOYSA-N"]

pos_urine_features_list = ["AGPKZVBTJJNPAG-UHFFFAOYSA-N", "AUNGANRZJHBGPY-SCRDCRAPSA-N", "AYFVYJQAPQTCCC-UHFFFAOYSA-N",
                           "BPMFZUMJYQTVII-UHFFFAOYSA-N", "COLNVLDHVKWLRT-UHFFFAOYSA-N", "CQOVPNPJLQNMDC-ZETCQYMHSA-N",
                           "CVSVTCORWBXHQV-UHFFFAOYSA-N", "CWLQUGTUXBXTLF-UHFFFAOYSA-N", "CYZKJBZEIFWZSR-LURJTMIESA-N",
                           "CZMRCDWAGMRECN-UGDNZRGBSA-N", "DCXYFEDJOCDNAF-REOHCLBHSA-N", "DDRJAANPRJIHGJ-UHFFFAOYSA-N",
                           "DFPAKSUCGFBDDF-UHFFFAOYSA-N", "DTERQYGMUDWYAZ-ZETCQYMHSA-N", "DZGWFCGJZKJUFP-UHFFFAOYSA-N",
                           "DZTHIGRZJZPRDV-GFCCVEGCSA-N", "FDGQSTZJBFJUBT-UHFFFAOYSA-N", "FEMXZDUTFRTWPE-UHFFFAOYSA-N",
                           "FONIWJIDLJEJTL-UHFFFAOYSA-N", "GFFGJBXGBJISGV-UHFFFAOYSA-N", "GFYLSDSUCHVORB-IOSLPCCCSA-N",
                           "GHOKWGTUZJEAQD-UHFFFAOYSA-N", "GUBGYTABKSRVRQ-QUYVBRFLSA-N", "HCZHHEIFKROPDY-UHFFFAOYSA-N",
                           "HJSLFCCWAKVHIW-UHFFFAOYSA-N", "HNDVDQJCIGZPNO-YFKPBYRVSA-N", "HNXQXTQTPAJEJL-UHFFFAOYSA-N",
                           "HZAXFHJVJLSVMW-UHFFFAOYSA-N", "IAZDPXIOMUYVGZ-UHFFFAOYSA-N", "JDHILDINMRGULE-UHFFFAOYSA-N",
                           "JFLIEFSWGNOPJJ-JTQLQIEISA-N", "JSJWCHRYRHKBBW-UHFFFAOYSA-N", "JZRWCGZRTZMZEH-UHFFFAOYSA-N",
                           "KBOJOGQFRVVWBH-ZETCQYMHSA-N", "KDXKERNSBIXSRK-YFKPBYRVSA-N", "KSPIYJQBLVDRRI-WDSKDSINSA-N",
                           "KSPQDMRTZZYQLM-UHFFFAOYSA-N", "KWIUHFFTVRNATP-UHFFFAOYSA-N", "LDHMAVIPBRSVRG-UHFFFAOYSA-O",
                           "LEVWYRKDKASIDU-IMJSIDKUSA-N", "LMIQERWZRIFWNZ-UHFFFAOYSA-N", "LNQVTSROQXJCDD-KQYNXXCUSA-N",
                           "LOIYMIARKYCTBW-OWOJBTEDSA-N", "MEFKEPWMEQBLKI-AIRLBKTGSA-N", "MTCFGRXMJLQNBG-UHFFFAOYSA-N",
                           "MXNRLFUSFKVQSK-UHFFFAOYSA-N", "MYYIAHXIVFADCU-QMMMGPOBSA-N", "NIDVTARKFBZMOT-PEBGCTIMSA-N",
                           "NOFNCLGCUJJPKU-UHFFFAOYSA-N", "NTYJJOPFIAHURM-UHFFFAOYSA-N", "NWGZOALPWZDXNG-UHFFFAOYSA-N",
                           "ODKSFYDXXFIFQN-BYPYZUCNSA-N", "OEYIOHPDSNJKLS-UHFFFAOYSA-N", "OIRDTQYFTABQOQ-KQYNXXCUSA-N",
                           "OIVLITBTBDPEFK-UHFFFAOYSA-N", "ONPXCLZMBSJLSP-CSMHCCOUSA-N", "OPVPGKGADVGKTG-UHFFFAOYSA-N",
                           "PFNFFQXMRSDOHW-UHFFFAOYSA-N", "PFWLFWPASULGAN-UHFFFAOYSA-N", "PFWQSHXPNKRLIV-UHFFFAOYSA-N",
                           "PHIQHXFUZVPYII-UHFFFAOYSA-N", "PMZDQRJGMBOQBF-UHFFFAOYSA-N", "PQNASZJZHFPQLE-UHFFFAOYSA-N",
                           "PRJKNHOMHKJCEJ-UHFFFAOYSA-N", "PWKSKIMOESPYIA-BYPYZUCNSA-N", "QFDRTQONISXGJA-UHFFFAOYSA-N",
                           "QIAFMBKCNZACKA-UHFFFAOYSA-N", "QIVBCDIJIAJPQS-UHFFFAOYSA-N", "RDHQFKQIGNGIED-UHFFFAOYSA-N",
                           "RDPUKVRQKWBSPK-ZOQUXTDFSA-N", "ROHFNLRQFUQHCH-UHFFFAOYSA-N", "RSPURTUNRHNVGF-IOSLPCCCSA-N",
                           "RWSXRVCMGQZWBV-UHFFFAOYSA-N", "RYYVLZVUVIJVGH-UHFFFAOYSA-N", "RZJCFLSPBDUNDH-ZOQUXTDFSA-N",
                           "SLEHROROQDYRAW-KQYNXXCUSA-N", "SNCKGJWJABDZHI-ZKWXMUAHSA-N", "SUHOQUVVVLNYQR-MRVPVSSYSA-N",
                           "SZJNCZMRZAUNQT-IUCAKERBSA-N", "TUHVEAJXIMEOSA-UHFFFAOYSA-N", "UFAHZIUFPNSHSL-MRVPVSSYSA-N",
                           "UTAIYTHAJQNQDW-KQYNXXCUSA-N", "UYPYRKYUKCHHIB-UHFFFAOYSA-N", "UYTPUPDQBNUYGX-UHFFFAOYSA-N",
                           "VEYYWZRYIYDQJM-ZETCQYMHSA-N", "VQAYFKKCNSOZKM-IOSLPCCCSA-N", "VVHOUVWJCQOYGG-UHFFFAOYSA-N",
                           "WHUUTDBJXJRKMK-UHFFFAOYSA-N", "WUUGFSXJNOTRMR-IOSLPCCCSA-N", "WWNNZCOKKKDOPX-UHFFFAOYSA-N",
                           "XGEGHDBEHXKFPX-UHFFFAOYSA-N", "XIGSAGMEBXLVJJ-UHFFFAOYSA-N", "XJODGRWDFZVTKW-LURJTMIESA-N",
                           "XJWPISBUKWZALE-UHFFFAOYSA-N", "XOAAWQZATWQOTB-UHFFFAOYSA-N", "XSQUKJJJFZCRTK-UHFFFAOYSA-N",
                           "XXEWFEBMSGLYBY-ZETCQYMHSA-N", "ZDXPYRJPNDTMRX-UHFFFAOYSA-N", "ZFXYFBGIUFBOJW-UHFFFAOYSA-N",
                           "ZJUKTBDSGOFHSH-WFMPWKQPSA-N"]

# Define styles for Dash app
external_stylesheets = [
    {
        "href": "https://fonts.googleapis.com/css2?"
                "family=Lato:wght@400;700&display=swap",
        "rel": "stylesheet",
    },
]

# Initialize Dash app
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.title = "MS-AutoQC"

# Color mapping for internal standards and urine features
istd_colors = {"1_Methionine_d8": "rgb(150, 222, 209)",
               "1_1_Methylnicotinamide_d3": "rgb(135, 206, 235)",
               "1_Creatinine_d3": "rgb(233, 116, 81)",
               "1_Carnitine_d3": "rgb(80, 200, 120)",
               "1_Acetylcarnitine_d3": "rgb(242, 140, 40)",
               "1_TMAO_d9": "rgb(255, 127, 80)",
               "1_Choline_d9": "rgb(227, 115, 131)",
               "1_Glutamine_d5": "rgb(187, 143, 206)",
               "1_CUDA": "rgb(187, 143, 206)",
               "1_Glutamic Acid_d3": "rgb(255, 192, 0)",
               " 1_Arginine_d7": "rgb(93, 173, 226)",
               "1_Alanine_d3": "rgb(155, 89, 182)",
               "1_Valine d8": "rgb(86, 101, 115)",
               "1_Tryptophan d5": "rgb(26, 188, 156)",
               "1_Serine d3": "rgb(204, 204, 255)",
               "1_Lysine d8": "rgb(230, 126, 34)",
               "1_Phenylalanine d8": "rgb(41, 128, 185)",
               "1_Hippuric acid d5": "rgb(29, 131, 72)"}

# Create Dash app layout
app.layout = html.Div(className="app-layout", children=[

    # Header
    html.Div(id="header", className="header", children=[
        html.H1(children="MS-AutoQC")
    ]),

    # App layout
    html.Div(className="page", children=[

        dcc.Tabs(id="tabs", children=[

            # QC dashboard for QE 1
            dcc.Tab(label="QE 1", children=[

                html.Div(id="QE1-table-container", className="table-container", children=[

                    # Table of past/active studies that were run on QE 1
                    dash_table.DataTable(id="QE1-table", page_action="none",
                        fixed_rows={'headers': True}, cell_selectable=True,
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
                                    "overflowY": "auto"}
                        ),

                    # Polarity filtering options
                    dcc.RadioItems(
                        id="QE1-polarity-options",
                        options=[
                            {"label": "Positive Mode", "value": "pos"},
                            {"label": "Negative Mode", "value": "neg"}],
                        value="pos",
                        style={"margin-top": "30px",
                               "width": "90%",
                               "margin-left": "5%",
                               "margin-right": "5%"},
                        inputStyle={"margin-left": "15px",
                                    "margin-right": "5px"}),

                    # Table of samples run for a particular study
                    dash_table.DataTable(id="QE1-sample-table", page_action="none",
                        fixed_rows={'headers': True}, cell_selectable=True,
                        style_cell={"textAlign": "left",
                                    "fontSize": "15px",
                                    "fontFamily": "sans-serif",
                                    "lineHeight": "25px",
                                    "padding": "10px",
                                    "borderRadius": "5px"},
                        style_data={"whiteSpace": "normal",
                                    "textOverflow": "ellipsis",
                                    "maxWidth": 0},
                        style_table={"height": "100%",
                                    "overflowY": "auto"}
                        )
                ]),

                # Data container for QE 1 plots
                html.Div(id="QE1-plot-container", className="all-plots-container", children=[

                    html.Div(className="istd-plot-div", children=[

                        html.Div(className="istd-plot-container", children=[

                            dcc.Dropdown(
                                id="QE1-istd-rt-dropdown",
                                className="QE1-istd-dropdown",
                                options=standards_list,
                                placeholder="Select internal standards...",
                                style={"text-align": "left",
                                       "height": "35px",
                                       "width": "100%",
                                       "display": "inline-block"},
                                multi=True),

                            # Scatter plot of internal standard retention times in QE 1 samples
                            dcc.Graph(id="QE1-istd-rt-plot", animate=True)
                        ]),

                        html.Div(className="istd-plot-container", children=[

                            dcc.Dropdown(
                                id="QE1-istd-intensity-dropdown",
                                className="istd-dropdown",
                                options=standards_list,
                                placeholder="Select internal standard...",
                                style={"text-align": "left",
                                       "height": "35px",
                                       "width": "100%",
                                       "display": "inline-block"}),

                            # Bar plot of internal standard intensity in QE 1 samples
                            dcc.Graph(id="QE1-istd-intensity-plot", animate=False)
                        ])

                    ]),

                    html.Div(className="urine-plot-div", children=[

                        # Scatter plot of QC urine feature retention times from QE 1
                        html.Div(className="plot-container", children=[
                            dcc.Graph(id="QE1-urine-rt-plot", animate=True)
                        ]),

                        # Bar plot of QC urine feature peak heights from QE 1
                        html.Div(className="plot-container", children=[
                            dcc.Graph(id="QE1-urine-intensity-plot", animate=False)
                        ])

                    ])

                ]),

            ]),

            # QC dashboard for QE 2
            dcc.Tab(label="QE 2", children=[

                html.Div(id="QE2-table-container", className="table-container", children=[

                    # Table of past/active studies that were run on QE 2
                    dash_table.DataTable(id="QE2-table", page_action="none",
                         fixed_rows={'headers': True}, cell_selectable=True,
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
                                      "overflowY": "auto"}
                     ),

                    # Polarity filtering options
                    dcc.RadioItems(
                        id="QE2-polarity-options",
                        options=[
                            {"label": "Positive Mode", "value": "pos"},
                            {"label": "Negative Mode", "value": "neg"}],
                        value="pos",
                        style={"margin-top": "30px",
                               "width": "90%",
                               "margin-left": "5%",
                               "margin-right": "5%"},
                        inputStyle={"margin-left": "15px",
                                    "margin-right": "5px"}),

                    # Table of samples run for a particular study
                    dash_table.DataTable(id="QE2-sample-table", page_action="none",
                        fixed_rows={'headers': True}, cell_selectable=True,
                        style_cell={"textAlign": "left",
                                    "fontSize": "15px",
                                    "fontFamily": "sans-serif",
                                    "lineHeight": "25px",
                                    "padding": "10px",
                                    "borderRadius": "5px"},
                        style_data={"whiteSpace": "normal",
                                    "textOverflow": "ellipsis",
                                    "maxWidth": 0},
                        style_table={"height": "100%",
                                    "overflowY": "auto"}
                        )
                ]),

                # Data container for Dash plots
                html.Div(id="QE2-plot-container", className="all-plots-container", children=[

                    html.Div(className="istd-plot-div", children=[

                        html.Div(className="istd-plot-container", children=[

                            dcc.Dropdown(
                                id="QE2-istd-rt-dropdown",
                                className="istd-dropdown",
                                options=standards_list,
                                placeholder="Select internal standards...",
                                style={"text-align": "left",
                                       "height": "35px",
                                       "width": "100%",
                                       "display": "inline-block"},
                                multi=True),

                            # Scatter plot of internal standard retention times in QE 2 samples
                            dcc.Graph(id="QE2-istd-rt-plot", animate=True)
                        ]),

                        html.Div(className="istd-plot-container", children=[

                            dcc.Dropdown(
                                id="QE2-istd-intensity-dropdown",
                                className="QE2-istd-dropdown",
                                options=standards_list,
                                placeholder="Select internal standard...",
                                style={"text-align": "left",
                                       "height": "35px",
                                       "width": "100%",
                                       "display": "inline-block"}),

                            # Bar plot of internal standard intensity in QE 2 samples
                            dcc.Graph(id="QE2-istd-intensity-plot", animate=False)
                        ]),

                    ]),

                    html.Div(className="urine-plot-div", children=[

                        # Scatter plot of QC urine feature retention times from QE 2
                        html.Div(className="plot-container", children=[
                            dcc.Graph(id="QE2-urine-rt-plot", animate=True)
                        ]),

                        # Bar plot of QC urine feature peak heights from QE 2
                        html.Div(className="plot-container", children=[
                            dcc.Graph(id="QE2-urine-intensity-plot", animate=False)
                        ])

                    ]),

                ]),

            ]),

            dcc.Tab(label="Fusion Lumos 1", children=[
                html.H2("Construction in progress")
            ]),

            dcc.Tab(label="Fusion Lumos 2", children=[
                html.H2("Construction in progress")
            ]),

            dcc.Tab(label="timsTOF", children=[
                html.H2("Construction in progress")
            ]),

        ])

    ])

])


def get_data(location, study_id):

    """
    Loads internal standard and urine m/z, RT, and peak height from instrument run into pandas DataFrames
    """

    # TODO: Get chromatography from study
    chromatography = "HILIC"

    # Overarching try/catch
    try:

        # Parse data into pandas DataFrames
        try:
            # Retrieve m/z, RT, and peak height .csv files from bufferbox2
            df_mz_pos = pd.read_csv(location + study_id + "_MZ_" + chromatography + "_Pos.csv", index_col=False)
            df_rt_pos = pd.read_csv(location + study_id + "_RT_" + chromatography + "_Pos.csv", index_col=False)
            df_intensity_pos = pd.read_csv(location + study_id + "_PeakHeight_" + chromatography + "_Pos.csv", index_col=False)
            df_urine_mz_pos = pd.read_csv(location + "urine_MZ_" + chromatography + "_Pos.csv", index_col=False)
            df_urine_rt_pos = pd.read_csv(location + "urine_RT_" + chromatography + "_Pos.csv", index_col=False)
            df_urine_intensity_pos = pd.read_csv(location + "urine_PeakHeight_" + chromatography + "_Pos.csv", index_col=False)

            df_mz_neg = pd.read_csv(location + study_id + "_MZ_" + chromatography + "_Neg.csv", index_col=False)
            df_rt_neg = pd.read_csv(location + study_id + "_RT_" + chromatography + "_Neg.csv", index_col=False)
            df_intensity_neg = pd.read_csv(location + study_id + "_PeakHeight_" + chromatography + "_Neg.csv", index_col=False)
            df_urine_mz_neg = pd.read_csv(location + "urine_MZ_" + chromatography + "_Neg.csv", index_col=False)
            df_urine_rt_neg = pd.read_csv(location + "urine_RT_" + chromatography + "_Neg.csv", index_col=False)
            df_urine_intensity_neg = pd.read_csv(location + "urine_PeakHeight_" + chromatography + "_Neg.csv", index_col=False)

        except Exception as error:
            return "Data retrieval error: " + str(error)

        files = {}

        # Add DataFrames to files dictionary
        files["mz_pos"] = df_mz_pos
        files["mz_neg"] = df_mz_neg
        files["rt_pos"] = df_rt_pos
        files["rt_neg"] = df_rt_neg
        files["intensity_pos"] = df_intensity_pos
        files["intensity_neg"] = df_intensity_neg
        files["urine_mz_pos"] = df_urine_mz_pos
        files["urine_mz_neg"] = df_urine_mz_neg
        files["urine_rt_pos"] = df_urine_rt_pos
        files["urine_rt_neg"] = df_urine_rt_neg
        files["urine_intensity_pos"] = df_urine_intensity_pos
        files["urine_intensity_neg"] = df_urine_intensity_neg

        # Manipulate DataFrames for plot readiness
        for key in files.keys():
            files[key] = files[key].fillna(0)

        return files

    except Exception as error:
        return "Data parsing error: " + str(error)


def create_istd_scatter_plot(dataframe, x, y):

    """
    Returns scatter plot figure of retention time vs. sample for internal standards
    """

    istd_rt_plot = px.line(dataframe,
                           title="RT – Internal Standards",
                           x=x,
                           y=y,
                           height=600,
                           markers=True,
                           hover_name=x,
                           labels={"variable": "Internal Standard",
                                   "index": "Sample",
                                   "value": "Retention Time"},
                           log_x=False,
                           color_discrete_map=istd_colors)
    istd_rt_plot.update_layout(transition_duration=500,
                               clickmode="event",
                               legend_title_text="Internal Standards",
                               margin=dict(t=75, b=75))
    istd_rt_plot.update_xaxes(showticklabels=False, title="Sample")
    istd_rt_plot.update_yaxes(title="Retention Time")

    return istd_rt_plot


def create_istd_intensity_plot(dataframe, x, y, text):

    """
    Returns bar plot figure of intensity vs. sample for internal standards
    """

    istd_intensity_plot = px.bar(dataframe,
                                 title="Intensity – " + y,
                                 x=x,
                                 y=y,
                                 # color=text,
                                 text=text,
                                 height=600)
    istd_intensity_plot.update_layout(showlegend=False,
                                      transition_duration=500,
                                      clickmode="event",
                                      xaxis=dict(rangeslider=dict(visible=True), autorange=True),
                                      legend=dict(font=dict(size=10)),
                                      margin=dict(t=75, b=75))
    istd_intensity_plot.update_xaxes(showticklabels=False, title="Sample")
    istd_intensity_plot.update_yaxes(title="Intensity")
    istd_intensity_plot.update_traces(textposition='outside', hovertemplate='Sample: %{x} <br>Intensity: %{y}<br>')

    return istd_intensity_plot


def create_urine_scatter_plot(dataframe, study_name):

    """
    Returns scatter plot figure of retention time vs. feature for urine features
    """

    urine_rt_plot = px.scatter(dataframe,
                               title="RT – Urine Features",
                               x="InChIKey",
                               y=study_name + ": RT (min)",
                               height=500,
                               hover_name="InChIKey",
                               color="InChIKey",
                               size=study_name + ": RT (min)",
                               size_max=30,
                               # hover_data=["Title"],
                               log_x=False,
                               color_discrete_map=istd_colors)
    urine_rt_plot.update_layout(showlegend=False,
                                transition_duration=500,
                                clickmode="event",
                                margin=dict(t=75, b=75))
    urine_rt_plot.update_xaxes(showticklabels=False, title="Feature")
    urine_rt_plot.update_yaxes(title="Retention Time")

    return urine_rt_plot


def create_urine_intensity_plot(dataframe, study_name):

    """
    Returns bar plot figure of intensity vs. feature for urine features
    """

    urine_intensity_plot = px.bar(dataframe,
                                  title="Intensity – Urine Features",
                                  x="InChIKey",
                                  y=study_name + ": Height",
                                  color="InChIKey",
                                  height=500,
                                  # text="Scientific Notation",
                                  hover_data=["InChIKey"])
    urine_intensity_plot.update_layout(showlegend=False,
                                       transition_duration=500,
                                       clickmode="event",
                                       xaxis=dict(rangeslider=dict(visible=True), autorange=True),
                                       legend=dict(font=dict(size=10)),
                                       margin=dict(t=75, b=75))
    urine_intensity_plot.update_xaxes(showticklabels=False, title="Feature")
    urine_intensity_plot.update_yaxes(title="Intensity")
    urine_intensity_plot.update_traces(textposition='outside')

    return urine_intensity_plot


def get_samples(instrument):

    """
    Returns list of samples for a given study run on an instrument
    """

    files = study_loaded[instrument]["study_file"]

    pos_samples = files["rt_pos"].transpose()
    neg_samples = files["rt_neg"].transpose()

    for dataframe in [pos_samples, neg_samples]:
        dataframe.columns = dataframe.iloc[0]
        dataframe.drop(dataframe.index[0], inplace=True)

    pos_samples = pos_samples.index.values.tolist()
    neg_samples = neg_samples.index.values.tolist()
    samples = pos_samples + neg_samples

    df_samples = pd.DataFrame()
    df_samples["Samples"] = samples
    df_samples["Order"] = df_samples["Samples"].str.split("_").str[-1]
    df_samples.sort_values(by="Order", ascending=False, inplace=True)

    sample_list = df_samples["Samples"].tolist()

    return [{"Completed Samples": sample.replace(": RT Info", "")} for sample in sample_list]


@app.callback(Output("QE1-table", "data"),
              Output("QE2-table", "data"),
              Input("header", "children"))
def populate_instrument_tables(placeholder_input):

    """
    Dash callback for populating tables with list of past/active instrument runs
    """

    list_of_QE1_studies = [f.name for f in os.scandir("QE_1") if f.is_dir()]
    list_of_QE2_studies = [f.name for f in os.scandir("QE_2") if f.is_dir()]

    data_for_QE1 = [{"Past / Active Studies": study} for study in list_of_QE1_studies]
    data_for_QE2 = [{"Past / Active Studies": study} for study in list_of_QE2_studies]

    return data_for_QE1, data_for_QE2


@app.callback(Output("QE1-sample-table", "data"),
              Input("QE1-istd-rt-plot", "figure"), prevent_initial_call=True)
def populate_QE1_sample_tables(rt_plot):

    """
    Populates table with list of samples for selected study from QE 1 instrument table
    """

    return get_samples("QE 1")


@app.callback(Output("QE2-sample-table", "data"),
              Input("QE2-istd-rt-plot", "figure"), prevent_initial_call=True)
def populate_sample_tables(rt_plot):

    """
    Populates table with list of samples for selected study from QE 2 instrument table
    """

    return get_samples("QE 2")


@app.callback(Output("QE1-istd-rt-plot", "figure"),
              Output("QE1-urine-rt-plot", "figure"),
              Output("QE1-istd-intensity-plot", "figure"),
              Output("QE1-urine-intensity-plot", "figure"),
              Input("QE1-table", "active_cell"),
              State("QE1-table", "data"),
              Input("QE1-polarity-options", "value"),
              Input("QE1-istd-rt-dropdown", "value"),
              Input("QE1-istd-intensity-dropdown", "value"), prevent_initial_call=True)
def populate_QE1_plots(active_cell, table_data, polarity, scatter_plot_standards, bar_plot_standard):

    """
    Dash callback for loading QE 1 instrument data into scatter and bar plots
    """

    # Get name of clicked study from table
    if active_cell:
        study_name = table_data[active_cell['row']][active_cell['column_id']]

        # Retrieve data for clicked study and store as a dictionary
        if study_loaded["QE 1"]["study_name"] != study_name:

            directory = "QE_1/" + study_name + "/"
            files = get_data(directory, study_name)

            study_loaded["QE 1"]["study_name"] = study_name
            study_loaded["QE 1"]["study_file"] = files

        else:
            files = study_loaded["QE 1"]["study_file"]

        # Get internal standards from QC DataFrames for RT scatter plot
        pos_internal_standards = files["rt_pos"]["Title"].astype(str).tolist()
        neg_internal_standards = files["rt_neg"]["Title"].astype(str).tolist()
        pos_urine_features = files["urine_rt_pos"]["InChIKey"].astype(str).tolist()
        neg_urine_features = files["urine_rt_pos"]["InChIKey"].astype(str).tolist()

        if polarity == "pos":
            internal_standards = pos_internal_standards
            urine_features = pos_urine_features
        elif polarity == "neg":
            internal_standards = neg_internal_standards
            urine_features = neg_urine_features

        # Set initial dropdown values when none are selected
        if not scatter_plot_standards:
            scatter_plot_standards = internal_standards

        if not bar_plot_standard:
            bar_plot_standard = internal_standards[0]

        # Prepare DataFrames for plotting
        df_istd_rt = files["rt_" + polarity]
        df_urine_rt = files["urine_rt_" + polarity]
        df_istd_intensity = files["intensity_" + polarity]
        df_urine_intensity = files["urine_intensity_" + polarity]

        # Transpose internal standard DataFrames
        df_istd_rt = df_istd_rt.transpose()
        df_istd_intensity = df_istd_intensity.transpose()

        for dataframe in [df_istd_rt, df_istd_intensity]:
            dataframe.columns = dataframe.iloc[0]
            dataframe.drop(dataframe.index[0], inplace=True)

        # Split text in internal_standard DataFrames
        for istd in internal_standards:
            rt = df_istd_rt[istd].str.split(": ").str[0]
            rt_diff = df_istd_rt[istd].str.split(": ").str[1]
            df_istd_rt[istd] = rt.astype(float)
            # df_istd_rt[istd + "_diff"] = rt_diff

        samples = df_istd_rt.index.values.tolist()
        samples = [sample.replace(": RT Info", "") for sample in samples]

        try:

            # Internal standards – retention time vs. sample
            istd_rt_plot = create_istd_scatter_plot(dataframe=df_istd_rt,
                                                    x=samples,
                                                    y=scatter_plot_standards)

            # Internal standards – intensity vs. sample
            istd_intensity_plot = create_istd_intensity_plot(dataframe=df_istd_intensity,
                                                             x=df_istd_intensity.index,
                                                             y=bar_plot_standard,
                                                             text=samples)

            # Urine features – retention time vs. feature
            urine_rt_plot = create_urine_scatter_plot(df_urine_rt, study_name)

            # Urine features – intensity vs. feature
            urine_intensity_plot = create_urine_intensity_plot(df_urine_intensity, study_name)

            return istd_rt_plot, urine_rt_plot, istd_intensity_plot, urine_intensity_plot

        except Exception as error:
            print(error)
            return dash.no_update

    else:
        return dash.no_update


@app.callback(Output("QE2-istd-rt-plot", "figure"),
              Output("QE2-urine-rt-plot", "figure"),
              Output("QE2-istd-intensity-plot", "figure"),
              Output("QE2-urine-intensity-plot", "figure"),
              Input("QE2-table", "active_cell"),
              State("QE2-table", "data"),
              Input("QE2-polarity-options", "value"),
              Input("QE2-istd-rt-dropdown", "value"),
              Input("QE2-istd-intensity-dropdown", "value"), prevent_initial_call=True)
def populate_QE2_plots(active_cell, table_data, polarity, scatter_plot_standards, bar_plot_standard):

    """
    Dash callback for loading QE 2 instrument data into scatter and bar plots
    """

    # Get name of clicked study from table
    if active_cell:
        study_name = table_data[active_cell['row']][active_cell['column_id']]

        # Retrieve data for clicked study and store as a dictionary
        if study_loaded["QE 2"]["study_name"] != study_name:

            directory = "QE_2/" + study_name + "/"
            files = get_data(directory, study_name)

            study_loaded["QE 2"]["study_name"] = study_name
            study_loaded["QE 2"]["study_file"] = files

        else:
            files = study_loaded["QE 2"]["study_file"]

        # Get internal standards from QC DataFrames for RT scatter plot
        pos_internal_standards = files["rt_pos"]["Title"].astype(str).tolist()
        neg_internal_standards = files["rt_neg"]["Title"].astype(str).tolist()
        pos_urine_features = files["urine_rt_pos"]["InChIKey"].astype(str).tolist()
        neg_urine_features = files["urine_rt_pos"]["InChIKey"].astype(str).tolist()

        if polarity == "pos":
            internal_standards = pos_internal_standards
            urine_features = pos_urine_features
        elif polarity == "neg":
            internal_standards = neg_internal_standards
            urine_features = neg_urine_features

        # Set initial dropdown values when none are selected
        if not scatter_plot_standards:
            scatter_plot_standards = internal_standards

        if not bar_plot_standard:
            bar_plot_standard = internal_standards[0]

        # Prepare DataFrames for plotting
        df_istd_rt = files["rt_" + polarity]
        df_urine_rt = files["urine_rt_" + polarity]
        df_istd_intensity = files["intensity_" + polarity]
        df_urine_intensity = files["urine_intensity_" + polarity]

        # Transpose internal standard DataFrames
        df_istd_rt = df_istd_rt.transpose()
        df_istd_intensity = df_istd_intensity.transpose()

        for dataframe in [df_istd_rt, df_istd_intensity]:
            dataframe.columns = dataframe.iloc[0]
            dataframe.drop(dataframe.index[0], inplace=True)

        # Split text in internal_standard DataFrames
        for istd in internal_standards:
            rt = df_istd_rt[istd].str.split(": ").str[0]
            rt_diff = df_istd_rt[istd].str.split(": ").str[1]
            df_istd_rt[istd] = rt.astype(float)
            # df_istd_rt[istd + "_diff"] = rt_diff

        samples = df_istd_rt.index.values.tolist()
        samples = [sample.replace(": RT Info", "") for sample in samples]

        try:

            # Internal standards – retention time vs. sample
            istd_rt_plot = create_istd_scatter_plot(dataframe=df_istd_rt,
                                                    x=samples,
                                                    y=scatter_plot_standards)

            # Internal standards – intensity vs. sample
            istd_intensity_plot = create_istd_intensity_plot(dataframe=df_istd_intensity,
                                                             x=df_istd_intensity.index,
                                                             y=bar_plot_standard,
                                                             text=samples)

            # Urine features – retention time vs. feature
            urine_rt_plot = create_urine_scatter_plot(df_urine_rt, study_name)

            # Urine features – intensity vs. feature
            urine_intensity_plot = create_urine_intensity_plot(df_urine_intensity, study_name)

            return istd_rt_plot, urine_rt_plot, istd_intensity_plot, urine_intensity_plot

        except Exception as error:
            print(error)
            return dash.no_update

    else:
        return dash.no_update


@app.callback(Output("QE1-istd-rt-dropdown", "options"),
              Output("QE1-istd-intensity-dropdown", "options"),
              Output("QE2-istd-rt-dropdown", "options"),
              Output("QE2-istd-intensity-dropdown", "options"),
              Input("QE1-polarity-options", "value"),
              Input("QE2-polarity-options", "value"), prevent_initial_call=True)
def update_dropdowns(polarity_QE1, polarity_QE2):

    """
    Updates internal standard dropdown list with correct standards for corresponding polarity
    """

    neg_internal_standards = ["1_Methionine_d8", "1_Creatinine_d3", "1_CUDA", "1_Glutamine_d5", "1_Glutamic Acid_d3",
                              "1_Arginine_d7", "1_Tryptophan d5", "1_Serine d3", "1_Hippuric acid d5"]

    QE1_dropdown = standards_list
    QE2_dropdown = standards_list

    if polarity_QE1 == "neg":
        QE1_dropdown = neg_internal_standards

    if polarity_QE2 == "neg":
        QE2_dropdown = neg_internal_standards

    return QE1_dropdown, QE1_dropdown, QE2_dropdown, QE2_dropdown


# @app.callback(Output("information-card", "children"),
#               Output("feature-bar-plot", "figure"),
#               Input("feature-scatter-plot", "clickData"),
#               Input("metabolite-table", "active_cell"),
#               State("metabolite-table", "data"),
#               Input("plot-container", "style"), prevent_initial_call=True)
# def update_info_card(click_data, active_cell, table_data, data_container):
#
#     """
#     TODO: Dash callback for updating information card on plot click
#     """
#
#     df_with_samples = studies[0][0]
#     df = studies[0][1]
#     df_samples_only = studies[0][2]
#     classes = studies[0][3]
#     sample_ids = studies[0][4]
#
#     # Get clicked feature from feature plot
#     if click_data or active_cell:
#
#         if click_source[0] == "plot":
#             feature = click_data["points"][0]["hovertext"]
#
#         elif click_source[0] == "table":
#             feature = table_data[active_cell['row']][active_cell['column_id']]
#
#         feature_data = df[df["Metabolite name"] == feature]
#
#     else:
#         feature_data = df[0:1]
#         feature = feature_data["Metabolite name"].astype(str).values
#
#     # Get feature information
#     metabolite_name = feature_data["Metabolite name"].astype(str).values
#     msi = feature_data["MSI"].astype(str).values
#     mz = feature_data["Average m/z"].astype(str).values
#     rt = feature_data["Retention time (min)"].astype(str).values
#     inchikey = feature_data["INCHIKEY"].astype(str).values
#     polarity = feature_data["Polarity"].astype(str).values
#     sample_average = feature_data["Average intensity"].astype(str).values
#
#     if polarity == "pos":
#         polarity = "Positive ion mode"
#     elif polarity == "neg":
#         polarity = "Negative ion mode"
#     elif polarity == "both":
#         polarity = "Found in both modes"
#
#     # Header
#     header = html.H3(children=feature)
#
#     # Get 2D structure from PubChem
#     pubchem_structure = "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/inchikey/" + inchikey[0] + "/PNG"
#     structure = html.Img(src=pubchem_structure)
#
#     # Get PubChem query link
#     pubchem_link = "https://pubchem.ncbi.nlm.nih.gov/#query=" + inchikey
#
#     # Create table rows with information
#     table = html.Table(id="info-table", className="info-table", children=[
#
#         html.Tr(children=[
#             html.Th("Metabolite"),
#             html.Td(feature)]),
#
#         html.Tr(children=[
#             html.Th("INCHIKEY"),
#             html.Td(html.A(href=pubchem_link[0], children=inchikey, target="_blank"))]),
#
#         html.Tr(children=[
#             html.Th("MSI"),
#             html.Td(msi)]),
#
#         html.Tr(children=[
#             html.Th("Average m/z"),
#             html.Td(mz)]),
#
#         html.Tr(children=[
#             html.Th("Average RT"),
#             html.Td(rt)]),
#
#         html.Tr(children=[
#             html.Th("Polarity"),
#             html.Td(polarity)]),
#
#         html.Tr(children=[
#             html.Th("Average intensity"),
#             html.Td(sample_average)]),
#     ])
#
#     container = html.Div(children=[header, structure, table])
#
#     # Get DataFrame with samples and intensities
#     df_samples_only_copy = df_samples_only.copy()
#     df_samples_only_copy.drop("Metabolite name", inplace=True, axis=1)
#
#     intensities = df_samples_only_copy.loc[
#         df_samples_only["Metabolite name"] == metabolite_name[0]].squeeze().tolist()
#
#     # Create a DataFrame of intensity versus samples for clicked feature
#     df_intensity_vs_sample = pd.DataFrame()
#     df_intensity_vs_sample["Class"] = classes
#     df_intensity_vs_sample["Sample"] = sample_ids
#     df_intensity_vs_sample["Intensity"] = intensities
#     df_intensity_vs_sample["Scientific Notation"] = ['{:.2e}'.format(x) for x in intensities]
#
#     # Update bar plot
#     bar_plot = px.bar(df_intensity_vs_sample, title=metabolite_name[0], x="Sample", y="Intensity", color="Class",
#                       text="Scientific Notation", hover_data=["Class", "Scientific Notation"])
#     bar_plot.update_layout(transition_duration=500, clickmode="event",
#                            xaxis=dict(rangeslider=dict(visible=True), autorange=True))
#     bar_plot.update_xaxes(showticklabels=False)
#     bar_plot.update_traces(textposition='outside')
#
#     return container, bar_plot


if __name__ == "__main__":

    # if sys.platform == "win32":
    #     chrome_path = "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe"
    #     webbrowser.register('chrome', None, webbrowser.BackgroundBrowser(chrome_path))
    #     webbrowser.get("chrome").open("http://127.0.0.1:8050/")
    # elif sys.platform == "darwin":
    #     webbrowser.get("chrome").open("http://127.0.0.1:8050/", new=1)

    app.run_server(debug=True)