import os, sys, webbrowser
import pandas as pd
import plotly.express as px
from dash import dash, dcc, html, dash_table, Input, Output, State
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

study_loaded = {
    "QE 1": {
        "study_name": "",
        "study_file": "",
        "drive_id": "1-0y1jUARBM1DwExjrhyl0WF3KRLFWHom",
        "ui_callback": False,
        "clicked_feature": False,
},
    "QE 2": {
        "study_name": "",
        "study_file": "",
        "drive_id": "1-9unZeOHyTPYZScox5Wv9X0CxTWIE-Ih",
        "ui_callback": False,
        "clicked_feature": False,
    },
}

standards_list = ["1_Methionine_d8", "1_1_Methylnicotinamide_d3", "1_Creatinine_d3", "1_Carnitine_d3",
             "1_Acetylcarnitine_d3", "1_TMAO_d9", "1_Choline_d9", "1_Glutamine_d5", "1_CUDA", "1_Glutamic Acid_d3",
             "1_Arginine_d7", "1_Alanine_d3", "1_Valine d8", "1_Tryptophan d5", "1_Serine d3", "1_Lysine d8",
             "1_Phenylalanine d8", "1_Hippuric acid d5"]

standards_dict = {
    "1_Methionine_d8": "Methionine d8",
    "1_1_Methylnicotinamide_d3": "1-Methylnicotinamide d3",
    "1_Creatinine_d3": "Creatinine d3",
    "1_Carnitine_d3": "Carnitine d3",
    "1_Acetylcarnitine_d3": "Acetylcarnitine d3",
    "1_TMAO_d9": "TMAO d9",
    "1_Choline_d9": "Choline d9",
    "1_Glutamine_d5": "Glutamine d5",
    "1_CUDA": "CUDA",
    "1_Glutamic Acid_d3": "Glutamic acid d3",
    "1_Arginine_d7": "Arginine d7",
    "1_Alanine_d3": "Alanine d3",
    "1_Valine d8": "Valine d8",
    "1_Tryptophan d5": "Tryptophan d5",
    "1_Serine d3": "Serine d3",
    "1_Lysine d8": "Lysine d8",
    "1_Phenylalanine d8": "Phenylalanine d8",
    "1_Hippuric acid d5": "Hippuric acid d5"
}

retention_times_dict = {
    "1_Methionine_d8": 7.479,
    "1_1_Methylnicotinamide_d3": 6.217,
    "1_Creatinine_d3": 4.908,
    "1_Carnitine_d3": 7.8,
    "1_Acetylcarnitine_d3": 7.169,
    "1_TMAO_d9": 5.495,
    "1_Choline_d9": 5.123,
    "1_CUDA": 1.104,
    "1_Glutamine_d5": 8.642,
    "1_Glutamic Acid_d3": 8.805,
    "1_Arginine_d7": 9.497,
    "1_Alanine_d3": 8.14,
    "1_Valine d8": 7.809,
    "1_Tryptophan d5": 6.897,
    "1_Serine d3": 8.704,
    "1_Lysine d8": 9.578,
    "1_Phenylalanine d8": 6.92,
    "1_Hippuric acid d5": 3.011
}

pos_urine_features_dict = {
    "L-Serine": "MTCFGRXMJLQNBG-UHFFFAOYSA-N",
    "L-Cystine": "LEVWYRKDKASIDU-IMJSIDKUSA-N",
    "DL-Isoleucine": "AGPKZVBTJJNPAG-UHFFFAOYSA-N",
    "Riboflavin": "AUNGANRZJHBGPY-SCRDCRAPSA-N",
    "3-Hydroxypropionic acid": "AYFVYJQAPQTCCC-UHFFFAOYSA-N",
    "o-Methoxyphenyl sulfate": "BPMFZUMJYQTVII-UHFFFAOYSA-N",
    "DL-Threonine": "COLNVLDHVKWLRT-UHFFFAOYSA-N",
    "4-Nitrophenol": "CQOVPNPJLQNMDC-ZETCQYMHSA-N",
    "Creatine": "CVSVTCORWBXHQV-UHFFFAOYSA-N",
    "7-Trimethyluric acid": "CWLQUGTUXBXTLF-UHFFFAOYSA-N",
    "Afalanine": "CYZKJBZEIFWZSR-LURJTMIESA-N",
    "Sucrose": "CZMRCDWAGMRECN-UGDNZRGBSA-N",
    "2-Hydroxyphenylacetic acid": "DCXYFEDJOCDNAF-REOHCLBHSA-N",
    "3-Acetylphenol sulfate": "DDRJAANPRJIHGJ-UHFFFAOYSA-N",
    "Nicotinamide": "DFPAKSUCGFBDDF-UHFFFAOYSA-N",
    "Dehydroepiandrosterone sulfate": "DTERQYGMUDWYAZ-ZETCQYMHSA-N",
    "Tyramine": "DZGWFCGJZKJUFP-UHFFFAOYSA-N",
    "N-Acetyl-D-tryptophan": "DZTHIGRZJZPRDV-GFCCVEGCSA-N",
    "Isobutyrylglycine": "FDGQSTZJBFJUBT-UHFFFAOYSA-N",
    "Asparagine": "FEMXZDUTFRTWPE-UHFFFAOYSA-N",
    "N8-Acetylspermidine": "FONIWJIDLJEJTL-UHFFFAOYSA-N",
    "Adenine": "GFFGJBXGBJISGV-UHFFFAOYSA-N",
    "1-Methyladenosine": "GFYLSDSUCHVORB-IOSLPCCCSA-N",
    "DL-Pantothenic acid": "GHOKWGTUZJEAQD-UHFFFAOYSA-N",
    "Maltose": "GUBGYTABKSRVRQ-QUYVBRFLSA-N",
    "Kynurenic acid": "HCZHHEIFKROPDY-UHFFFAOYSA-N",
    "3-Cyclohexanedione": "HJSLFCCWAKVHIW-UHFFFAOYSA-N",
    "Histidine": "HNDVDQJCIGZPNO-YFKPBYRVSA-N",
    "Citraconic acid": "HNXQXTQTPAJEJL-UHFFFAOYSA-N",
    "Ethanolamine": "HZAXFHJVJLSVMW-UHFFFAOYSA-N",
    "Dimethyl sulfoxide": "IAZDPXIOMUYVGZ-UHFFFAOYSA-N",
    "3-dihydro-1H-indol-3-yl)acetic acid": "JDHILDINMRGULE-UHFFFAOYSA-N",
    "Phenylacetylglutamine": "JFLIEFSWGNOPJJ-JTQLQIEISA-N",
    "3-Ureidopropionic acid": "JSJWCHRYRHKBBW-UHFFFAOYSA-N",
    "Thiamine": "JZRWCGZRTZMZEH-UHFFFAOYSA-N",
    "N-Acetylhistidine": "KBOJOGQFRVVWBH-ZETCQYMHSA-N",
    "Lysine": "KDXKERNSBIXSRK-YFKPBYRVSA-N",
    "Succinic acid": "KSPIYJQBLVDRRI-WDSKDSINSA-N",
    "N-(2-Furoyl)glycine": "KSPQDMRTZZYQLM-UHFFFAOYSA-N",
    "Betaine": "KWIUHFFTVRNATP-UHFFFAOYSA-N",
    "1-Methylnicotinamide": "LDHMAVIPBRSVRG-UHFFFAOYSA-O",
    "Urocanic acid": "LOIYMIARKYCTBW-OWOJBTEDSA-N",
    "5-Hydroxyindole": "LMIQERWZRIFWNZ-UHFFFAOYSA-N",
    "3'-Adenylic acid": "LNQVTSROQXJCDD-KQYNXXCUSA-N",
    "S-adenosylmethionine": "MEFKEPWMEQBLKI-AIRLBKTGSA-N",
    "1,7-Dimethyluric acid": "NOFNCLGCUJJPKU-UHFFFAOYSA-N",
    "2-Amino-6-(trimethylazaniumyl)hexanoate": "MXNRLFUSFKVQSK-UHFFFAOYSA-N",
    "Anserine": "MYYIAHXIVFADCU-QMMMGPOBSA-N",
    "N4-Acetylcytidine": "NIDVTARKFBZMOT-PEBGCTIMSA-N",
    "Histamine": "NTYJJOPFIAHURM-UHFFFAOYSA-N",
    "Dimethylarginine": "NWGZOALPWZDXNG-UHFFFAOYSA-N",
    "N-Formylmethionine": "ODKSFYDXXFIFQN-BYPYZUCNSA-N",
    "Choline": "OEYIOHPDSNJKLS-UHFFFAOYSA-N",
    "Adenosine": "OIRDTQYFTABQOQ-KQYNXXCUSA-N",
    "Dihydrouracil": "OIVLITBTBDPEFK-UHFFFAOYSA-N",
    "3-(3-Hydroxyphenyl)propanoic acid": "ONPXCLZMBSJLSP-CSMHCCOUSA-N",
    "N-Acetyl-1-aspartylglutamic acid": "OPVPGKGADVGKTG-UHFFFAOYSA-N",
    "Spermine": "PFNFFQXMRSDOHW-UHFFFAOYSA-N",
    "7-Methylxanthine": "PFWLFWPASULGAN-UHFFFAOYSA-N",
    "3-Methylcrotonylglycine": "PFWQSHXPNKRLIV-UHFFFAOYSA-N",
    "DL-Carnitine": "PHIQHXFUZVPYII-UHFFFAOYSA-N",
    "4-Hydroxyquinoline": "PMZDQRJGMBOQBF-UHFFFAOYSA-N",
    "Suberic acid": "PQNASZJZHFPQLE-UHFFFAOYSA-N",
    "Imidazoleacetic acid": "PRJKNHOMHKJCEJ-UHFFFAOYSA-N",
    "Acetylcysteine": "PWKSKIMOESPYIA-BYPYZUCNSA-N",
    "1-Methyluric acid": "QFDRTQONISXGJA-UHFFFAOYSA-N",
    "Hippuric acid": "QIAFMBKCNZACKA-UHFFFAOYSA-N",
    "DL-Tryptophan": "QIVBCDIJIAJPQS-UHFFFAOYSA-N",
    "Acetylcarnitine": "RDHQFKQIGNGIED-UHFFFAOYSA-N",
    "3-Methylcytidine": "RDPUKVRQKWBSPK-ZOQUXTDFSA-N",
    "DL-Leucine": "ROHFNLRQFUQHCH-UHFFFAOYSA-N",
    "N2,N2-Dimethylguanosine": "RSPURTUNRHNVGF-IOSLPCCCSA-N",
    "gamma-Glutamylcysteinylglycine": "RWSXRVCMGQZWBV-UHFFFAOYSA-N",
    "Caffeine": "RYYVLZVUVIJVGH-UHFFFAOYSA-N",
    "3'-O-Methylcytidine": "RZJCFLSPBDUNDH-ZOQUXTDFSA-N",
    "2-Methylguanosine": "SLEHROROQDYRAW-KQYNXXCUSA-N",
    "gamma-Glu-Ile": "SNCKGJWJABDZHI-ZKWXMUAHSA-N",
    "Choline Alfoscerate": "SUHOQUVVVLNYQR-MRVPVSSYSA-N",
    "Cyclo(-Leu-Pro)": "SZJNCZMRZAUNQT-IUCAKERBSA-N",
    "4-Guanidinobutyric acid": "TUHVEAJXIMEOSA-UHFFFAOYSA-N",
    "Levocarnitine propionate": "UFAHZIUFPNSHSL-MRVPVSSYSA-N",
    "1-Methylguanosine": "UTAIYTHAJQNQDW-KQYNXXCUSA-N",
    "Trimethylamine oxide": "UYPYRKYUKCHHIB-UHFFFAOYSA-N",
    "Guanine": "UYTPUPDQBNUYGX-UHFFFAOYSA-N",
    "N-alpha-Acetyl-L-lysine": "VEYYWZRYIYDQJM-ZETCQYMHSA-N",
    "N6-Methyladenosine": "VQAYFKKCNSOZKM-IOSLPCCCSA-N",
    "N-amidinoaspartic acid": "VVHOUVWJCQOYGG-UHFFFAOYSA-N",
    "DL-Glutamic acid": "WHUUTDBJXJRKMK-UHFFFAOYSA-N",
    "5'-Deoxy-5'-methylthioadenosine": "WUUGFSXJNOTRMR-IOSLPCCCSA-N",
    "Trigonelline": "WWNNZCOKKKDOPX-UHFFFAOYSA-N",
    "Methylurea": "XGEGHDBEHXKFPX-UHFFFAOYSA-N",
    "2-Amino-6-ureidohexanoic acid": "XIGSAGMEBXLVJJ-UHFFFAOYSA-N",
    "N-Methylleucine": "XJODGRWDFZVTKW-LURJTMIESA-N",
    "N-Acetylhistamine": "XJWPISBUKWZALE-UHFFFAOYSA-N",
    "Taurine": "XOAAWQZATWQOTB-UHFFFAOYSA-N",
    "Urea": "XSQUKJJJFZCRTK-UHFFFAOYSA-N",
    "N(6),N(6)-Dimethyl-L-lysine": "XXEWFEBMSGLYBY-ZETCQYMHSA-N",
    "DL-Glutamine": "ZDXPYRJPNDTMRX-UHFFFAOYSA-N",
    "Theophylline": "ZFXYFBGIUFBOJW-UHFFFAOYSA-N",
    "S-adenosyl-L-homocysteine": "ZJUKTBDSGOFHSH-WFMPWKQPSA-N"
 }

neg_urine_features_dict = {
    "D-Glucuronic Acid": "AEMOLEFTQBMNLQ-AQKNRBDQSA-N",
    "2,6-Dihydroxybenzoic acid": "AKEUNCKRJATALU-UHFFFAOYSA-N",
    "3-Hydroxypropionic acid": "ALRHLSYJTWAHJZ-UHFFFAOYSA-N",
    "o-Methoxyphenyl sulfate": "AQTYXAPIHMXAAV-UHFFFAOYSA-N",
    "DL-Threonine": "AYFVYJQAPQTCCC-UHFFFAOYSA-N",
    "4-Nitrophenol": "BTJIUGUIPKRLHP-UHFFFAOYSA-N",
    "Indoxyl sulfate": "BXFFHSIDQOFMLE-UHFFFAOYSA-N",
    "1,3,7-Trimethyluric acid": "BYXCFUMGEBZDDI-UHFFFAOYSA-N",
    "Afalanine": "CBQJSKKFNMDLON-UHFFFAOYSA-N",
    "2-Hydroxyphenylacetic acid": "CCVYRRGZDBSHFU-UHFFFAOYSA-N",
    "3-Acetylphenol sulfate": "CGFRVKXGZRODPA-UHFFFAOYSA-N",
    "L-Phenylalanine": "COLNVLDHVKWLRT-UHFFFAOYSA-N",
    "Sucrose": "CZMRCDWAGMRECN-UGDNZRGBSA-N",
    "Dehydroepiandrosterone sulfate": "CZWCKYRVOZZJNM-USOAJAOKSA-N",
    "Isobutyrylglycine": "DCICDMMXFIELDF-UHFFFAOYSA-N",
    "Asparagine": "DCXYFEDJOCDNAF-REOHCLBHSA-N",
    "Creatinine": "DDRJAANPRJIHGJ-UHFFFAOYSA-N",
    "Uridine": "DRTQHJPVMGBUCF-XVFCMESISA-N",
    "N6-Acetyl-L-lysine": "DTERQYGMUDWYAZ-ZETCQYMHSA-N",
    "5-Hydroxyindole-3-acetic acid": "DUUGKQCEGZLZNO-UHFFFAOYSA-N",
    "Hypoxanthine": "FDGQSTZJBFJUBT-UHFFFAOYSA-N",
    "4-Hydroxybenzoic acid": "FJKROLUGYXJWQN-UHFFFAOYSA-N",
    "DL-Pantothenic acid": "GHOKWGTUZJEAQD-UHFFFAOYSA-N",
    "Kynurenic acid": "HCZHHEIFKROPDY-UHFFFAOYSA-N",
    "D-Arabinitol": "HEBKCHPVOIAQTA-QWWZWVQMSA-N",
    "Histidine": "HNDVDQJCIGZPNO-YFKPBYRVSA-N",
    "Citraconic acid": "HNEGQIOMVPPMNR-IHWYPQMZSA-N",
    "Homogentisic acid": "IGMNYECMUMZDDF-UHFFFAOYSA-N",
    "(2-oxo-2,3-dihydro-1H-indol-3-yl)acetic acid": "ILGMGHZPXRDCCS-UHFFFAOYSA-N",
    "3-methyl-DL-histidine": "JDHILDINMRGULE-UHFFFAOYSA-N",
    "Glutaric acid": "JFCQEDHGNNZCLN-UHFFFAOYSA-N",
    "Phenylacetylglutamine": "JFLIEFSWGNOPJJ-JTQLQIEISA-N",
    "2-Hydroxy-3-(4-hydroxyphenyl)propanoic acid": "JVGVDSSUAVXRDY-UHFFFAOYSA-N",
    "Lactic acid": "JVTAAEKCZFNVCJ-UHFFFAOYSA-N",
    "N-Acetylhistidine": "KBOJOGQFRVVWBH-ZETCQYMHSA-N",
    "Succinic acid": "KDYFGRWQOYBRFD-UHFFFAOYSA-N",
    "N-(2-Furoyl)glycine": "KSPQDMRTZZYQLM-UHFFFAOYSA-N",
    "N-Acetyl-DL-alanine": "KTHDTJVBEPMMGL-UHFFFAOYSA-N",
    "Urocanic acid": "LOIYMIARKYCTBW-OWOJBTEDSA-N",
    "2-Ethyl-2-hydroxybutyric acid": "LXVSANCQXSSLPA-UHFFFAOYSA-N",
    "DL-Serine": "MTCFGRXMJLQNBG-UHFFFAOYSA-N",
    "Anserine": "MYYIAHXIVFADCU-QMMMGPOBSA-N",
    "Phosphoric acid": "NBIIXXVUZAFLBC-UHFFFAOYSA-N",
    "N4-Acetylcytidine": "NIDVTARKFBZMOT-PEBGCTIMSA-N",
    "1,7-Dimethyluric acid": "NOFNCLGCUJJPKU-UHFFFAOYSA-N",
    "5-(Diaminomethylideneamino)-2-(dimethylamino)pentanoic acid": "NWGZOALPWZDXNG-UHFFFAOYSA-N",
    "2-Hydroxycinnamic acid": "ONPXCLZMBSJLSP-CSMHCCOUSA-N",
    "Allantoin": "PMOWTIHVNWZYFI-AATRIKPKSA-N",
    "Pseudouridine": "POJWUDADGALRAB-UHFFFAOYSA-N",
    "Orotic acid": "PTJWIQPHWPFNBW-GBNDHIKLSA-N",
    "N-Formylmethionine": "PXQPEWDEAKTCGB-UHFFFAOYSA-N",
    "1-Methyluric acid": "PYUSHNKNPOHWEZ-YFKPBYRVSA-N",
    "DL-Tryptophan": "QFDRTQONISXGJA-UHFFFAOYSA-N",
    "3-(3-Hydroxyphenyl)propanoic acid": "QIVBCDIJIAJPQS-UHFFFAOYSA-N",
    "4-Amino-1-[4-hydroxy-5-(hydroxymethyl)-3-methoxyoxolan-2-yl]pyrimidin-2-one": "QVWAEZJXDYOKEH-UHFFFAOYSA-N",
    "2?-O-Methylcytidine": "RFCQJGFZUQFYRF-UHFFFAOYSA-N",
    "N2,N2-Dimethylguanosine": "RSPURTUNRHNVGF-IOSLPCCCSA-N",
    "gamma-Glutamylcysteinylglycine": "RWSXRVCMGQZWBV-UHFFFAOYSA-N",
    "N-Acetyl-Neuraminic Acid": "SQVRNKJHWKZAKO-LUWBGTNYSA-N",
    "2'-O-Methyluridine": "SXUXMRMBWZCMEN-ZOQUXTDFSA-N",
    "Suberic acid": "TYFQFVWCELRYAO-UHFFFAOYSA-N",
    "1-Methylguanosine": "UTAIYTHAJQNQDW-KQYNXXCUSA-N",
    "4-Acetamidobutyric acid": "UZTFMUBKZQVKLK-UHFFFAOYSA-N",
    "2-Keto-L-gulonic acid": "VBUYCZFBVCCYFD-NUNKFHFFSA-N",
    "N-amidinoaspartic acid": "VVHOUVWJCQOYGG-UHFFFAOYSA-N",
    "Maleic acid": "VZCYOOQTPOCHFL-UPHRSURJSA-N",
    "p-Cresol sulfate": "WGNAKZGUSRVWRH-UHFFFAOYSA-N",
    "DL-Glutamic acid": "WHUUTDBJXJRKMK-UHFFFAOYSA-N",
    "Pimelic acid": "WLJVNTCWHIRURA-UHFFFAOYSA-N",
    "Adipic acid": "WNLRTRBMVRJNCN-UHFFFAOYSA-N",
    "Tiglylglycine": "WRUSVQOKJIDBLP-HWKANZROSA-N",
    "Acetylleucine": "WXNXCEHXYPACJF-UHFFFAOYSA-N",
    "2,5-Dihydroxybenzoic acid": "WXTMDXOMEHJXQO-UHFFFAOYSA-N",
    "Indole-3-lactic acid": "XGILAAMKEQUXLS-UHFFFAOYSA-N",
    "N-Methyl-L-glutamic acid": "XLBVNMSMFQMKEY-BYPYZUCNSA-N",
    "Taurine": "XOAAWQZATWQOTB-UHFFFAOYSA-N",
    "N-Acetyl-DL-methionine": "XUYPXLNMDZIRQH-UHFFFAOYSA-N",
    "Salicylic acid": "YGSDEFSMJLZEOE-UHFFFAOYSA-N",
    "DL-Glutamine": "ZDXPYRJPNDTMRX-UHFFFAOYSA-N",
    "Theophylline": "ZFXYFBGIUFBOJW-UHFFFAOYSA-N",
    "4-Hydroxyhippuric acid": "ZMHLUFWWWPBTIU-UHFFFAOYSA-N"
}

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

# Authenticate with Google Drive
gauth = GoogleAuth()
drive = GoogleDrive(gauth)

# Get directory for files
current_directory =  os.getcwd()

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
        html.H1(id="header-text", children="MS-AutoQC")
    ]),

    # App layout
    html.Div(className="page", children=[

        dcc.Tabs(id="tabs", children=[

            # QC dashboard for QE 1
            dcc.Tab(label="QE 1", children=[

                html.Div(id="QE1-table-container", className="table-container", style={"display": "none"}, children=[

                    # Table of past/active studies that were run on QE 1
                    dash_table.DataTable(id="QE1-table", page_action="none",
                        fixed_rows={'headers': True},
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
                                'if': {
                                    'state': 'active'
                                },
                               'backgroundColor': 'rgba(0, 116, 217, 0.3)',
                               'border': '1px solid rgb(0, 116, 217)'
                            }]
                        ),

                    # Polarity filtering options
                    dcc.RadioItems(
                        id="QE1-polarity-options",
                        options=[
                            {"label": "Positive Mode", "value": "pos"},
                            {"label": "Negative Mode", "value": "neg"}],
                        value="pos",
                        style={"margin-top": "30px",
                               "text-align": "center",
                               "width": "100%"},
                        inputStyle={"margin-left": "15px",
                                    "margin-right": "5px"}),

                    # Table of samples run for a particular study
                    dash_table.DataTable(id="QE1-sample-table", page_action="none",
                        fixed_rows={'headers': True},
                        # cell_selectable=True,
                        style_cell={"textAlign": "left",
                                    "fontSize": "15px",
                                    "fontFamily": "sans-serif",
                                    "lineHeight": "25px",
                                    "padding": "10px",
                                    "borderRadius": "5px"},
                        style_data={"whiteSpace": "normal",
                                    "textOverflow": "ellipsis",
                                    "maxWidth": 0},
                        style_table={"overflowY": "auto"},
                        style_data_conditional=[
                            {'if': {'filter_query': '{QC} = "Fail"'},
                                'backgroundColor': '#F5B7B1',
                                'font-weight': 'bold'
                            }
                        ],
                        style_cell_conditional=[
                            {'if': {'column_id': 'Sample'},
                                'width': '80%'},
                            {'if': {'column_id': 'QC'},
                                'width': '20%'},
                         ]
                    )
                ]),

                # Data container for QE 1 plots
                html.Div(id="QE1-plot-container", className="all-plots-container", children=[

                    html.Div(className="istd-plot-div", children=[

                        html.Div(className="plot-container", children=[

                            # Dropdown for internal standard RT plot
                            dcc.Dropdown(
                                id="QE1-istd-rt-dropdown",
                                options=standards_list,
                                placeholder="Select internal standards...",
                                style={"text-align": "left",
                                       "height": "35px",
                                       "width": "100%",
                                       "display": "inline-block"}
                            ),

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

                            # Bar plot of internal standard intensity in QE 1 samples
                            dcc.Graph(id="QE1-istd-intensity-plot")
                        ])

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
                                options=list(pos_urine_features_dict.keys()),
                                placeholder="Select urine feature...",
                                style={"text-align": "left",
                                       "height": "35px",
                                       "width": "100%",
                                       "display": "inline-block"}
                            ),

                            dcc.Graph(id="QE1-urine-intensity-plot")
                        ])

                    ])

                ]),

            ]),

            # QC dashboard for QE 2
            dcc.Tab(label="QE 2", children=[

                html.Div(id="QE2-table-container", className="table-container", style={"display": "none"}, children=[

                    # Table of past/active studies that were run on QE 2
                    dash_table.DataTable(id="QE2-table", page_action="none",
                         fixed_rows={'headers': True},
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
                                'if': {
                                    'state': 'active'
                                },
                               'backgroundColor': 'rgba(0, 116, 217, 0.3)',
                               'border': '1px solid rgb(0, 116, 217)'
                            }]
                     ),

                    # Polarity filtering options
                    dcc.RadioItems(
                        id="QE2-polarity-options",
                        options=[
                            {"label": "Positive Mode", "value": "pos"},
                            {"label": "Negative Mode", "value": "neg"}],
                        value="pos",
                        style={"margin-top": "30px",
                               "text-align": "center",
                               "width": "100%"},
                        inputStyle={"margin-left": "15px",
                                    "margin-right": "5px"}),

                    # Table of samples run for a particular study
                    dash_table.DataTable(id="QE2-sample-table", page_action="none",
                        fixed_rows={'headers': True},
                        # cell_selectable=True,
                        style_cell={"textAlign": "left",
                                    "fontSize": "15px",
                                    "fontFamily": "sans-serif",
                                    "lineHeight": "25px",
                                    "padding": "10px",
                                    "borderRadius": "5px"},
                        style_data={"whiteSpace": "normal",
                                    "textOverflow": "ellipsis",
                                    "maxWidth": 0},
                        style_table={"overflowY": "auto"},
                        style_data_conditional=[
                            {'if': {'filter_query': '{QC} = "Fail"'},
                                'backgroundColor': '#F5B7B1',
                                'font-weight': 'bold'
                            }
                        ],
                        style_cell_conditional=[
                            {'if': {'column_id': 'Sample'},
                                'width': '80%'},
                            {'if': {'column_id': 'QC'},
                                'width': '20%'},
                        ]
                    )
                ]),

                # Data container for QE 2 plots
                html.Div(id="QE2-plot-container", className="all-plots-container", children=[

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

                            # Bar plot of internal standard intensity in QE 2 samples
                            dcc.Graph(id="QE2-istd-intensity-plot")
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
                                options=list(pos_urine_features_dict.keys()),
                                placeholder="Select urine feature...",
                                style={"text-align": "left",
                                       "height": "35px",
                                       "width": "100%",
                                       "display": "inline-block"}
                            ),

                            dcc.Graph(id="QE2-urine-intensity-plot")

                        ])

                    ]),

                ]),

            ]),

            dcc.Tab(label="Fusion Lumos 1", children=[
                # Construction TBD
                html.Div(id="placeholder-one", children=""),
                html.Div(id="placeholder-two", children=""),
            ]),

            dcc.Tab(label="Fusion Lumos 2", children=[
                # Construction TBD
                html.H2("Construction in progress")
            ]),

            dcc.Tab(label="timsTOF", children=[
                # Construction TBD
                html.H2("Construction in progress")
            ]),

        ])

    ])

])


def get_data(instrument, study_id):

    """
    Loads internal standard and urine m/z, RT, and peak height from instrument run into pandas DataFrames
    """

    chromatography = ""

    # Save .csv files in folder
    files_directory = current_directory + "/qc_files"
    if not os.path.exists(files_directory):
        os.makedirs(files_directory)
    os.chdir(files_directory)

    # Auto-iterate through all QC files for the particular study in Google Drive
    file_list = drive.ListFile({'q': "'" + study_loaded[instrument]["drive_id"] + "' in parents and trashed=false"}).GetList()

    for file in file_list:

        # Download files for study
        if study_id in file["title"] or "urine" in file["title"]:
            file.GetContentFile(file["title"])

        # Set chromatography (if it hasn't already been set)
        if chromatography == "":
            if "HILIC" in file["title"]:
                chromatography = "HILIC"
            elif "C18" in file["title"]:
                chromatography = "C18"
            elif "Lipidomics" in file["title"]:
                chromatography = "Lipidomics"

    # Overarching try/catch
    try:

        # Parse data into pandas DataFrames
        try:
            # Retrieve m/z, RT, and peak height .csv files from bufferbox2
            df_mz_pos = pd.read_csv(study_id + "_MZ_" + chromatography + "_Pos.csv", index_col=False)
            df_rt_pos = pd.read_csv(study_id + "_RT_" + chromatography + "_Pos.csv", index_col=False)
            df_intensity_pos = pd.read_csv(study_id + "_PeakHeight_" + chromatography + "_Pos.csv", index_col=False)
            df_urine_mz_pos = pd.read_csv("urine_MZ_" + chromatography + "_Pos.csv", index_col=False)
            df_urine_rt_pos = pd.read_csv("urine_RT_" + chromatography + "_Pos.csv", index_col=False)
            df_urine_intensity_pos = pd.read_csv("urine_PeakHeight_" + chromatography + "_Pos.csv", index_col=False)

            df_mz_neg = pd.read_csv(study_id + "_MZ_" + chromatography + "_Neg.csv", index_col=False)
            df_rt_neg = pd.read_csv(study_id + "_RT_" + chromatography + "_Neg.csv", index_col=False)
            df_intensity_neg = pd.read_csv(study_id + "_PeakHeight_" + chromatography + "_Neg.csv", index_col=False)
            df_urine_mz_neg = pd.read_csv("urine_MZ_" + chromatography + "_Neg.csv", index_col=False)
            df_urine_rt_neg = pd.read_csv("urine_RT_" + chromatography + "_Neg.csv", index_col=False)
            df_urine_intensity_neg = pd.read_csv("urine_PeakHeight_" + chromatography + "_Neg.csv", index_col=False)

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


def istd_scatter_plot(dataframe, x, y):

    """
    Returns scatter plot figure of retention time vs. sample for internal standards
    """

    y_min = retention_times_dict[y] - 0.1
    y_max = retention_times_dict[y] + 0.1
    # median_rt = dataframe[y].median()

    istd_rt_plot = px.line(dataframe,
                           title="Internal Standards RT – " + standards_dict[y],
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
                               showlegend=False,
                               legend_title_text="Internal Standards",
                               margin=dict(t=75, b=75))
    istd_rt_plot.update_xaxes(showticklabels=False, title="Sample")
    istd_rt_plot.update_yaxes(title="Retention time (min)", range=[y_min, y_max])
    istd_rt_plot.add_hline(y=retention_times_dict[y], line_width=2, line_dash='dash')

    return istd_rt_plot


def istd_bar_plot(dataframe, x, y, text):

    """
    Returns bar plot figure of intensity vs. sample for internal standards
    """

    istd_intensity_plot = px.bar(dataframe,
                                 title="Internal Standards Intensity – " + standards_dict[y],
                                 x=x,
                                 y=y,
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
    istd_intensity_plot.update_traces(textposition='outside',
                                      hovertemplate='Sample: %{x} <br>Intensity: %{y:.2e}<br>')

    return istd_intensity_plot


def urine_scatter_plot(study_name, df_rt, df_mz, df_intensity, urine_features_dict):

    """
    Returns scatter plot figure of m/z vs. retention time for urine features
    """

    urine_df = pd.DataFrame()

    inverted_dict = {value: key for key, value in urine_features_dict.items()}
    inchikey_list = df_mz["InChIKey"].tolist()
    metabolite_list = [inverted_dict[inchikey] for inchikey in inchikey_list]

    urine_df["Metabolite name"] = metabolite_list
    urine_df["INCHIKEY"] = df_mz["InChIKey"]
    urine_df["Precursor m/z"] = df_mz[study_name + ":Precursor m/z"]
    urine_df["Retention time (min)"] = df_rt[study_name + ":RT (min)"]
    urine_df["Intensity"] = df_intensity[study_name + ":Height"]

    plasma = px.colors.sequential.Plasma
    colorscale = [
        [0, plasma[0]],
        [1. / 1000000, plasma[2]],
        [1. / 10000, plasma[4]],
        [1. / 100, plasma[7]],
        [1., plasma[9]],
    ]

    urine_rt_plot = px.scatter(urine_df,
                               title="QC Urine Metabolites",
                               x="Retention time (min)",
                               y="Precursor m/z",
                               height=600,
                               hover_name="Metabolite name",
                               color="Intensity",
                               color_continuous_scale=colorscale,
                               labels={"Retention time (min)": "Retention time (min)",
                                       "Precursor m/z": "Precursor m/z",
                                       "Intensity": "Intensity"},
                               log_x=False)
    urine_rt_plot.update_layout(showlegend=False,
                                transition_duration=500,
                                clickmode="event",
                                margin=dict(t=75, b=75))
    urine_rt_plot.update_xaxes(title="Retention time (min)")
    urine_rt_plot.update_yaxes(title="Precursor m/z")
    urine_rt_plot.update_traces(marker={'size': 30})

    return urine_rt_plot


def urine_bar_plot(dataframe, study, feature_name, polarity):

    """
    Returns bar plot figure of intensity vs. study for urine features
    """

    if polarity == "pos":
        inchikey = pos_urine_features_dict[feature_name]
    elif polarity == "neg":
        inchikey = neg_urine_features_dict[feature_name]

    intensities = dataframe[inchikey].astype(float).values.tolist()
    intensities = ['{:.2e}'.format(x) for x in intensities]

    urine_intensity_plot = px.bar(dataframe,
                                  x=study,
                                  y=inchikey,
                                  text=intensities,
                                  height=600)
    urine_intensity_plot.update_layout(title="QC Urine Benchmark",
                                       showlegend=False,
                                       transition_duration=500,
                                       clickmode="event",
                                       xaxis=dict(rangeslider=dict(visible=True), autorange=True),
                                       legend=dict(font=dict(size=10)),
                                       margin=dict(t=75, b=75))
    urine_intensity_plot.update_xaxes(title="Study")
    urine_intensity_plot.update_yaxes(title="Intensity")
    urine_intensity_plot.update_traces(textposition='outside',
                                      hovertemplate=f'{feature_name}' + '<br>Study: %{x} <br>Intensity: %{text}<br>')

    return urine_intensity_plot


def get_samples(instrument):

    """
    Returns list of samples for a given study run on an instrument
    """

    files = study_loaded[instrument]["study_file"]
    pass_fail_list = []

    df_pos = files["rt_pos"]
    df_neg = files["rt_neg"]

    pos_samples = df_pos.columns.tolist()
    neg_samples = df_neg.columns.tolist()
    pos_samples.remove("Title")
    neg_samples.remove("Title")
    samples = pos_samples + neg_samples

    for column in pos_samples:
        if (df_pos[column] == 0).sum() >= 4:
            pass_fail_list.append("Fail")
        else:
            pass_fail_list.append("Pass")

    for column in neg_samples:
        if (df_neg[column] == 0).sum() >= 4:
            pass_fail_list.append("Fail")
        else:
            pass_fail_list.append("Pass")

    df_samples = pd.DataFrame()
    df_samples["Sample"] = samples
    df_samples["Order"] = df_samples["Sample"].str.split("_").str[-1]
    df_samples["QC"] = pass_fail_list
    df_samples.sort_values(by="Order", ascending=False, inplace=True)

    df_samples["Sample"] = df_samples['Sample'].str.replace(": RT Info", "")
    df_samples.drop(columns=["Order"], inplace=True)

    return df_samples.to_dict("records")


@app.callback(Output("header-text", "children"),
              Input("QE1-table", "active_cell"),
              State("QE1-table", "data"),
              Input("QE2-table", "active_cell"),
              State("QE2-table", "data"),
              Input("QE1-sample-table", "data"),
              Input("QE2-sample-table", "data"), prevent_initial_call=True)
def loading_data_feedback(active_cell_QE1, table_data_QE1, active_cell_QE2, table_data_QE2, placeholder_input_1, placeholder_input_2):

    """
    Dash callback for providing user feedback when retrieving data from Google Drive
    """

    study_name_QE1 = study_loaded["QE 1"]["study_name"]
    study_name_QE2 = study_loaded["QE 2"]["study_name"]
    header = "MS-AutoQC"

    if active_cell_QE1:
        if table_data_QE1[active_cell_QE1['row']][active_cell_QE1['column_id']] != study_name_QE1:
            header = "Loading data..."
        else:
            header = "MS-AutoQC"

    if active_cell_QE2:
        if table_data_QE2[active_cell_QE2['row']][active_cell_QE2['column_id']] != study_name_QE2:
            header = "Loading data..."
        else:
            header = "MS-AutoQC"

    return header


@app.callback(Output("QE1-table", "data"),
              Output("QE2-table", "data"),
              Output("QE1-table-container", "style"),
              Output("QE2-table-container", "style"),
              Input("header", "children"))
def populate_instrument_tables(placeholder_input):

    """
    Dash callback for populating tables with list of past/active instrument runs
    """

    list_of_QE1_studies = []
    list_of_QE2_studies = []

    QE1_files = drive.ListFile({'q': "'" + study_loaded["QE 1"]["drive_id"] + "' in parents and trashed=false"}).GetList()
    QE2_files = drive.ListFile({'q': "'" + study_loaded["QE 2"]["drive_id"] + "' in parents and trashed=false"}).GetList()

    for file in QE1_files:
        study = file["title"].split("_")[0]
        if study not in list_of_QE1_studies and "urine" not in file["title"]:
            list_of_QE1_studies.append(study)

    for file in QE2_files:
        study = file["title"].split("_")[0]
        if study not in list_of_QE2_studies and "urine" not in file["title"]:
            list_of_QE2_studies.append(study)

    data_for_QE1 = [{"Past / Active Studies": study} for study in list_of_QE1_studies]
    data_for_QE2 = [{"Past / Active Studies": study} for study in list_of_QE2_studies]

    if list_of_QE1_studies == []:
        data_for_QE1 = [{"Past / Active Studies": "No studies found"}]
    if list_of_QE2_studies == []:
        data_for_QE1 = [{"Past / Active Studies": "No studies found"}]

    return data_for_QE1, data_for_QE2, {"display": "block"}, {"display": "block"}


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


@app.callback(Output("QE1-istd-rt-dropdown", "options"),
              Output("QE1-istd-intensity-dropdown", "options"),
              Output("QE2-istd-rt-dropdown", "options"),
              Output("QE2-istd-intensity-dropdown", "options"),
              Output("QE1-urine-intensity-dropdown", "options"),
              Output("QE2-urine-intensity-dropdown", "options"),
              Input("QE1-polarity-options", "value"),
              Input("QE2-polarity-options", "value"), prevent_initial_call=True)
def update_dropdowns(polarity_QE1, polarity_QE2):

    """
    Updates internal standard dropdown list with correct standards for corresponding polarity
    """

    neg_internal_standards = ["1_Methionine_d8", "1_Creatinine_d3", "1_CUDA", "1_Glutamine_d5", "1_Glutamic Acid_d3",
                              "1_Arginine_d7", "1_Tryptophan d5", "1_Serine d3", "1_Hippuric acid d5"]

    # QE 1
    if polarity_QE1 == "neg":
        QE1_istd_dropdown = neg_internal_standards
        QE1_urine_dropdown = list(neg_urine_features_dict.keys())
        study_loaded["QE 1"]["ui_callback"] = True

    elif polarity_QE1 == "pos":
        QE1_istd_dropdown = standards_list
        QE1_urine_dropdown = list(pos_urine_features_dict.keys())
        study_loaded["QE 1"]["ui_callback"] = True

    # QE 2
    if polarity_QE2 == "neg":
        QE2_istd_dropdown = neg_internal_standards
        QE2_urine_dropdown = list(neg_urine_features_dict.keys())
        study_loaded["QE 2"]["ui_callback"] = True

    elif polarity_QE2 == "pos":
        QE2_istd_dropdown = standards_list
        QE2_urine_dropdown = list(pos_urine_features_dict.keys())
        study_loaded["QE 2"]["ui_callback"] = True

    return QE1_istd_dropdown, QE1_istd_dropdown, QE2_istd_dropdown, QE2_istd_dropdown, QE1_urine_dropdown, QE2_urine_dropdown


@app.callback(Output("QE1-polarity-options", "value"),
              Input("QE1-istd-rt-dropdown", "value"),
              Input("QE1-istd-intensity-dropdown", "value"),
              Input("QE1-urine-intensity-dropdown", "value"),
              Input("QE1-polarity-options", "value"),
              Input("QE1-urine-rt-plot", "clickData"), prevent_initial_call=True)
def dropdown_callback_intermediate_for_QE1(dropdown1, dropdown2, dropdown3, polarity, click_data):

    """
    Helper function for setting the "ui_callback" key to True,
    so that the populate_QE1_plots() function only re-download QC data
    when a study is selected from the table, not when dropdowns are changed
    """

    study_loaded["QE 1"]["ui_callback"] = True
    return polarity


@app.callback(Output("QE2-polarity-options", "value"),
              Input("QE2-istd-rt-dropdown", "value"),
              Input("QE2-istd-intensity-dropdown", "value"),
              Input("QE2-urine-intensity-dropdown", "value"),
              Input("QE2-polarity-options", "value"),
              Input("QE2-urine-rt-plot", "clickData"), prevent_initial_call=True)
def dropdown_callback_intermediate_for_QE2(dropdown1, dropdown2, dropdown3, polarity, click_data):

    """
    Helper function for setting the "ui_callback" key to True,
    so that the populate_QE2_plots() function only re-download QC data
    when a study is selected from the table, not when dropdowns are changed
    """

    study_loaded["QE 2"]["ui_callback"] = True
    return polarity


@app.callback(Output("placeholder-one", "children"),
              Input("QE1-urine-rt-plot", "clickData"), prevent_initial_call=True)
def click_callback_intermediate_for_QE1(click_data):

    study_loaded["QE 1"]["clicked_feature"] = True
    return ""


@app.callback(Output("placeholder-two", "children"),
              Input("QE2-urine-rt-plot", "clickData"), prevent_initial_call=True)
def click_callback_intermediate_for_QE2(click_data):

    study_loaded["QE 2"]["clicked_feature"] = True
    return ""


@app.callback(Output("QE1-istd-rt-plot", "figure"),
              Output("QE1-urine-rt-plot", "figure"),
              Output("QE1-istd-intensity-plot", "figure"),
              Output("QE1-urine-intensity-plot", "figure"),
              Output("QE1-urine-intensity-dropdown", "value"),
              Input("QE1-table", "active_cell"),
              State("QE1-table", "data"),
              Input("QE1-polarity-options", "value"),
              Input("QE1-istd-rt-dropdown", "value"),
              Input("QE1-istd-intensity-dropdown", "value"),
              Input("QE1-urine-intensity-dropdown", "value"), prevent_initial_call=True)
def populate_QE1_plots(active_cell, table_data, polarity, scatter_plot_standard, bar_plot_standard, urine_plot_feature):

    """
    Dash callback for loading QE 1 instrument data into scatter and bar plots
    """

    # If a study was selected
    if active_cell:

        # Get name of clicked study from table
        study_name = table_data[active_cell['row']][active_cell['column_id']]

        if study_name != "No studies found":

            # Retrieve data for clicked study and store as a dictionary
            if study_loaded["QE 1"]["study_name"] != study_name or study_loaded["QE 1"]["ui_callback"] == False:
                files = get_data("QE 1", study_name)
                study_loaded["QE 1"]["study_name"] = study_name
                study_loaded["QE 1"]["study_file"] = files

            elif study_loaded["QE 1"]["ui_callback"] == True or study_loaded["QE 1"]["clicked_feature"] == True:
                files = study_loaded["QE 1"]["study_file"]

            # Get internal standards from QC DataFrames for RT scatter plot
            if polarity == "pos":
                internal_standards = files["rt_pos"]["Title"].astype(str).tolist()
                urine_features_dict = pos_urine_features_dict
            elif polarity == "neg":
                internal_standards = files["rt_neg"]["Title"].astype(str).tolist()
                urine_features_dict = neg_urine_features_dict

            # Set initial dropdown values when none are selected
            if not scatter_plot_standard:
                scatter_plot_standard = internal_standards[0]

            if not bar_plot_standard:
                bar_plot_standard = internal_standards[0]

            # Get clicked or selected feature from QC urine m/z-RT plot
            if not urine_plot_feature:
                urine_plot_feature = list(urine_features_dict.keys())[0]

            if urine_plot_feature and study_loaded["QE 1"]["ui_callback"] == True:
                study_loaded["QE 1"]["ui_callback"] == False

            if click_data and study_loaded["QE 1"]["clicked_feature"] == True:
                urine_plot_feature = click_data["points"][0]["hovertext"]
                study_loaded["QE 1"]["clicked_feature"] = False

            # Prepare DataFrames for plotting
            df_istd_rt = files["rt_" + polarity]
            df_istd_intensity = files["intensity_" + polarity]
            df_urine_rt = files["urine_rt_" + polarity]
            df_urine_intensity = files["urine_intensity_" + polarity]
            df_urine_mz = files["urine_mz_" + polarity]

            # Transpose DataFrames
            df_istd_rt = df_istd_rt.transpose()
            df_istd_intensity = df_istd_intensity.transpose()
            df_urine_intensity = df_urine_intensity.transpose()

            for dataframe in [df_istd_rt, df_istd_intensity, df_urine_intensity]:
                dataframe.columns = dataframe.iloc[0]
                dataframe.drop(dataframe.index[0], inplace=True)

            # Split text in internal_standard DataFrames
            for istd in internal_standards:
                rt = df_istd_rt[istd].str.split(": ").str[0]
                rt_diff = df_istd_rt[istd].str.split(": ").str[1]
                df_istd_rt[istd] = rt.astype(float)
                # df_istd_rt[istd + "_diff"] = rt_diff

            # Get list of samples and features from transposed DataFrames
            samples = df_istd_rt.index.values.tolist()
            samples = [sample.replace(": RT Info", "") for sample in samples]

            try:

                # Internal standards – retention time vs. sample
                istd_rt_plot = istd_scatter_plot(dataframe=df_istd_rt,
                                                 x=samples,
                                                 y=scatter_plot_standard)

                # Internal standards – intensity vs. sample
                istd_intensity_plot = istd_bar_plot(dataframe=df_istd_intensity,
                                                          x=df_istd_intensity.index,
                                                          y=bar_plot_standard,
                                                          text=samples)

                # Urine features – retention time vs. feature
                urine_rt_plot = urine_scatter_plot(study_name=study_name,
                                                   df_rt=df_urine_rt,
                                                   df_mz=df_urine_mz,
                                                   df_intensity=files["urine_intensity_" + polarity],
                                                   urine_features_dict=urine_features_dict)

                # Urine features – intensity vs. feature
                urine_intensity_plot = urine_bar_plot(dataframe=df_urine_intensity,
                                                      study=df_urine_intensity.index,
                                                      feature_name=urine_plot_feature,
                                                      polarity=polarity)

                study_loaded["QE 1"]["ui_callback"] = False

                return istd_rt_plot, urine_rt_plot, istd_intensity_plot, urine_intensity_plot, urine_plot_feature

            except Exception as error:
                print(error)
                return dash.no_update

        else:
            return dash.no_update

    else:
        return dash.no_update

    study_loaded["QE 1"]["ui_callback"] = False


@app.callback(Output("QE2-istd-rt-plot", "figure"),
              Output("QE2-urine-rt-plot", "figure"),
              Output("QE2-istd-intensity-plot", "figure"),
              Output("QE2-urine-intensity-plot", "figure"),
              Output("QE2-urine-intensity-dropdown", "value"),
              Input("QE2-table", "active_cell"),
              State("QE2-table", "data"),
              Input("QE2-polarity-options", "value"),
              Input("QE2-istd-rt-dropdown", "value"),
              Input("QE2-istd-intensity-dropdown", "value"),
              Input("QE2-urine-intensity-dropdown", "value"),
              Input("QE2-urine-rt-plot", "clickData"), prevent_initial_call=True)
def populate_QE2_plots(active_cell, table_data, polarity, scatter_plot_standard, bar_plot_standard, urine_plot_feature, click_data):

    """
    Dash callback for loading QE 2 instrument data into scatter and bar plots
    """

    # If a study was selected
    if active_cell:

        # Get name of clicked study from table
        study_name = table_data[active_cell['row']][active_cell['column_id']]

        if study_name != "No studies found":

            # Retrieve data for clicked study and store as a dictionary
            if study_loaded["QE 2"]["study_name"] != study_name or study_loaded["QE 2"]["ui_callback"] == False:
                files = get_data("QE 2", study_name)
                study_loaded["QE 2"]["study_name"] = study_name
                study_loaded["QE 2"]["study_file"] = files

            elif study_loaded["QE 2"]["ui_callback"] == True or study_loaded["QE 2"]["clicked_feature"] == True:
                files = study_loaded["QE 2"]["study_file"]

            # Get internal standards from QC DataFrames for RT scatter plot
            if polarity == "pos":
                internal_standards = files["rt_pos"]["Title"].astype(str).tolist()
                urine_features_dict = pos_urine_features_dict
            elif polarity == "neg":
                internal_standards = files["rt_neg"]["Title"].astype(str).tolist()
                urine_features_dict = neg_urine_features_dict

            # Set initial dropdown values when none are selected
            if not scatter_plot_standard:
                scatter_plot_standard = internal_standards[0]

            if not bar_plot_standard:
                bar_plot_standard = internal_standards[0]

            # Get clicked or selected feature from QC urine m/z-RT plot
            if not urine_plot_feature:
                urine_plot_feature = list(urine_features_dict.keys())[0]

            if urine_plot_feature and study_loaded["QE 2"]["ui_callback"] == True:
                study_loaded["QE 2"]["ui_callback"] == False

            if click_data and study_loaded["QE 2"]["clicked_feature"] == True:
                urine_plot_feature = click_data["points"][0]["hovertext"]
                study_loaded["QE 2"]["clicked_feature"] = False

            # Prepare DataFrames for plotting
            df_istd_rt = files["rt_" + polarity]
            df_istd_intensity = files["intensity_" + polarity]
            df_urine_rt = files["urine_rt_" + polarity]
            df_urine_intensity = files["urine_intensity_" + polarity]
            df_urine_mz = files["urine_mz_" + polarity]

            # Transpose DataFrames
            df_istd_rt = df_istd_rt.transpose()
            df_istd_intensity = df_istd_intensity.transpose()
            df_urine_intensity = df_urine_intensity.transpose()

            for dataframe in [df_istd_rt, df_istd_intensity, df_urine_intensity]:
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
                istd_rt_plot = istd_scatter_plot(dataframe=df_istd_rt,
                                                 x=samples,
                                                 y=scatter_plot_standard)

                # Internal standards – intensity vs. sample
                istd_intensity_plot = istd_bar_plot(dataframe=df_istd_intensity,
                                                    x=df_istd_intensity.index,
                                                    y=bar_plot_standard,
                                                    text=samples)

                # Urine features – retention time vs. feature
                urine_rt_plot = urine_scatter_plot(study_name=study_name,
                                                   df_rt=df_urine_rt,
                                                   df_mz=df_urine_mz,
                                                   df_intensity=files["urine_intensity_" + polarity],
                                                   urine_features_dict=urine_features_dict)

                # Urine features – intensity vs. feature
                urine_intensity_plot = urine_bar_plot(dataframe=df_urine_intensity,
                                                      study=df_urine_intensity.index,
                                                      feature_name=urine_plot_feature,
                                                      polarity=polarity)

                study_loaded["QE 2"]["ui_callback"] = False

                return istd_rt_plot, urine_rt_plot, istd_intensity_plot, urine_intensity_plot, urine_plot_feature

            except Exception as error:
                print(error)
                return dash.no_update

        else:
            return dash.no_update

    else:
        return dash.no_update

    study_loaded["QE 2"]["ui_callback"] = False


if __name__ == "__main__":

    # if sys.platform == "win32":
    #     chrome_path = "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe"
    #     webbrowser.register('chrome', None, webbrowser.BackgroundBrowser(chrome_path))
    #     webbrowser.get("chrome").open("http://127.0.0.1:8050/")
    # elif sys.platform == "darwin":
    #     webbrowser.get("chrome").open("http://127.0.0.1:8050/", new=1)

    # Start Dash app
    app.run_server(debug=True)