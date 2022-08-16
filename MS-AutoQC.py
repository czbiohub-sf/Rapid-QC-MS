import os, sys, webbrowser
import pandas as pd
import plotly.express as px
from dash import dash, dcc, html, dash_table, Input, Output, State
import dash_bootstrap_components as dbc
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

# Local resources for plots
study_loaded = {
    "QE 1": {
        "study_name": "",
        "study_file": "",
        "chromatography": "",
        "pos_internal_standards": "",
        "neg_internal_standards": "",
        "retention_times_dict": "",
        "drive_id": "1-0y1jUARBM1DwExjrhyl0WF3KRLFWHom",
        "ui_callback": False,
        "clicked_feature": False,
        "df_samples": ""
    },
    "QE 2": {
        "study_name": "",
        "study_file": "",
        "chromatography": "",
        "pos_internal_standards": "",
        "neg_internal_standards": "",
        "retention_times_dict": "",
        "drive_id": "1-9unZeOHyTPYZScox5Wv9X0CxTWIE-Ih",
        "ui_callback": False,
        "clicked_feature": False,
        "df_samples": ""
    },
}

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

standards_list = ["1_Methionine_d8", "1_1_Methylnicotinamide_d3", "1_Creatinine_d3", "1_Carnitine_d3",
             "1_Acetylcarnitine_d3", "1_TMAO_d9", "1_Choline_d9", "1_Glutamine_d5", "1_CUDA", "1_Glutamic Acid_d3",
             "1_Arginine_d7", "1_Alanine_d3", "1_Valine d8", "1_Tryptophan d5", "1_Serine d3", "1_Lysine d8",
             "1_Phenylalanine d8", "1_Hippuric acid d5"]

pos_internal_standards_HILIC = standards_list

neg_internal_standards_HILIC = ["1_Methionine_d8", "1_Creatinine_d3", "1_CUDA", "1_Glutamine_d5", "1_Glutamic Acid_d3",
                                "1_Arginine_d7", "1_Tryptophan d5", "1_Serine d3", "1_Hippuric acid d5"]

pos_internal_standards_C18 = ["1_Acetylcarnitine_d3", "1_CUDA", "1_Valine d8", "1_Tryptophan d5",
                              "1_Phenylalanine d8", "1_Hippuric acid d5"]

neg_internal_standards_C18 = ["1_CUDA", "1_Glutamine_d5", "1_Glutamic Acid_d3", "1_Phenylalanine_d8",
                              "1_Tryptophan d5", "1_Hippuric acid d5"]

retention_times_HILIC = {
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

retention_times_C18 = {
    "1_Acetylcarnitine_d3": 1.388,
    "1_CUDA": 7.798,
    "1_Glutamine_d5": 0.99,
    "1_Glutamic Acid_d3": 1.014,
    "1_Valine d8": 1.259,
    "1_Tryptophan d5": 3.303,
    "1_Phenylalanine d8": 2.827,
    "1_Hippuric acid d5": 3.952
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
def serve_layout():

    biohub_logo = "https://user-images.githubusercontent.com/7220175/184942387-0acf5deb-d81e-4962-ab27-05b453c7a688.png"

    # Clear instrument dictionary data
    elements_to_clear = ["study_name", "chromatography", "study_file", "df_samples", "pos_internal_standards",
                     "neg_internal_standards"]
    for instrument in ["QE 1", "QE 2"]:
        for key in elements_to_clear:
            study_loaded[instrument][key] = ""

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

        # Header
        # html.Div(id="header", children=[
        #     html.H1(id="header-text")
        # ]),

        # App layout
        html.Div(className="page", children=[

            dcc.Tabs(id="tabs", children=[

                # QC dashboard for QE 1
                dcc.Tab(label="Thermo QE 1", children=[

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
                            ])]),

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
                            style_table={"height": "473px",
                                         "overflowY": "auto"},
                            style_data_conditional=[
                                {"if": {"filter_query": "{QC} = 'Fail'"},
                                    "backgroundColor": bootstrap_colors["red-low-opacity"],
                                    "font-weight": "bold"
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

                    # Modal/dialog for sample information card
                    dbc.Modal(id="QE1-sample-info-modal", size="xl", centered=True, is_open=False, scrollable=True, children=[
                        dbc.ModalHeader(dbc.ModalTitle(id="QE1-sample-modal-title"), close_button=True),
                        dbc.ModalBody(id="QE1-sample-modal-body"),
                        dbc.ModalFooter(
                            dbc.Button(
                                "Close",
                                id="QE1-close-modal",
                                className="ms-auto",
                                n_clicks=0,
                            )
                        )]
                    ),

                    # Modal/dialog for alerting user that data is loading
                    dbc.Modal(id="QE1-loading-modal", size="md", centered=True, is_open=False, scrollable=True, children=[
                        dbc.ModalHeader(dbc.ModalTitle(id="QE1-loading-modal-title")),
                        dbc.ModalBody(id="QE1-loading-modal-body")
                  ]),

                ]),

                # QC dashboard for QE 2
                dcc.Tab(label="Thermo QE 2", children=[

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
                            ])]),

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
                            style_table={"height": "473px",
                                         "overflowY": "auto"},
                            style_data_conditional=[
                                {"if": {"filter_query": "{QC} = 'Fail'"},
                                    "backgroundColor": bootstrap_colors["red-low-opacity"],
                                    "font-weight": "bold"
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

                    # Modal/dialog for sample information card
                    dbc.Modal(id="QE2-sample-info-modal", size="xl", centered=True, is_open=False, scrollable=True, children=[
                        dbc.ModalHeader(dbc.ModalTitle(id="QE2-sample-modal-title"), close_button=True),
                        dbc.ModalBody(id="QE2-sample-modal-body"),
                        dbc.ModalFooter(
                            dbc.Button(
                                "Close",
                                id="QE2-close-modal",
                                className="ms-auto",
                                n_clicks=0,
                            )
                        )]
                    ),

                    # Modal/dialog for alerting user that data is loading
                    dbc.Modal(id="QE2-loading-modal", size="md", centered=True, is_open=False, scrollable=True, children=[
                        dbc.ModalHeader(dbc.ModalTitle(id="QE2-loading-modal-title")),
                        dbc.ModalBody(id="QE2-loading-modal-body")
                    ]),

                    html.Div(id="placeholder-one", children=""),
                    html.Div(id="placeholder-two", children=""),

                ]),

                dcc.Tab(label="Fusion Lumos 1", children=[]),

                dcc.Tab(label="Fusion Lumos 2", children=[]),

                dcc.Tab(label="Bruker timsTOF", children=[]),

            ])

        ])

    ])


app.layout = serve_layout


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
    file_list = drive.ListFile({"q": "'" + study_loaded[instrument]["drive_id"] + "' in parents and trashed=false"}).GetList()

    for file in file_list:

        # Download files for study
        if study_id in file["title"] or "urine" in file["title"]:
            file.GetContentFile(file["title"])

        # Set chromatography (if it hasn't already been set)
        if chromatography == "" and study_id in file["title"]:

            if "HILIC" in file["title"]:
                study_loaded[instrument]["chromatography"] = "HILIC"
                study_loaded[instrument]["pos_internal_standards"] = pos_internal_standards_HILIC
                study_loaded[instrument]["neg_internal_standards"] = neg_internal_standards_HILIC
                study_loaded[instrument]["retention_times_dict"] = retention_times_HILIC
                chromatography = "HILIC"

            elif "C18" in file["title"]:
                study_loaded[instrument]["chromatography"] = "C18"
                study_loaded[instrument]["pos_internal_standards"] = pos_internal_standards_C18
                study_loaded[instrument]["neg_internal_standards"] = neg_internal_standards_C18
                study_loaded[instrument]["retention_times_dict"] = retention_times_C18
                chromatography = "C18"

            elif "Lipidomics" in file["title"]:
                study_loaded[instrument]["chromatography"] = "Lipidomics"
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

            # Retrieve metadata and sequence files from bufferbox2
            df_metadata = pd.read_csv(study_id + "_seq_MetaData.csv", index_col=False)
            df_sequence = pd.read_csv(study_id + "_seq.csv")
            df_sequence.columns = df_sequence.iloc[0]
            df_sequence = df_sequence.drop(df_sequence.index[0])

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
        files["sequence"] = df_sequence
        files["metadata"] = df_metadata
        files["urine_mz_pos"] = df_urine_mz_pos
        files["urine_mz_neg"] = df_urine_mz_neg
        files["urine_rt_pos"] = df_urine_rt_pos
        files["urine_rt_neg"] = df_urine_rt_neg
        files["urine_intensity_pos"] = df_urine_intensity_pos
        files["urine_intensity_neg"] = df_urine_intensity_neg

        return files

    except Exception as error:
        return "Data parsing error: " + str(error)


def generate_sample_metadata_dataframe(sample, instrument):

    """
    Creates a DataFrame for a single sample with m/z, RT, intensity and metadata info
    """

    if "pos" in sample.lower():
        polarity = "pos"
    elif "neg" in sample.lower():
        polarity = "neg"

    df_sample_istd = pd.DataFrame()
    df_sample_info = pd.DataFrame()

    df_istd_rt = study_loaded[instrument]["study_file"]["rt_" + polarity]
    df_istd_delta_mz = study_loaded[instrument]["study_file"]["mz_" + polarity]
    df_istd_intensity = study_loaded[instrument]["study_file"]["intensity_" + polarity]

    df_sequence = study_loaded[instrument]["study_file"]["sequence"]
    df_metadata = study_loaded[instrument]["study_file"]["metadata"]

    df_sequence = df_sequence.loc[df_sequence["File Name"].astype(str) == sample]
    df_metadata = df_metadata.loc[df_metadata["Filename"].astype(str) == sample]

    internal_standards = df_istd_rt["Title"].astype(str).tolist()
    retention_times = df_istd_rt[sample + ": RT Info"].astype(str).tolist()
    intensities = df_istd_intensity[sample + ": Height"].fillna("0").astype(float).tolist()
    mz_values = df_istd_delta_mz[sample + ": Precursor m/z Info"].astype(str).tolist()

    df_sample_istd["Internal Standard"] = [x.replace("1_", "") for x in internal_standards]
    df_sample_istd["m/z"] = [x.split(": ")[0] for x in mz_values]
    df_sample_istd["RT"] = [x.split(": ")[0] for x in retention_times]
    df_sample_istd["Intensity"] = ["{:.2e}".format(x) for x in intensities]
    df_sample_istd["Delta RT"] = [x.split(": ")[-1] for x in retention_times]
    df_sample_istd["Delta m/z"] = [x.split(": ")[-1] for x in mz_values]

    if len(df_sequence) > 0:
        df_sample_info["Sample ID"] = df_sequence["L1 Study"].astype(str).values
        df_sample_info["Position"] = df_sequence["Position"].astype(str).values
        df_sample_info["Injection Volume"] = df_sequence["Inj Vol"].astype(str).values + " uL"
        df_sample_info["Instrument Method"] = df_sequence["Instrument Method"].astype(str).values

    if len(df_metadata) > 0:
        df_sample_info["Species"] = df_metadata["Species"].astype(str).values
        df_sample_info["Matrix"] = df_metadata["Matrix"].astype(str).values
        df_sample_info["Growth-Harvest Conditions"] = df_metadata["Growth-Harvest Conditions"].astype(str).values
        df_sample_info["Treatment"] = df_metadata["Treatment"].astype(str).values

    df_sample_info = df_sample_info.append(df_sample_info.iloc[0])
    df_sample_info.iloc[0] = df_sample_info.columns.tolist()
    df_sample_info = df_sample_info.rename(index={0: "Sample Information"})
    df_sample_info = df_sample_info.transpose()

    return df_sample_istd, df_sample_info


def load_istd_rt_plot(dataframe, samples, internal_standard, retention_times_dict):

    """
    Returns scatter plot figure of retention time vs. sample for internal standards
    """

    samples = [sample + ": RT Info" for sample in samples]
    samples = sorted(samples, key=lambda x: str(x.split("_")[-1]))
    df_filtered_by_samples = dataframe.loc[samples]

    y_min = retention_times_dict[internal_standard] - 0.1
    y_max = retention_times_dict[internal_standard] + 0.1
    # median_rt = dataframe[y].median()

    fig = px.line(df_filtered_by_samples,
                  title="Internal Standards RT – " + standards_dict[internal_standard],
                  x=samples,
                  y=internal_standard,
                  height=600,
                  markers=True,
                  hover_name=samples,
                  labels={"variable": "Internal Standard",
                          "index": "Sample",
                          "value": "Retention Time"},
                  log_x=False,
                  color_discrete_map=istd_colors)
    fig.update_layout(transition_duration=500,
                     clickmode="event",
                     showlegend=False,
                     legend_title_text="Internal Standards",
                     margin=dict(t=75, b=75))
    fig.update_xaxes(showticklabels=False, title="Sample")
    fig.update_yaxes(title="Retention time (min)", range=[y_min, y_max])
    fig.add_hline(y=retention_times_dict[internal_standard], line_width=2, line_dash="dash")
    fig.update_traces(hovertemplate="Sample: %{x} <br>Retention Time: %{y} min<br>")

    return fig


def load_istd_intensity_plot(dataframe, samples, internal_standard, text):

    """
    Returns bar plot figure of intensity vs. sample for internal standards
    """

    samples = [sample + ": Height" for sample in samples]
    samples = sorted(samples, key=lambda x: str(x.split("_")[-1]))
    df_filtered_by_samples = dataframe.loc[samples]

    fig = px.bar(df_filtered_by_samples,
                 title="Internal Standards Intensity – " + standards_dict[internal_standard],
                 x=samples,
                 y=internal_standard,
                 text=text,
                 height=600)
    fig.update_layout(showlegend=False,
                      transition_duration=500,
                      clickmode="event",
                      xaxis=dict(rangeslider=dict(visible=True), autorange=True),
                      legend=dict(font=dict(size=10)),
                      margin=dict(t=75, b=75))
    fig.update_xaxes(showticklabels=False, title="Sample")
    fig.update_yaxes(title="Intensity")
    fig.update_traces(textposition="outside",
                      hovertemplate="Sample: %{x} <br>Intensity: %{y:.2e}<br>")

    return fig


def load_istd_delta_mz_plot(dataframe, samples, internal_standard):

    """
    Returns scatter plot figure of delta m/z vs. sample for internal standards
    """

    samples = [sample + ": Precursor m/z Info" for sample in samples]
    samples = sorted(samples, key=lambda x: str(x.split("_")[-1]))
    df_filtered_by_samples = dataframe.loc[samples]

    fig = px.line(df_filtered_by_samples,
                  title="Delta m/z – " + standards_dict[internal_standard],
                  x=samples,
                  y=internal_standard,
                  height=600,
                  markers=True,
                  hover_name=samples,
                  labels={"variable": "Internal Standard",
                          "index": "Sample",
                          "value": "Delta m/z"},
                  log_x=False,
                  color_discrete_map=istd_colors)
    fig.update_layout(transition_duration=500,
                      clickmode="event",
                      showlegend=False,
                      legend_title_text="Internal Standards",
                      margin=dict(t=75, b=75))
    fig.update_xaxes(showticklabels=False, title="Sample")
    fig.update_yaxes(title="delta m/z", range=[-0.01, 0.01])
    fig.update_traces(hovertemplate="Sample: %{x} <br>Delta m/z: %{y}<br>")

    return fig


def load_urine_feature_plot(study_name, df_rt, df_mz, df_intensity, urine_features_dict):

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

    fig = px.scatter(urine_df,
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
    fig.update_layout(showlegend=False,
                      transition_duration=500,
                      clickmode="event",
                      margin=dict(t=75, b=75))
    fig.update_xaxes(title="Retention time (min)")
    fig.update_yaxes(title="Precursor m/z")
    fig.update_traces(marker={"size": 30})

    return fig


def load_urine_benchmark_plot(dataframe, study, feature_name, polarity):

    """
    Returns bar plot figure of intensity vs. study for urine features
    """

    if polarity == "pos":
        inchikey = pos_urine_features_dict[feature_name]
    elif polarity == "neg":
        inchikey = neg_urine_features_dict[feature_name]

    intensities = dataframe[inchikey].astype(float).values.tolist()
    intensities = ["{:.2e}".format(x) for x in intensities]

    fig = px.bar(dataframe,
                 x=study,
                 y=inchikey,
                 text=intensities,
                 height=600)
    fig.update_layout(title="QC Urine Benchmark",
                      showlegend=False,
                      transition_duration=500,
                      clickmode="event",
                      xaxis=dict(rangeslider=dict(visible=True), autorange=True),
                      legend=dict(font=dict(size=10)),
                      margin=dict(t=75, b=75))
    fig.update_xaxes(title="Study")
    fig.update_yaxes(title="Intensity")
    fig.update_traces(textposition="outside",
                      hovertemplate=f"{feature_name}" + "<br>Study: %{x} <br>Intensity: %{text}<br>")

    return fig


def get_samples(instrument):

    """
    Returns list of samples for a given study run on an instrument
    """

    files = study_loaded[instrument]["study_file"]

    # Get list of sample names in both polarities
    df_pos = files["rt_pos"]
    df_neg = files["rt_neg"]

    pos_samples = df_pos.columns.tolist()
    neg_samples = df_neg.columns.tolist()
    pos_samples.remove("Title")
    neg_samples.remove("Title")

    samples = pos_samples + neg_samples

    # Determine whether a sample passed or failed QC checks
    # Currently, a QC fail is defined as a sample missing 4 or more internal standards
    pass_fail_list = []

    for column in pos_samples:
        if df_pos[column].isnull().sum() >= 4:
            pass_fail_list.append("Fail")
        else:
            pass_fail_list.append("Pass")

    for column in neg_samples:
        if df_neg[column].isnull().sum() >= 4:
            pass_fail_list.append("Fail")
        else:
            pass_fail_list.append("Pass")

    samples = [sample.replace(": RT Info", "") for sample in samples]

    # Get autosampler positions from sequence file
    df_sequence = files["sequence"]
    df_sequence = df_sequence.loc[df_sequence["File Name"].isin(samples)]
    positions = df_sequence["Position"].astype(str).tolist()
    positions.reverse()

    # Create DataFrame for sample information
    df_samples = pd.DataFrame()
    df_samples["Sample"] = samples
    df_samples["Order"] = df_samples["Sample"].str.split("_").str[-1]
    df_samples["QC"] = pass_fail_list

    df_samples.sort_values(by="Order", ascending=False, inplace=True)
    df_samples.drop(columns=["Order"], inplace=True)

    df_samples["Position"] = positions

    # Store sample DataFrame in local instrument dictionary
    study_loaded[instrument]["df_samples"] = df_samples

    # Return sample dictionary for sample table
    return df_samples.to_dict("records")


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
              State("QE2-loading-modal", "is_open"), prevent_initial_call=True)
def loading_data_feedback(active_cell_QE1, table_data_QE1, active_cell_QE2, table_data_QE2,
                          placeholder_input_1, placeholder_input_2, modal_is_open_QE1, modal_is_open_QE2):

    """
    Dash callback for providing user feedback when retrieving data from Google Drive
    """

    study_name_QE1 = study_loaded["QE 1"]["study_name"]
    study_name_QE2 = study_loaded["QE 2"]["study_name"]

    if active_cell_QE1:
        if table_data_QE1[active_cell_QE1["row"]][active_cell_QE1["column_id"]] != study_name_QE1:
            study_name = table_data_QE1[active_cell_QE1["row"]][active_cell_QE1["column_id"]]
            loading_on = "QE 1"
        else:
            if modal_is_open_QE1:
                return False, None, None, False, None, None

    if active_cell_QE2:
        if table_data_QE2[active_cell_QE2["row"]][active_cell_QE2["column_id"]] != study_name_QE2:
            study_name = table_data_QE2[active_cell_QE2["row"]][active_cell_QE2["column_id"]]
            loading_on = "QE 2"
        else:
            if modal_is_open_QE2:
                return False, None, None, False, None, None

    if loading_on == "QE 1":
        return True, "Loading QC results for " + study_name, "This may take a few seconds...", False, None, None
    elif loading_on == "QE 2":
        return False, None, None, True, "Loading QC results for " + study_name, "This may take a few seconds..."


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

    QE1_files = drive.ListFile({"q": "'" + study_loaded["QE 1"]["drive_id"] + "' in parents and trashed=false"}).GetList()
    QE2_files = drive.ListFile({"q": "'" + study_loaded["QE 2"]["drive_id"] + "' in parents and trashed=false"}).GetList()

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

    # if QE1_studies == []:
    #     studies_on_QE1 = [{"Past / Active Studies": "No studies found"}]
    # if QE2_studies == []:
    #     studies_on_QE2 = [{"Past / Active Studies": "No studies found"}]

    display_div = {"display": "block"}

    return QE1_studies, QE2_studies, display_div, display_div, display_div, display_div


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
              Output("QE1-istd-mz-dropdown", "options"),
              Output("QE1-istd-intensity-dropdown", "options"),
              Output("QE1-urine-intensity-dropdown", "options"),
              Output("QE1-rt-plot-sample-dropdown", "options"),
              Output("QE1-mz-plot-sample-dropdown", "options"),
              Output("QE1-intensity-plot-sample-dropdown", "options"),
              Input("QE1-polarity-options", "value"),
              Input("QE1-sample-table", "data"), prevent_initial_call=True)
def update_QE1_dropdowns_on_polarity_change(polarity, table_data):

    """
    Updates QE 1 dropdown lists with correct items for user-selected polarity
    """

    df_samples = study_loaded["QE 1"]["df_samples"]

    if polarity == "neg":
        istd_dropdown = study_loaded["QE 1"]["neg_internal_standards"]
        urine_dropdown = list(neg_urine_features_dict.keys())
        df_samples = df_samples.loc[df_samples["Sample"].str.contains("Neg")]
        sample_dropdown = df_samples["Sample"].tolist()

    elif polarity == "pos":
        istd_dropdown = study_loaded["QE 1"]["pos_internal_standards"]
        urine_dropdown = list(pos_urine_features_dict.keys())
        df_samples = df_samples.loc[df_samples["Sample"].str.contains("Pos")]
        sample_dropdown = df_samples["Sample"].tolist()

    study_loaded["QE 1"]["ui_callback"] = True

    return istd_dropdown, istd_dropdown, istd_dropdown, urine_dropdown, sample_dropdown, sample_dropdown, sample_dropdown


@app.callback(Output("QE2-istd-rt-dropdown", "options"),
              Output("QE2-istd-intensity-dropdown", "options"),
              Output("QE2-istd-mz-dropdown", "options"),
              Output("QE2-urine-intensity-dropdown", "options"),
              Output("QE2-rt-plot-sample-dropdown", "options"),
              Output("QE2-mz-plot-sample-dropdown", "options"),
              Output("QE2-intensity-plot-sample-dropdown", "options"),
              Input("QE2-polarity-options", "value"),
              Input("QE2-sample-table", "data"), prevent_initial_call=True)
def update_QE2_dropdowns_on_polarity_change(polarity, table_data):

    """
    Updates QE 2 dropdown lists with correct items for user-selected polarity
    """

    df_samples = study_loaded["QE 2"]["df_samples"]

    if polarity == "neg":
        istd_dropdown = study_loaded["QE 2"]["neg_internal_standards"]
        urine_dropdown = list(neg_urine_features_dict.keys())
        df_samples = df_samples.loc[df_samples["Sample"].str.contains("Neg")]
        sample_dropdown = df_samples["Sample"].tolist()

    elif polarity == "pos":
        istd_dropdown = study_loaded["QE 2"]["pos_internal_standards"]
        urine_dropdown = list(pos_urine_features_dict.keys())
        df_samples = df_samples.loc[df_samples["Sample"].str.contains("Pos")]
        sample_dropdown = df_samples["Sample"].tolist()

    study_loaded["QE 2"]["ui_callback"] = True

    return istd_dropdown, istd_dropdown, istd_dropdown, urine_dropdown, sample_dropdown, sample_dropdown, sample_dropdown


@app.callback(Output("QE1-polarity-options", "value"),
              Input("QE1-istd-rt-dropdown", "value"),
              Input("QE1-istd-intensity-dropdown", "value"),
              Input("QE1-istd-mz-dropdown", "value"),
              Input("QE1-urine-intensity-dropdown", "value"),
              Input("QE1-polarity-options", "value"),
              Input("QE1-urine-rt-plot", "clickData"), prevent_initial_call=True)
def dropdown_callback_intermediate_for_QE1(dropdown1, dropdown2, dropdown3, dropdown4, polarity, click_data):

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
              Input("QE2-istd-mz-dropdown", "value"),
              Input("QE2-urine-intensity-dropdown", "value"),
              Input("QE2-polarity-options", "value"),
              Input("QE2-urine-rt-plot", "clickData"), prevent_initial_call=True)
def dropdown_callback_intermediate_for_QE2(dropdown1, dropdown2, dropdown3, dropdown4, polarity, click_data):

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
              Output("QE1-istd-intensity-plot", "figure"),
              Output("QE1-istd-mz-plot", "figure"),
              Output("QE1-urine-rt-plot", "figure"),
              Output("QE1-urine-intensity-plot", "figure"),
              Output("QE1-urine-intensity-dropdown", "value"),
              Input("QE1-table", "active_cell"),
              State("QE1-table", "data"),
              Input("QE1-polarity-options", "value"),
              Input("QE1-istd-rt-dropdown", "value"),
              Input("QE1-istd-intensity-dropdown", "value"),
              Input("QE1-istd-mz-dropdown", "value"),
              Input("QE1-urine-intensity-dropdown", "value"),
              Input("QE1-urine-rt-plot", "clickData"),
              Input("QE1-rt-plot-sample-dropdown", "value"),
              Input("QE1-intensity-plot-sample-dropdown", "value"),
              Input("QE1-mz-plot-sample-dropdown", "value"), prevent_initial_call=True)
def populate_QE1_plots(active_cell, table_data, polarity, rt_plot_standard, intensity_plot_standard, mz_plot_standard,
                       urine_plot_feature, click_data, rt_plot_samples, intensity_plot_samples, mz_plot_samples):

    """
    Dash callback for loading QE 1 instrument data into scatter and bar plots
    """

    instrument = "QE 1"

    # If a study was selected
    if active_cell:

        # Get name of clicked study from table
        study_name = table_data[active_cell["row"]][active_cell["column_id"]]

        if study_name != "No studies found":

            # Retrieve data for clicked study and store as a dictionary
            if study_loaded[instrument]["study_name"] != study_name or study_loaded[instrument]["ui_callback"] == False:
                files = get_data(instrument, study_name)
                study_loaded[instrument]["study_name"] = study_name
                study_loaded[instrument]["study_file"] = files

            elif study_loaded[instrument]["ui_callback"] == True or study_loaded[instrument]["clicked_feature"] == True:
                files = study_loaded[instrument]["study_file"]

            # Get retention times
            retention_times_dict = study_loaded[instrument]["retention_times_dict"]

            # Get internal standards from QC DataFrames for RT scatter plot
            if polarity == "pos":
                internal_standards = files["rt_pos"]["Title"].astype(str).tolist()
                urine_features_dict = pos_urine_features_dict
            elif polarity == "neg":
                internal_standards = files["rt_neg"]["Title"].astype(str).tolist()
                urine_features_dict = neg_urine_features_dict

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

            if urine_plot_feature and study_loaded[instrument]["ui_callback"] == True:
                study_loaded[instrument]["ui_callback"] == False

            if click_data and study_loaded[instrument]["clicked_feature"] == True:
                urine_plot_feature = click_data["points"][0]["hovertext"]
                study_loaded[instrument]["clicked_feature"] = False

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
                                                              text=intensity_plot_samples)

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

                study_loaded[instrument]["ui_callback"] = False

                return istd_rt_plot, istd_intensity_plot, istd_delta_mz_plot, \
                       urine_feature_plot, urine_benchmark_plot, urine_plot_feature

            except Exception as error:
                print(error)
                return dash.no_update

        else:
            return dash.no_update

    else:
        return dash.no_update

    study_loaded[instrument]["ui_callback"] = False


@app.callback(Output("QE2-istd-rt-plot", "figure"),
              Output("QE2-istd-intensity-plot", "figure"),
              Output("QE2-istd-mz-plot", "figure"),
              Output("QE2-urine-rt-plot", "figure"),
              Output("QE2-urine-intensity-plot", "figure"),
              Output("QE2-urine-intensity-dropdown", "value"),
              Input("QE2-table", "active_cell"),
              State("QE2-table", "data"),
              Input("QE2-polarity-options", "value"),
              Input("QE2-istd-rt-dropdown", "value"),
              Input("QE2-istd-intensity-dropdown", "value"),
              Input("QE2-istd-mz-dropdown", "value"),
              Input("QE2-urine-intensity-dropdown", "value"),
              Input("QE2-urine-rt-plot", "clickData"),
              Input("QE2-rt-plot-sample-dropdown", "value"),
              Input("QE2-intensity-plot-sample-dropdown", "value"),
              Input("QE2-mz-plot-sample-dropdown", "value"), prevent_initial_call=True)
def populate_QE2_plots(active_cell, table_data, polarity, rt_plot_standard, intensity_plot_standard, mz_plot_standard,
                       urine_plot_feature, click_data, rt_plot_samples, intensity_plot_samples, mz_plot_samples):

    """
    Dash callback for loading QE 2 instrument data into scatter and bar plots
    """

    instrument = "QE 2"

    # If a study was selected
    if active_cell:

        # Get name of clicked study from table
        study_name = table_data[active_cell["row"]][active_cell["column_id"]]

        if study_name != "No studies found":

            # Retrieve data for clicked study and store as a dictionary
            if study_loaded[instrument]["study_name"] != study_name or study_loaded[instrument]["ui_callback"] == False:
                files = get_data(instrument, study_name)
                study_loaded[instrument]["study_name"] = study_name
                study_loaded[instrument]["study_file"] = files

            elif study_loaded[instrument]["ui_callback"] == True or study_loaded[instrument]["clicked_feature"] == True:
                files = study_loaded[instrument]["study_file"]

            # Get internal standards from QC DataFrames for RT scatter plot
            if polarity == "pos":
                internal_standards = files["rt_pos"]["Title"].astype(str).tolist()
                urine_features_dict = pos_urine_features_dict
            elif polarity == "neg":
                internal_standards = files["rt_neg"]["Title"].astype(str).tolist()
                urine_features_dict = neg_urine_features_dict

            # Get retention times
            retention_times_dict = study_loaded[instrument]["retention_times_dict"]

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

            if urine_plot_feature and study_loaded[instrument]["ui_callback"] == True:
                study_loaded[instrument]["ui_callback"] == False

            if click_data and study_loaded[instrument]["clicked_feature"] == True:
                urine_plot_feature = click_data["points"][0]["hovertext"]
                study_loaded[instrument]["clicked_feature"] = False

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
                                                               text=intensity_plot_samples)

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

                study_loaded[instrument]["ui_callback"] = False

                return istd_rt_plot, istd_intensity_plot, istd_delta_mz_plot, \
                       urine_feature_plot, urine_benchmark_plot, urine_plot_feature

            except Exception as error:
                print(error)
                return dash.no_update

        else:
            return dash.no_update

    else:
        return dash.no_update

    study_loaded[instrument]["ui_callback"] = False


@app.callback(Output("QE1-sample-info-modal", "is_open"),
              Output("QE1-sample-modal-title", "children"),
              Output("QE1-sample-modal-body", "children"),
              Output("QE1-sample-table", "active_cell"),
              Input("QE1-close-modal", "n_clicks"),
              State("QE1-sample-info-modal", "is_open"),
              Input("QE1-sample-table", "active_cell"),
              State("QE1-sample-table", "data"), prevent_initial_call=True)
def toggle_sample_card_for_QE1(close_button, is_open, active_cell, table_data):

    """
    Opens information modal when a sample is clicked from the sample table
    """

    # Get selected sample
    if active_cell:
        clicked_sample = table_data[active_cell["row"]][active_cell["column_id"]]

    # Generate DataFrames with iSTD and metadata info for selected sample
    df_sample_istd, df_sample_info = generate_sample_metadata_dataframe(clicked_sample, "QE 1")

    # Create tables from DataFrames
    metadata_table = dbc.Table.from_dataframe(df_sample_info, striped=True, bordered=True, hover=True)
    istd_table = dbc.Table.from_dataframe(df_sample_istd, striped=True, bordered=True, hover=True)

    # Add tables to sample information modal
    title = clicked_sample
    body = html.Div(children=[metadata_table, istd_table])

    # Toggle modal
    if is_open:
        return False, title, body, None
    else:
        return True, title, body, None


@app.callback(Output("QE2-sample-info-modal", "is_open"),
              Output("QE2-sample-modal-title", "children"),
              Output("QE2-sample-modal-body", "children"),
              Output("QE2-sample-table", "active_cell"),
              Input("QE2-close-modal", "n_clicks"),
              State("QE2-sample-info-modal", "is_open"),
              Input("QE2-sample-table", "active_cell"),
              State("QE2-sample-table", "data"), prevent_initial_call=True)
def toggle_sample_card_for_QE2(close_button, is_open, active_cell, table_data):

    """
    Opens information modal when a sample is clicked from the sample table
    """

    # Get selected sample
    if active_cell:
        clicked_sample = table_data[active_cell["row"]][active_cell["column_id"]]

    # Generate DataFrames with iSTD and metadata info for selected sample
    df_sample_istd, df_sample_info = generate_sample_metadata_dataframe(clicked_sample, "QE 2")

    # Create tables from DataFrames
    metadata_table = dbc.Table.from_dataframe(df_sample_info, striped=True, bordered=True, hover=True)
    istd_table = dbc.Table.from_dataframe(df_sample_istd, striped=True, bordered=True, hover=True)

    # Add tables to sample information modal
    title = clicked_sample
    body = html.Div(children=[metadata_table, istd_table])

    # Toggle modal
    if is_open:
        return False, title, body, None
    else:
        return True, title, body, None


if __name__ == "__main__":

    # if sys.platform == "win32":
    #     chrome_path = "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe"
    #     webbrowser.register("chrome", None, webbrowser.BackgroundBrowser(chrome_path))
    #     webbrowser.get("chrome").open("http://127.0.0.1:8050/")
    # elif sys.platform == "darwin":
    #     webbrowser.get("chrome").open("http://127.0.0.1:8050/", new=1)

    # Start Dash app
    app.run_server(debug=True)