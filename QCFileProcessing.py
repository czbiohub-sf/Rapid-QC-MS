import os, json
import plotly.express as px
import pandas as pd
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

# Authenticate with Google Drive
gauth = GoogleAuth()
drive = GoogleDrive(gauth)

# Get directory for files
current_directory = os.getcwd()

# Define client secrets file
GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = current_directory + "/assets/client_secrets.json"

# Google Drive ID's (from URL) for QE 1 and QE 2 folders
drive_ids = {
    "QE 1": "1-0y1jUARBM1DwExjrhyl0WF3KRLFWHom",
    "QE 2": "1-9unZeOHyTPYZScox5Wv9X0CxTWIE-Ih",
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


def get_data(instrument, study_id):

    """
    Loads internal standard and urine m/z, RT, and peak height from instrument run into pandas DataFrames
    """

    study_resources = {
        "study_name": study_id,
        "chromatography": "",
        "pos_internal_standards": "",
        "neg_internal_standards": "",
        "retention_times_dict": "",
        "ui_callback": False,
        "clicked_feature": False
    }

    # Save .csv files in folder
    files_directory = current_directory + "/qc_files/" + instrument
    if not os.path.exists(files_directory):
        os.makedirs(files_directory)
    os.chdir(files_directory)

    # Auto-iterate through all QC files for the particular study in Google Drive
    file_list = drive.ListFile({"q": "'" + drive_ids[instrument] + "' in parents and trashed=false"}).GetList()

    for file in file_list:

        # Download files for study
        if study_id in file["title"] or "urine" in file["title"]:
            file.GetContentFile(file["title"])

        # Set chromatography (if it hasn't already been set)
        if study_resources["chromatography"] == "" and study_id in file["title"]:

            if "HILIC" in file["title"]:
                study_resources["chromatography"] = "HILIC"
                study_resources["pos_internal_standards"] = pos_internal_standards_HILIC
                study_resources["neg_internal_standards"] = neg_internal_standards_HILIC
                study_resources["retention_times_dict"] = retention_times_HILIC

            elif "C18" in file["title"]:
                study_resources["chromatography"] = "C18"
                study_resources["pos_internal_standards"] = pos_internal_standards_C18
                study_resources["neg_internal_standards"] = neg_internal_standards_C18
                study_resources["retention_times_dict"] = retention_times_C18

            elif "Lipidomics" in file["title"]:
                study_resources["chromatography"] = "Lipidomics"

    chromatography = study_resources["chromatography"]

    # Overarching try/catch
    try:
        # Parse data into pandas DataFrames
        try:
            # Retrieve m/z, RT, and peak height .csv files from bufferbox2
            df_rt_pos = pd.read_csv(study_id + "_RT_" + chromatography + "_Pos.csv", index_col=False)
            df_intensity_pos = pd.read_csv(study_id + "_PeakHeight_" + chromatography + "_Pos.csv", index_col=False).to_json(orient="split")
            df_mz_pos = pd.read_csv(study_id + "_MZ_" + chromatography + "_Pos.csv", index_col=False).to_json(orient="split")
            df_urine_rt_pos = pd.read_csv("urine_RT_" + chromatography + "_Pos.csv", index_col=False).to_json(orient="split")
            df_urine_intensity_pos = pd.read_csv("urine_PeakHeight_" + chromatography + "_Pos.csv", index_col=False).to_json(orient="split")
            df_urine_mz_pos = pd.read_csv("urine_MZ_" + chromatography + "_Pos.csv", index_col=False).to_json(orient="split")

            df_rt_neg = pd.read_csv(study_id + "_RT_" + chromatography + "_Neg.csv", index_col=False)
            df_intensity_neg = pd.read_csv(study_id + "_PeakHeight_" + chromatography + "_Neg.csv", index_col=False).to_json(orient="split")
            df_mz_neg = pd.read_csv(study_id + "_MZ_" + chromatography + "_Neg.csv", index_col=False).to_json(orient="split")
            df_urine_mz_neg = pd.read_csv("urine_MZ_" + chromatography + "_Neg.csv", index_col=False).to_json(orient="split")
            df_urine_rt_neg = pd.read_csv("urine_RT_" + chromatography + "_Neg.csv", index_col=False).to_json(orient="split")
            df_urine_intensity_neg = pd.read_csv("urine_PeakHeight_" + chromatography + "_Neg.csv", index_col=False).to_json(orient="split")

            # Retrieve metadata and sequence files from bufferbox2
            df_metadata = pd.read_csv(study_id + "_seq_MetaData.csv", index_col=False).to_json(orient="split")
            df_sequence = pd.read_csv(study_id + "_seq.csv")
            df_sequence.columns = df_sequence.iloc[0]
            df_sequence = df_sequence.drop(df_sequence.index[0])

            # Generate samples DataFrame
            df_samples = get_samples(df_rt_pos, df_rt_neg, df_sequence).to_json(orient="split")

            df_rt_pos = df_rt_pos.to_json(orient="split")
            df_rt_neg = df_rt_neg.to_json(orient="split")
            df_sequence = df_sequence.to_json(orient="split")

        except Exception as error:
            print("Data retrieval error: " + str(error))
            return None

        return (df_rt_pos, df_rt_neg, df_intensity_pos, df_intensity_neg, df_mz_pos, df_mz_neg, df_sequence, df_metadata, \
        df_urine_rt_pos, df_urine_rt_neg, df_urine_intensity_pos, df_urine_intensity_neg, df_urine_mz_pos, df_urine_mz_neg, \
        json.dumps(study_resources), df_samples)

    except Exception as error:
        print("Data parsing error: " + str(error))
        return None


def get_samples(df_pos, df_neg, df_sequence):

    """
    Returns list of samples for a given study run on an instrument
    """

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
        # elif df_pos[column].isnull().sum() == 3:
        #     pass_fail_list.append("Check")
        else:
            pass_fail_list.append("Pass")

    for column in neg_samples:
        if df_neg[column].isnull().sum() >= 4:
            pass_fail_list.append("Fail")
        # elif df_neg[column].isnull().sum() == 3:
        #     pass_fail_list.append("Check")
        else:
            pass_fail_list.append("Pass")

    samples = [sample.replace(": RT Info", "") for sample in samples]

    # Get autosampler positions from sequence file
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

    # Return sample dictionary for sample table
    return df_samples


def generate_sample_metadata_dataframe(sample, df_istd_rt, df_istd_delta_mz, df_istd_intensity, df_sequence, df_metadata):

    """
    Creates a DataFrame for a single sample with m/z, RT, intensity and metadata info
    """

    if "pos" in sample.lower():
        polarity = "pos"
    elif "neg" in sample.lower():
        polarity = "neg"

    df_sample_istd = pd.DataFrame()
    df_sample_info = pd.DataFrame()

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

    # samples = sorted(samples, key=lambda x: str(x.split("_")[-1]))
    samples = [sample + ": RT Info" for sample in samples]
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
                  log_x=False)
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


def load_istd_intensity_plot(dataframe, samples, internal_standard, text, treatments):

    """
    Returns bar plot figure of intensity vs. sample for internal standards
    """

    # samples = sorted(samples, key=lambda x: str(x.split("_")[-1]))
    samples = [sample + ": Height" for sample in samples]
    df_filtered_by_samples = dataframe.loc[samples]

    if treatments:
        if len(treatments) == len(df_filtered_by_samples):
            df_filtered_by_samples["Treatment"] = treatments
    else:
        df_filtered_by_samples["Treatment"] = " "

    fig = px.bar(df_filtered_by_samples,
                 title="Internal Standards Intensity – " + standards_dict[internal_standard],
                 x=samples,
                 y=internal_standard,
                 text=text,
                 color=df_filtered_by_samples["Treatment"],
                 height=600)
    fig.update_layout(showlegend=False,
                      transition_duration=500,
                      clickmode="event",
                      xaxis=dict(rangeslider=dict(visible=True), autorange=True),
                      legend=dict(font=dict(size=10)),
                      margin=dict(t=75, b=75))
    fig.update_xaxes(showticklabels=False, title="Sample")
    fig.update_yaxes(title="Intensity")
    fig.update_traces(textposition="outside", hovertemplate="Sample: %{x}<br> Intensity: %{y:.2e}<br>")

    return fig


def load_istd_delta_mz_plot(dataframe, samples, internal_standard):

    """
    Returns scatter plot figure of delta m/z vs. sample for internal standards
    """

    # samples = sorted(samples, key=lambda x: str(x.split("_")[-1]))
    samples = [sample + ": Precursor m/z Info" for sample in samples]
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
                  log_x=False)
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

    # Get standard deviation of feature intensities
    df_intensity = df_intensity.fillna(0)
    feature_intensity_from_study = df_intensity.loc[:, study_name + ":Height"].astype(float)
    average_intensity_in_studies = df_intensity.iloc[:, 1:].astype(float).mean(axis=1)
    urine_df["% Change"] = ((feature_intensity_from_study - average_intensity_in_studies).abs() / average_intensity_in_studies) * 100
    urine_df["% Change"] = urine_df["% Change"].fillna(0)

    # plasma = px.colors.sequential.Plasma
    # colorscale = [
    #     [0, plasma[0]],
    #     [1. / 100000, plasma[2]],
    #     [1. / 1000, plasma[4]],
    #     [1. / 10, plasma[7]],
    #     [1., plasma[9]],
    # ]

    fig = px.scatter(urine_df,
                     title="QC Urine Metabolites",
                     x="Retention time (min)",
                     y="Precursor m/z",
                     height=600,
                     hover_name="Metabolite name",
                     color="% Change",
                     color_continuous_scale=px.colors.sequential.Plasma,
                     labels={"Retention time (min)": "Retention time (min)",
                             "Precursor m/z": "Precursor m/z",
                             "Intensity": "Intensity"},
                     log_x=False,
                     range_color=[0, 100])
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


def get_pos_urine_features_dict():

    """
    Returns dictionary of urine features detected in positive ESI mode
    """

    return pos_urine_features_dict


def get_neg_urine_features_dict():

    """
    Returns dictionary of urine features detected in negative ESI mode
    """

    return neg_urine_features_dict