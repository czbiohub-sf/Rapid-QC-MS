import os, io, shutil
import pandas as pd
import sqlalchemy as sa

sqlite_db_location = "sqlite:///data/QC Database.db"

def connect_to_database():

    """
    Connects to local SQLite database
    """

    engine = sa.create_engine(sqlite_db_location)
    db_metadata = sa.MetaData(bind=engine)
    connection = engine.connect()

    return db_metadata, connection


def get_instruments():

    """
    Returns list of instruments in database
    """

    # Connect to SQLite database
    engine = sa.create_engine(sqlite_db_location)

    # Get instruments table as DataFrame
    df_instruments = pd.read_sql("SELECT * FROM instruments", engine)

    # Return list of instruments
    return df_instruments["id"].astype(str).tolist()


def insert_new_run(run_id, instrument_id, chromatography, sequence, metadata, msdial_config_id):

    """
    Inserts a new instrument run into the "runs" table, and
    inserts sample rows into the "sample_qc_results" table.
    """

    # Connect to database
    db_metadata, connection = connect_to_database()

    # Get instrument runs table
    runs_table = sa.Table("runs", db_metadata, autoload=True)

    # Prepare insert of user-inputted run data
    insert_run = runs_table.insert().values(
        {"run_id": run_id,
         "instrument_id": instrument_id,
         "chromatography": chromatography,
         "sequence": sequence,
         "metadata": metadata,
         "status": "active",
         "samples": "",
         "completed": 0,
         "passes": 0,
         "fails": 0,
         "latest_sample": "",
         "msdial_config_id": msdial_config_id})

    # Execute INSERT to database
    connection.execute(insert_run)

    # Get sample_qc_results table
    sample_qc_results_table = sa.Table("sample_qc_results", db_metadata, autoload=True)

    # Get list of samples from sequence
    df_sequence = pd.read_json(sequence, orient="split")
    samples = df_sequence["File Name"].astype(str).tolist()

    # Insert each sample into sample_qc_results table
    for sample in samples:
        insert_sample = sample_qc_results_table.insert().values(
            {"sample_id": sample,
             "run_id": run_id,
             "precursor_mz": "",
             "retention_time": "",
             "intensity": "",
             "md5": ""})
        connection.execute(insert_sample)

    # Close the connection
    connection.close()


def get_instrument_run(run_id):

    """
    Returns DataFrame of selected instrument run from "runs" table
    """

    engine = sa.create_engine(sqlite_db_location)
    query = "SELECT * FROM runs WHERE run_id = '" + run_id + "'"
    df_instrument_run = pd.read_sql(query, engine)
    return df_instrument_run


def get_md5(sample_id):

    """
    Returns MD5 checksum for a data file in "sample_qc_results" table
    """

    # Connect to database
    engine = sa.create_engine(sqlite_db_location)

    # Get sample from "sample_qc_results" table
    df_sample_qc_results = pd.read_sql(
        "SELECT * FROM sample_qc_results WHERE sample_id = '" + sample_id + "'", engine)

    return df_sample_qc_results["md5"].astype(str).values[0]


def update_md5_checksum(sample_id, md5_checksum):

    """
    Updates MD5 checksum for a data file in "sample_qc_results" table
    """

    # Connect to database
    db_metadata, connection = connect_to_database()

    # Get sample_qc_results table
    sample_qc_results_table = sa.Table("sample_qc_results", db_metadata, autoload=True)

    # Prepare update of MD5 checksum at sample row
    update_md5 = (
        sa.update(sample_qc_results_table)
            .where(sample_qc_results_table.c.sample_id == sample_id)
            .values(md5=md5_checksum)
    )

    # Execute UPDATE into database, then close the connection
    connection.execute(update_md5)
    connection.close()


def write_qc_results(sample_id, run_id, json_mz, json_rt, json_intensity, qc_result):

    """
    Updates m/z, RT, and intensity info (as JSON strings) to
    "sample_qc_results" table upon MS-DIAL processing completion.
    """

    # Connect to database
    db_metadata, connection = connect_to_database()

    # Get sample_qc_results table
    sample_qc_results_table = sa.Table("sample_qc_results", db_metadata, autoload=True)

    # Prepare update (insert) of QC results to correct sample row
    update_qc_results = (
        sa.update(sample_qc_results_table)
            .where((sample_qc_results_table.c.sample_id == sample_id)
                   & (sample_qc_results_table.c.run_id == run_id))
            .values(precursor_mz=json_mz,
                    retention_time=json_rt,
                    intensity=json_intensity,
                    qc_result=qc_result)
    )

    # Execute INSERT into database, then close the connection
    connection.execute(update_qc_results)
    connection.close()


def insert_chromatography_method(method_id):

    """
    Inserts new chromatography method in "chromatography_methods" table
    """

    # Connect to database
    db_metadata, connection = connect_to_database()

    # Get "chromatography_methods" table
    chromatography_table = sa.Table("chromatography_methods", db_metadata, autoload=True)

    # Prepare insert
    insert_method = chromatography_table.insert().values(
        {"method_id": method_id,
         "num_pos_standards": 0,
         "num_neg_standards": 0,
         "num_pos_features": 0,
         "num_neg_features": 0,
         "pos_istd_msp_file": "",
         "neg_istd_msp_file": "",
         "pos_bio_msp_file": "",
         "neg_bio_msp_file": "",
         "pos_parameter_file": "",
         "neg_parameter_file": ""})

    # Execute INSERT to database, then close the connection
    connection.execute(insert_method)
    connection.close()


def get_chromatography_methods(for_internal_standards=True):

    """
    Returns DataFrame of chromatography methods
    """

    # Get table from database
    engine = sa.create_engine(sqlite_db_location)
    df_methods = pd.read_sql("SELECT * FROM chromatography_methods", engine)

    # DataFrame refactoring
    df_methods = df_methods.rename(
        columns={"method_id": "Method ID",
        "num_pos_standards": "Positive (+) Mode Standards",
        "num_neg_standards": "Negative (–) Mode Standards",
        "num_pos_features": "Positive (+) Mode Features",
        "num_neg_features": "Negative (–) Mode Features"})

    # Drop columns based on purpose of retrieving methods
    if for_internal_standards:
        df_methods = df_methods[["Method ID", "Positive (+) Mode Standards", "Negative (–) Mode Standards"]]
    else:
        df_methods = df_methods[["Method ID", "Positive (+) Mode Features", "Negative (–) Mode Features"]]

    return df_methods


def add_msp_to_database(msp_file, chromatography, polarity, is_bio_standard=False):

    """
    Inserts location of pos/neg MSP files into "chromatography_methods" table, and
    parses compounds from MSP into "internal_standards" or "targeted_features" table
    """

    # Connect to database
    db_metadata, connection = connect_to_database()

    # Write MSP file to folder, store file path in database (further down in function)
    methods_directory = os.path.join(os.getcwd(), "methods")
    if not os.path.exists(methods_directory):
        os.makedirs(methods_directory)

    if polarity == "Positive Mode":
        msp_file_path = os.path.join(methods_directory, chromatography + "_Pos.msp")
    elif polarity == "Negative Mode":
        msp_file_path = os.path.join(methods_directory, chromatography + "_Neg.msp")

    with open(msp_file_path, "w") as file:
        msp_file.seek(0)
        shutil.copyfileobj(msp_file, file)

    # Read MSP file
    with open(msp_file_path, "r") as msp:

        list_of_features = []

        # Split MSP into list of compounds
        data = msp.read().split("\n\n")
        data = [element.split("\n") for element in data]

        # Add each line of each compound into a list
        for feature in data:
            if len(feature) != 1:
                list_of_features.append(feature)

        features_dict = {}

        # Iterate through features in MSP
        for feature_index, feature in enumerate(list_of_features):

            features_dict[feature_index] = {}

            # Iterate through each line of each feature in the MSP
            for data_index, feature_data in enumerate(feature):

                # Capture, name, inchikey, m/z, and RT
                if "Name" in feature_data:
                    features_dict[feature_index]["Name"] = feature_data.replace("Name: ", "")
                    continue
                elif "Precursormz" in feature_data:
                    features_dict[feature_index]["Precursor m/z"] = feature_data.replace("Precursormz: ", "")
                    continue
                elif "InChIKey" in feature_data:
                    features_dict[feature_index]["INCHIKEY"] = feature_data.replace("InChIKey: ", "")
                    continue
                elif "RETENTIONTIME" in feature_data:
                    features_dict[feature_index]["Retention time"] = feature_data.replace("RETENTIONTIME: ", "")
                    continue

                # Capture MS2 spectrum
                elif "Num Peaks" in feature_data:

                    # Get number of peaks in MS2 spectrum
                    num_peaks = int(feature_data.replace("Num Peaks: ", ""))

                    # Each line in the MSP corresponds to a peak
                    start_index = data_index + 1
                    end_index = data_index + num_peaks + 1

                    # Each peak is represented as a string e.g. "56.04977\t247187"
                    peaks_in_spectrum = []
                    for peak in feature[start_index:end_index]:
                        peaks_in_spectrum.append(peak.replace("\t", ":"))

                    features_dict[feature_index]["MS2 spectrum"] = str(peaks_in_spectrum)
                    break

    # Get internal_standards table
    internal_standards_table = sa.Table("internal_standards", db_metadata, autoload=True)

    # Prepare DELETE of old internal standards
    delete_old_internal_standards = (
        sa.delete(internal_standards_table)
            .where((internal_standards_table.c.chromatography == chromatography)
                   & (internal_standards_table.c.polarity == polarity))
    )

    # Execute DELETE
    connection.execute(delete_old_internal_standards)

    # Execute INSERT of each internal standard into internal_standards table
    for feature in features_dict:
        insert_feature = internal_standards_table.insert().values(
            {"name": features_dict[feature]["Name"],
             "chromatography": chromatography,
             "polarity": polarity,
             "precursor_mz": features_dict[feature]["Precursor m/z"],
             "retention_time": features_dict[feature]["Retention time"],
             "ms2_spectrum": features_dict[feature]["MS2 spectrum"],
             "inchikey": features_dict[feature]["INCHIKEY"]})
        connection.execute(insert_feature)

    # Get "chromatography" table
    chromatography_table = sa.Table("chromatography_methods", db_metadata, autoload=True)

    # Write location of msp file to respective cell
    if polarity == "Positive Mode":
        update_msp_file = (
            sa.update(chromatography_table)
                .where(chromatography_table.c.method_id == chromatography)
                .values(num_pos_standards=len(features_dict),
                        pos_istd_msp_file=msp_file_path)
        )
    elif polarity == "Negative Mode":
        update_msp_file = (
            sa.update(chromatography_table)
                .where(chromatography_table.c.method_id == chromatography)
                .values(num_neg_standards=len(features_dict),
                        neg_istd_msp_file=msp_file_path)
        )

    # Execute UPDATE of MSP file location
    connection.execute(update_msp_file)

    # Close the connection
    connection.close()


def get_msdial_configurations():

    """
    Returns list of user configurations of MS-DIAL parameters
    """

    engine = sa.create_engine(sqlite_db_location)
    df_msdial_configurations = pd.read_sql("SELECT * FROM msdial_parameters", engine)
    return df_msdial_configurations["config_name"].astype(str).tolist()


def generate_msdial_parameters_file(config_name, chromatography, polarity, msp_file_path):

    """
    Uses parameters from user-curated MS-DIAL configuration to create a
    parameters.txt file for MS-DIAL console app usage
    """

    # Get parameters of selected configuration
    parameters = db.get_msdial_configuration_parameters(config_name)

    if polarity == "Positive":
        adduct_type = "[M+H]+"
    elif polarity == "Negative":
        adduct_type = "[M-H]-"

    if msp_file_path.endswith(".msp"):
        filepath = "MSP file: " + msp_filepath
    elif msp_file_path.endswith(".txt"):
        filepath = "Text file: " + txt_filepath

    # Text file contents
    lines = [
        "#Data type",
        "MS1 data type: Centroid",
        "MS2 data type: Centroid",
        "Ion mode: " + polarity,
        "DIA file:", "\n"

        "#Data collection parameters",
        "Retention time begin: " + str(parameters[0]),
        "Retention time end: " + str(parameters[1]),
        "Mass range begin: " + str(parameters[2]),
        "Mass range end: " + str(parameters[3]), "\n",

        "#Centroid parameters",
        "MS1 tolerance for centroid: " + str(parameters[4]),
        "MS2 tolerance for centroid: " + str(parameters[5]), "\n",

        "#Peak detection parameters",
        "Smoothing method: " + str(parameters[6]),
        "Smoothing level: " + str(parameters[7]),
        "Minimum peak width: " + str(parameters[8]),
        "Minimum peak height: " + str(parameters[9]),
        "Mass slice width: " + str(parameters[10]), "\n",

        "#Deconvolution parameters",
        "Sigma window value: 0.5",
        "Amplitude cut off: 0", "\n",

        "#Adduct list",
        "Adduct list: " + adduct_type, "\n",

        "#Text file and post identification (retention time and accurate mass based) setting",
        filepath,
        "Retention time tolerance for post identification: " + str(parameters[11]),
        "Accurate ms1 tolerance for post identification: " + str(parameters[12]),
        "Post identification score cut off: " + str(parameters[13]), "\n",

        "#Alignment parameters setting",
        "Retention time tolerance for alignment: " + str(parameters[14]),
        "MS1 tolerance for alignment: " + str(parameters[15]),
        "Retention time factor for alignment: " + str(parameters[16]),
        "MS1 factor for alignment: " + str(parameters[17]),
        "Peak count filter: " + str(parameters[18]),
        "QC at least filter: " + str(parameters[19]),
    ]

    # Write parameters to a text file
    methods_directory = os.path.join(os.getcwd(), "methods")
    if not os.path.exists(methods_directory):
        os.makedirs(methods_directory)

    if polarity == "Positive":
        parameters_file = os.path.join(methods_directory, config_name.replace(" ", "_") + "Parameters_Pos.txt")
    elif polarity == "Negative":
        parameters_file = os.path.join(methods_directory, config_name.replace(" ", "_") + "Parameters_Neg.txt")

    with open(parameters_file, "w") as file:
        for line in lines:
            file.write(line)
            if line != "\n":
                file.write("\n")

    # Write path of parameters text file to chromatography method in database
    db_metadata, connection = connect_to_database()
    chromatography_table = sa.Table("chromatography_methods", db_metadata, autoload=True)

    if polarity == "Positive":
        update_parameter_file = (
            sa.update(chromatography_table)
                .where(chromatography_table.c.method_id == chromatography)
                .values(pos_parameter_file=parameters_file)
        )
    elif polarity == "Negative":
        update_parameter_file = (
            sa.update(chromatography_table)
                .where(chromatography_table.c.method_id == chromatography)
                .values(neg_parameter_file=parameters_file)
        )

    connection.execute(update_parameter_file)
    connection.close()


def add_msdial_configuration(msdial_config_name, msdial_directory):

    """
    Inserts new user configuration of MS-DIAL parameters into the "msdial_parameters" table
    """

    # Connect to database
    db_metadata, connection = connect_to_database()

    # Get MS-DIAL parameters table
    msdial_parameters_table = sa.Table("msdial_parameters", db_metadata, autoload=True)

    # Prepare insert of user-inputted run data
    insert_config = msdial_parameters_table.insert().values(
        {"config_name": msdial_config_name,
         "rt_begin": 0,
         "rt_end": 100,
         "mz_begin": 0,
         "mz_end": 2000,
         "ms1_centroid_tolerance": 0.008,
         "ms2_centroid_tolerance": 0.01,
         "smoothing_method": "LinearWeightedMovingAverage",
         "smoothing_level": 3,
         "min_peak_width": 3,
         "min_peak_height": 35000,
         "mass_slice_width": 0.1,
         "post_id_rt_tolerance": 0.3,
         "post_id_mz_tolerance": 0.008,
         "post_id_score_cutoff": 85,
         "alignment_rt_tolerance": 0.05,
         "alignment_mz_tolerance": 0.008,
         "alignment_rt_factor": 0.5,
         "alignment_mz_factor": 0.5,
         "peak_count_filter": 0,
         "qc_at_least_filter": "True",
         "msdial_directory": msdial_directory}
    )

    # Execute INSERT to database, then close the connection
    connection.execute(insert_config)
    connection.close()


def remove_msdial_configuration(msdial_config_name):

    """
    Deletes user configuration of MS-DIAL parameters from the "msdial_parameters" table
    """

    # Connect to database
    db_metadata, connection = connect_to_database()

    # Get MS-DIAL parameters table
    msdial_parameters_table = sa.Table("msdial_parameters", db_metadata, autoload=True)

    # Prepare DELETE of MS-DIAL configuration
    delete_config = (
        sa.delete(msdial_parameters_table)
            .where(msdial_parameters_table.c.config_name == msdial_config_name)
    )

    # Execute DELETE, then close the connection
    connection.execute(delete_config)
    connection.close()


def get_msdial_configuration_parameters(msdial_config_name):

    """
    Returns a tuple of parameters defined for a selected MS-DIAL configuration
    """

    # Get "msdial_parameters" table from database as a DataFrame
    engine = sa.create_engine(sqlite_db_location)
    df_configurations = pd.read_sql("SELECT * FROM msdial_parameters", engine)

    # Get selected configuration
    selected_config = df_configurations.loc[
        df_configurations["config_name"] == msdial_config_name]

    selected_config.drop(["id", "config_name"], inplace=True, axis=1)

    # Get parameters of selected configuration as a tuple
    return tuple(selected_config.to_records(index=False)[0])


def update_msdial_configuration(config_name, rt_begin, rt_end, mz_begin, mz_end, ms1_centroid_tolerance,
    ms2_centroid_tolerance, smoothing_method, smoothing_level, mass_slice_width, min_peak_width, min_peak_height,
    post_id_rt_tolerance, post_id_mz_tolerance, post_id_score_cutoff, alignment_rt_tolerance, alignment_mz_tolerance,
    alignment_rt_factor, alignment_mz_factor, peak_count_filter, qc_at_least_filter, msdial_directory):

    """
    Updates parameters of a selected MS-DIAL configuration
    """

    # Connect to database
    db_metadata, connection = connect_to_database()

    # Get MS-DIAL parameters table
    msdial_parameters_table = sa.Table("msdial_parameters", db_metadata, autoload=True)

    # Prepare insert of user-inputted MS-DIAL parameters
    update_parameters = (
        sa.update(msdial_parameters_table)
            .where(msdial_parameters_table.c.config_name == config_name)
            .values(rt_begin=rt_begin,
                    rt_end=rt_end,
                    mz_begin=mz_begin,
                    mz_end=mz_end,
                    ms1_centroid_tolerance=ms1_centroid_tolerance,
                    ms2_centroid_tolerance=ms2_centroid_tolerance,
                    smoothing_method=smoothing_method,
                    smoothing_level=smoothing_level,
                    min_peak_width=min_peak_width,
                    min_peak_height=min_peak_height,
                    mass_slice_width=mass_slice_width,
                    post_id_rt_tolerance=post_id_rt_tolerance,
                    post_id_mz_tolerance=post_id_mz_tolerance,
                    post_id_score_cutoff=post_id_score_cutoff,
                    alignment_rt_tolerance=alignment_rt_tolerance,
                    alignment_mz_tolerance=alignment_mz_tolerance,
                    alignment_rt_factor=alignment_rt_factor,
                    alignment_mz_factor=alignment_mz_factor,
                    peak_count_filter=peak_count_filter,
                    qc_at_least_filter=qc_at_least_filter,
                    msdial_directory=msdial_directory)
    )

    update_msdial_location = (
        sa.update(msdial_parameters_table)
            .where(msdial_parameters_table.c.config_name == "Default configuration")
            .values(msdial_directory=msdial_directory)
    )

    # Execute UPDATE to database, then close the connection
    connection.execute(update_parameters)
    connection.execute(update_msdial_location)
    connection.close()


def get_msp_file_paths(chromatography, polarity, type):

    """
    Returns file paths of positive and negative MSPs (both stored in the methods folder
    upon user upload) MS-DIAL for parameter file generation – from the "chromatography_methods" table
    """

    # Connect to database and get selected chromatography method
    engine = sa.create_engine(sqlite_db_location)
    query = "SELECT * FROM chromatography_methods WHERE method_id='" + chromatography + "'"
    df_methods = pd.read_sql(query, engine)

    # Return file path of MSP in chromatography, based on polarity/type requested
    if polarity == "Positive":
        msp_file_path = df_methods["pos_" + type + "_msp_file"].astype(str).values[0]
    elif polarity == "Negative":
        msp_file_path = df_methods["neg_" + type + "_msp_file"].astype(str).values[0]

    return msp_file_path


def get_parameter_file_path(chromatography, polarity):

    """
    Returns file path of parameters file stored in "chromatography_methods" table
    """

    engine = sa.create_engine(sqlite_db_location)
    query = "SELECT * FROM chromatography_methods WHERE method_id='" + chromatography + "'"
    df_methods = pd.read_sql(query, engine)

    if polarity == "Positive":
        parameter_file = df_methods["pos_parameter_file"].astype(str).values[0]
    elif polarity == "Negative":
        parameter_file = df_methods["neg_parameter_file"].astype(str).values[0]

    return parameter_file


def get_msdial_directory(config_id):

    """
    Returns location of MS-DIAL "installation" folder
    """

    return get_msdial_configuration_parameters(config_id)[-1]


def get_internal_standards(chromatography, polarity):

    """
    Returns list of internal standards for a given chromatography method and polarity
    """

    engine = sa.create_engine(sqlite_db_location)
    query = "SELECT * FROM internal_standards " + \
            "WHERE chromatography='" + chromatography + "' AND polarity='" + polarity + "'"
    df_internal_standards = pd.read_sql(query, engine)
    return df_internal_standards["name"].astype(str).tolist()