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
         "pos_istd_msp_file": "",
         "neg_istd_msp_file": "",
         "pos_istd_csv_file": "",
         "neg_istd_csv_file": "",
         "pos_bio_msp_file": "",
         "neg_bio_msp_file": "",
         "num_pos_standards": 0,
         "num_neg_standards": 0})

    # Execute INSERT to database, then close the connection
    connection.execute(insert_method)
    connection.close()


def get_chromatography_methods():

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
        "num_neg_standards": "Negative (–) Mode Standards"})

    df_methods.drop(["id", "num_pos_features", "num_neg_features"], inplace=True, axis=1)

    return df_methods


def add_msp_to_database(msp_file, chromatography, polarity, is_bio_standard=False):

    """
    Inserts location of pos/neg MSP files into "chromatography_methods" table, and
    parses compounds from MSP into "internal_standards" or "targeted_features" table
    """

    # Connect to database
    db_metadata, connection = connect_to_database()

    # Read MSP file
    with msp_file as msp:

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
                .values(num_pos_standards=len(features_dict))
        )

    elif polarity == "Negative Mode":
        update_msp_file = (
            sa.update(chromatography_table)
                .where(chromatography_table.c.method_id == chromatography)
                .values(num_neg_standards=len(features_dict))
        )

    # Execute UPDATE of MSP file location
    connection.execute(update_msp_file)

    # Close the connection
    connection.close()