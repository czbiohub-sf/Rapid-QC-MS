import os, io, shutil
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import INTEGER, REAL, TEXT

# Location of SQLite database
sqlite_db_location = "sqlite:///QC Database.db"

def is_valid():

    """
    Confirms that all required tables are present
    """

    if len(sa.create_engine(sqlite_db_location).table_names()) > 0:
        return True
    else:
        return False


def create_database():

    """
    Initializes a new, empty SQLite database
    """

    # Initialize SQLAlchemy
    engine = sa.create_engine(sqlite_db_location)
    db_metadata = sa.MetaData()

    # Create tables
    bio_qc_results = sa.Table(
        "bio_qc_results", db_metadata,
        sa.Column("id", INTEGER, primary_key=True),
        sa.Column("sample_id", TEXT),
        sa.Column("run_id", TEXT),
        sa.Column("precursor_mz", TEXT),
        sa.Column("retention_time", TEXT),
        sa.Column("intensity", TEXT),
        sa.Column("md5", TEXT),
        sa.Column("qc_result", TEXT),
        sa.Column("biological_standard", TEXT),
        sa.Column("position", TEXT)
    )

    biological_standards = sa.Table(
        "biological_standards", db_metadata,
        sa.Column("id", INTEGER, primary_key=True),
        sa.Column("name", TEXT),
        sa.Column("identifier", TEXT),
        sa.Column("chromatography", TEXT),
        sa.Column("num_pos_features", INTEGER),
        sa.Column("num_neg_features", INTEGER),
        sa.Column("pos_bio_msp_file", TEXT),
        sa.Column("neg_bio_msp_file", TEXT),
        sa.Column("pos_parameter_file", TEXT),
        sa.Column("neg_parameter_file", TEXT),
        sa.Column("msdial_config_id", TEXT)
    )

    chromatography_methods = sa.Table(
        "chromatography_methods", db_metadata,
        sa.Column("id", INTEGER, primary_key=True),
        sa.Column("method_id", TEXT),
        sa.Column("num_pos_standards", INTEGER),
        sa.Column("num_neg_standards", INTEGER),
        sa.Column("pos_istd_msp_file", TEXT),
        sa.Column("neg_istd_msp_file", TEXT),
        sa.Column("pos_parameter_file", TEXT),
        sa.Column("neg_parameter_file", TEXT),
        sa.Column("msdial_config_id", TEXT)
    )

    gdrive_users = sa.Table(
        "gdrive_users", db_metadata,
        sa.Column("id", INTEGER, primary_key=True),
        sa.Column("email", TEXT)
    )

    instruments = sa.Table(
        "instruments", db_metadata,
        sa.Column("id", INTEGER, primary_key=True),
        sa.Column("name", TEXT),
        sa.Column("vendor", TEXT),
        sa.Column("gdrive_folder_id", TEXT),
        sa.Column("gdrive_file_id", TEXT)
    )

    internal_standards = sa.Table(
        "internal_standards", db_metadata,
        sa.Column("id", INTEGER, primary_key=True),
        sa.Column("name", TEXT),
        sa.Column("chromatography", TEXT),
        sa.Column("polarity", TEXT),
        sa.Column("precursor_mz", REAL),
        sa.Column("retention_time", REAL),
        sa.Column("ms2_spectrum", TEXT),
        sa.Column("inchikey", TEXT)
    )

    msdial_parameters = sa.Table(
        "msdial_parameters", db_metadata,
        sa.Column("id", INTEGER, primary_key=True),
        sa.Column("config_name", TEXT),
        sa.Column("rt_begin", INTEGER),
        sa.Column("rt_end", INTEGER),
        sa.Column("mz_begin", INTEGER),
        sa.Column("mz_end", INTEGER),
        sa.Column("ms1_centroid_tolerance", REAL),
        sa.Column("ms2_centroid_tolerance", REAL),
        sa.Column("smoothing_method", TEXT),
        sa.Column("smoothing_level", INTEGER),
        sa.Column("min_peak_width", INTEGER),
        sa.Column("min_peak_height", INTEGER),
        sa.Column("mass_slice_width", REAL),
        sa.Column("post_id_rt_tolerance", REAL),
        sa.Column("post_id_mz_tolerance", REAL),
        sa.Column("post_id_score_cutoff", REAL),
        sa.Column("alignment_rt_tolerance", REAL),
        sa.Column("alignment_mz_tolerance", REAL),
        sa.Column("alignment_rt_factor", REAL),
        sa.Column("alignment_mz_factor", REAL),
        sa.Column("peak_count_filter", INTEGER),
        sa.Column("qc_at_least_filter", TEXT),
        sa.Column("msdial_directory", TEXT)
    )

    qc_parameters = sa.Table(
        "qc_parameters", db_metadata,
        sa.Column("id", INTEGER, primary_key=True),
        sa.Column("config_name", TEXT),
        sa.Column("intensity_dropouts_cutoff", INTEGER),
        sa.Column("max_rt_shift", REAL),
        sa.Column("allowed_delta_rt_trends", INTEGER)
    )

    runs = sa.Table(
        "runs", db_metadata,
        sa.Column("id", INTEGER, primary_key=True),
        sa.Column("run_id", TEXT),
        sa.Column("instrument_id", TEXT),
        sa.Column("chromatography", TEXT),
        sa.Column("sequence", TEXT),
        sa.Column("metadata", TEXT),
        sa.Column("status", TEXT),
        sa.Column("samples", INTEGER),
        sa.Column("completed", INTEGER),
        sa.Column("passes", INTEGER),
        sa.Column("fails", INTEGER),
        sa.Column("latest_sample", TEXT),
        sa.Column("qc_config_id", TEXT),
        sa.Column("biological_standards", TEXT),
    )

    sample_qc_results = sa.Table(
        "sample_qc_results", db_metadata,
        sa.Column("id", INTEGER, primary_key=True),
        sa.Column("sample_id", TEXT),
        sa.Column("run_id", TEXT),
        sa.Column("precursor_mz", TEXT),
        sa.Column("retention_time", TEXT),
        sa.Column("intensity", TEXT),
        sa.Column("md5", TEXT),
        sa.Column("qc_result", TEXT),
        sa.Column("position", TEXT)
    )

    targeted_features = sa.Table(
        "targeted_features", db_metadata,
        sa.Column("id", INTEGER, primary_key=True),
        sa.Column("name", TEXT),
        sa.Column("chromatography", TEXT),
        sa.Column("polarity", TEXT),
        sa.Column("biological_standard", TEXT),
        sa.Column("precursor_mz", REAL),
        sa.Column("retention_time", REAL),
        sa.Column("ms2_spectrum", TEXT),
        sa.Column("inchikey", TEXT)
    )

    # Insert tables into database
    db_metadata.create_all(engine)

    # Insert default configurations for MS-DIAL and MS-AutoQC
    add_msdial_configuration("Default", "")
    add_qc_configuration("Default")

    return sqlite_db_location


def connect_to_database():

    """
    Connects to local SQLite database
    """

    engine = sa.create_engine(sqlite_db_location)
    db_metadata = sa.MetaData(bind=engine)
    connection = engine.connect()

    return db_metadata, connection


def get_table(table_name):

    """
    Returns table from database as a DataFrame
    """

    engine = sa.create_engine(sqlite_db_location)
    return pd.read_sql("SELECT * FROM " + table_name, engine)


def insert_new_instrument(name, vendor, gdrive_file_id=None, gdrive_folder_id=None):

    """
    Inserts a new instrument into the "instruments" table
    """

    # Connect to database
    db_metadata, connection = connect_to_database()

    # Get "instruments" table
    instruments_table = sa.Table("instruments", db_metadata, autoload=True)

    # Prepare insert of new instrument
    insert_instrument = instruments_table.insert().values(
        {"name": name,
         "vendor": vendor,
         "gdrive_folder_id": gdrive_folder_id,
         "gdrive_file_id": gdrive_file_id}
    )

    # Execute the insert, then close the connection
    connection.execute(insert_instrument)
    connection.close()


def get_instruments_list():

    """
    Returns list of instruments in database
    """

    # Connect to SQLite database
    engine = sa.create_engine(sqlite_db_location)

    # Get instruments table as DataFrame
    df_instruments = pd.read_sql("SELECT * FROM instruments", engine)

    # Return list of instruments
    return df_instruments["name"].astype(str).tolist()


def insert_new_run(run_id, instrument_id, chromatography, bio_standards, sequence, metadata, qc_config_id):

    """
    1. Inserts a new instrument run into the "runs" table
    2. Inserts sample rows into the "sample_qc_results" table
    3. Inserts biological standard sample rows into the "bio_qc_results" table
    """

    # Get list of samples from sequence
    df_sequence = pd.read_json(sequence, orient="split")
    samples = df_sequence["File Name"].astype(str).tolist()
    positions = df_sequence["Position"].astype(str).tolist()

    for index, sample in enumerate(samples.copy()):
        if "_BK_" and "_pre_" in sample:
            samples.remove(sample)
            positions.remove(positions[index])
        elif "wash" in sample:
            samples.remove(sample)
            positions.remove(positions[index])
        elif "shutdown" in sample:
            samples.remove(sample)
            positions.remove(positions[index])

    num_samples = len(samples)

    # Connect to database
    db_metadata, connection = connect_to_database()

    # Get relevant tables
    runs_table = sa.Table("runs", db_metadata, autoload=True)
    sample_qc_results_table = sa.Table("sample_qc_results", db_metadata, autoload=True)
    bio_qc_results_table = sa.Table("bio_qc_results", db_metadata, autoload=True)

    # Get identifiers for biological standard (if any)
    identifiers = get_biological_standard_identifiers(bio_standards)

    # Prepare insert of user-inputted run data
    insert_run = runs_table.insert().values(
        {"run_id": run_id,
         "instrument_id": instrument_id,
         "chromatography": chromatography,
         "sequence": sequence,
         "metadata": metadata,
         "status": "Active",
         "samples": num_samples,
         "completed": 0,
         "passes": 0,
         "fails": 0,
         "latest_sample": "",
         "qc_config_id": qc_config_id,
         "biological_standards": str(bio_standards)})

    insert_samples = []

    for index, sample in enumerate(samples):
        # Check if the biological standard identifier is in the sample name
        is_bio_standard = False

        for identifier in identifiers.keys():
            if identifier in sample:
                is_bio_standard = True
                break

        # Prepare insert of the sample row into the "sample_qc_results" table
        if not is_bio_standard:
            insert_sample = sample_qc_results_table.insert().values(
                {"sample_id": sample,
                 "run_id": run_id,
                 "position": positions[index]})

        # Prepare insert of the sample row into the "bio_qc_results" table
        else:
            insert_sample = bio_qc_results_table.insert().values(
                {"sample_id": sample,
                 "run_id": run_id,
                 "biological_standard": identifiers[identifier],
                 "position": positions[index]})

        # Add this INSERT query into the list of insert queries
        insert_samples.append(insert_sample)

    # Execute INSERT to database
    connection.execute(insert_run)

    for insert_sample in insert_samples:
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


def get_instrument_runs(instrument_id):

    """
    Returns DataFrame of all runs on a given instrument from "runs" table
    """

    engine = sa.create_engine(sqlite_db_location)
    query = "SELECT * FROM runs WHERE instrument_id = '" + instrument_id + "'"
    df_instrument_runs = pd.read_sql(query, engine)
    return df_instrument_runs


def get_md5(sample_id):

    """
    Returns MD5 checksum for a data file in "sample_qc_results" table
    """

    # Connect to database
    engine = sa.create_engine(sqlite_db_location)

    # Check if sample is a biological standard
    table = "sample_qc_results"

    for identifier in get_biological_standard_identifiers().keys():
        if identifier in sample_id:
            table = "bio_qc_results"
            break

    # Get sample from correct table
    df_sample_qc_results = pd.read_sql(
        "SELECT * FROM " + table + " WHERE sample_id = '" + sample_id + "'", engine)

    return df_sample_qc_results["md5"].astype(str).values[0]


def update_md5_checksum(sample_id, md5_checksum):

    """
    Updates MD5 checksum for a data file in "sample_qc_results" table
    """

    # Connect to database
    db_metadata, connection = connect_to_database()

    # Check if sample is a biological standard and get relevant table
    qc_results_table = sa.Table("sample_qc_results", db_metadata, autoload=True)

    for identifier in get_biological_standard_identifiers().keys():
        if identifier in sample_id:
            qc_results_table = sa.Table("bio_qc_results", db_metadata, autoload=True)
            break

    # Prepare update of MD5 checksum at sample row
    update_md5 = (
        sa.update(qc_results_table)
            .where(qc_results_table.c.sample_id == sample_id)
            .values(md5=md5_checksum)
    )

    # Execute UPDATE into database, then close the connection
    connection.execute(update_md5)
    connection.close()


def write_qc_results(sample_id, run_id, json_mz, json_rt, json_intensity, qc_result, is_bio_standard):

    """
    Updates m/z, RT, and intensity info (as JSON strings) in appropriate table upon MS-DIAL processing completion
    """

    # Connect to database
    db_metadata, connection = connect_to_database()

    # Get "sample_qc_results" or "bio_qc_results" table
    if not is_bio_standard:
        qc_results_table = sa.Table("sample_qc_results", db_metadata, autoload=True)
    else:
        qc_results_table = sa.Table("bio_qc_results", db_metadata, autoload=True)

    # Prepare update (insert) of QC results to correct sample row
    update_qc_results = (
        sa.update(qc_results_table)
            .where((qc_results_table.c.sample_id == sample_id)
                   & (qc_results_table.c.run_id == run_id))
            .values(precursor_mz=json_mz,
                    retention_time=json_rt,
                    intensity=json_intensity,
                    qc_result=qc_result)
    )

    # Execute UPDATE into database, then close the connection
    connection.execute(update_qc_results)
    connection.close()


def get_chromatography_methods():

    """
    Returns DataFrame of chromatography methods
    """

    engine = sa.create_engine(sqlite_db_location)
    df_methods = pd.read_sql("SELECT * FROM chromatography_methods", engine)
    return df_methods


def get_chromatography_methods_list():

    """
    Returns list of chromatography method ID's
    """

    df_methods = get_chromatography_methods()
    return df_methods["method_id"].astype(str).tolist()


def insert_chromatography_method(method_id):

    """
    Inserts new chromatography method in "chromatography_methods" table
    """

    # Connect to database
    db_metadata, connection = connect_to_database()

    # Get "chromatography_methods" table and "biological_standards" table
    chromatography_table = sa.Table("chromatography_methods", db_metadata, autoload=True)
    biological_standards_table = sa.Table("biological_standards", db_metadata, autoload=True)

    # Execute insert of chromatography method
    insert_method = chromatography_table.insert().values(
        {"method_id": method_id,
         "num_pos_standards": 0,
         "num_neg_standards": 0,
         "pos_istd_msp_file": "",
         "neg_istd_msp_file": "",
         "pos_parameter_file": "",
         "neg_parameter_file": ""})

    connection.execute(insert_method)

    # Execute insert of method for each biological standard
    df_biological_standards = get_biological_standards()
    biological_standards = df_biological_standards["name"].astype(str).unique().tolist()
    identifiers = df_biological_standards["identifier"].astype(str).tolist()

    for index, biological_standard in enumerate(biological_standards):
        insert_method_for_bio_standard = biological_standards_table.insert().values({
            "name": biological_standard,
            "identifier": identifiers[index],
            "chromatography": method_id,
            "num_pos_features": 0,
            "num_neg_features": 0})
        connection.execute(insert_method_for_bio_standard)

    # Execute INSERT to database, then close the connection
    connection.close()


def remove_chromatography_method(method_id):

    """
    1. Removes chromatography method in "chromatography_methods" table
    2. Removes method from "biological_standards" table
    3. Removes associated internal standards from "internal_standards" table
    4. Removes associated targeted features from "targeted_features" table
    5. Deletes corresponding MSPs from folders
    """

    # Connect to database and get relevant tables
    db_metadata, connection = connect_to_database()
    chromatography_table = sa.Table("chromatography_methods", db_metadata, autoload=True)
    biological_standards_table = sa.Table("biological_standards", db_metadata, autoload=True)
    internal_standards_table = sa.Table("internal_standards", db_metadata, autoload=True)
    targeted_features_table = sa.Table("targeted_features", db_metadata, autoload=True)

    delete_queries = []

    # Remove from "chromatography_methods" table
    delete_chromatography_method = (
        sa.delete(chromatography_table)
            .where((chromatography_table.c.method_id == method_id))
    )

    delete_queries.append(delete_chromatography_method)

    # Remove all entries in other tables associated with chromatography
    for table in [biological_standards_table, internal_standards_table, targeted_features_table]:
        delete_from_table = (
            sa.delete(table)
                .where((table.c.chromatography == method_id))
        )
        delete_queries.append(delete_from_table)

    # Execute all deletes, then close the connection
    for query in delete_queries:
        connection.execute(query)

    connection.close()


def update_msdial_config_for_internal_standards(chromatography, config_id):

    """
    Updates MS-DIAL configuration for a given chromatography method
    """

    # Connect to database and get relevant tables
    db_metadata, connection = connect_to_database()
    methods_table = sa.Table("chromatography_methods", db_metadata, autoload=True)

    # Update MS-DIAL configuration for chromatography method
    update_msdial_config = (
        sa.update(methods_table)
            .where(methods_table.c.method_id == chromatography)
            .values(msdial_config_id=config_id)
    )

    connection.execute(update_msdial_config)
    connection.close()


def add_msp_to_database(msp_file, chromatography, polarity, bio_standard=None):

    """
    Parses compounds from MSP into "internal_standards" or "targeted_features" table,
    and inserts location of pos/neg MSP files into "chromatography_methods" table
    """

    # Connect to database
    db_metadata, connection = connect_to_database()

    # Write MSP file to folder, store file path in database (further down in function)
    methods_directory = os.path.join(os.getcwd(), "methods")
    if not os.path.exists(methods_directory):
        os.makedirs(methods_directory)

    if bio_standard is not None:
        if polarity == "Positive Mode":
            msp_file_path = os.path.join(methods_directory, bio_standard.replace(" ", "_")
                                         + "_" + chromatography + "_Pos.msp")
        elif polarity == "Negative Mode":
            msp_file_path = os.path.join(methods_directory, bio_standard.replace(" ", "_")
                                         + "_" + chromatography + "_Neg.msp")
    else:
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

            features_dict[feature_index] = {
                "Name": None,
                "Precursor m/z": None,
                "Retention time": None,
                "INCHIKEY": None,
                "MS2 spectrum": None
            }

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

    # Adding MSP for biological standards
    if bio_standard is not None:

        # Get "targeted_features" table
        targeted_features_table = sa.Table("targeted_features", db_metadata, autoload=True)

        # Prepare DELETE of old targeted features
        delete_old_targeted_features = (
            sa.delete(targeted_features_table)
                .where((targeted_features_table.c.chromatography == chromatography)
                       & (targeted_features_table.c.polarity == polarity)
                       & (targeted_features_table.c.biological_standard == bio_standard))
        )

        # Execute DELETE
        connection.execute(delete_old_targeted_features)

        # Execute INSERT of each targeted feature into targeted_features table
        for feature in features_dict:
            insert_feature = targeted_features_table.insert().values(
                {"name": features_dict[feature]["Name"],
                 "chromatography": chromatography,
                 "polarity": polarity,
                 "biological_standard": bio_standard,
                 "precursor_mz": features_dict[feature]["Precursor m/z"],
                 "retention_time": features_dict[feature]["Retention time"],
                 "ms2_spectrum": features_dict[feature]["MS2 spectrum"],
                 "inchikey": features_dict[feature]["INCHIKEY"]})
            connection.execute(insert_feature)

        # Get "biological_standards" table
        biological_standards_table = sa.Table("biological_standards", db_metadata, autoload=True)

        # Write location of msp file to respective cell
        if polarity == "Positive Mode":
            update_msp_file = (
                sa.update(biological_standards_table)
                    .where((biological_standards_table.c.chromatography == chromatography)
                           & (biological_standards_table.c.name == bio_standard))
                    .values(num_pos_features=len(features_dict),
                            pos_bio_msp_file=msp_file_path)
            )
        elif polarity == "Negative Mode":
            update_msp_file = (
                sa.update(biological_standards_table)
                    .where((biological_standards_table.c.chromatography == chromatography)
                           & (biological_standards_table.c.name == bio_standard))
                    .values(num_neg_features=len(features_dict),
                            neg_bio_msp_file=msp_file_path)
            )

        # Execute UPDATE of MSP file location
        connection.execute(update_msp_file)

    # Adding MSP for internal standards
    else:

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


def add_csv_to_database(csv_file, chromatography, polarity):

    """
    Parses compounds from a CSV into the "internal_standards" table, and stores
    the location of the pos/neg TXT files in "chromatography_methods" table
    """

    # Convert CSV file into Python dictionary
    df_internal_standards = pd.read_csv(csv_file, index_col=False)
    internal_standards_dict = df_internal_standards.to_dict("index")

    # Create methods directory if it doesn't already exist
    methods_directory = os.path.join(os.getcwd(), "methods")
    if not os.path.exists(methods_directory):
        os.makedirs(methods_directory)

    # Name file accordingly
    if polarity == "Positive Mode":
        txt_file_path = os.path.join(methods_directory, chromatography + "_Pos.txt")
    elif polarity == "Negative Mode":
        txt_file_path = os.path.join(methods_directory, chromatography + "_Neg.txt")

    # Write CSV columns to tab-delimited text file
    df_internal_standards.to_csv(txt_file_path, sep="\t", index=False)

    # Connect to database
    db_metadata, connection = connect_to_database()

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
    for row in internal_standards_dict.keys():
        insert_standard = internal_standards_table.insert().values(
            {"name": internal_standards_dict[row]["Common Name"],
             "chromatography": chromatography,
             "polarity": polarity,
             "precursor_mz": internal_standards_dict[row]["MS1 m/z"],
             "retention_time": internal_standards_dict[row]["RT (min)"]})
        connection.execute(insert_standard)

    # Get "chromatography" table
    chromatography_table = sa.Table("chromatography_methods", db_metadata, autoload=True)

    # Write location of CSV file to respective cell
    if polarity == "Positive Mode":
        update_msp_file = (
            sa.update(chromatography_table)
                .where(chromatography_table.c.method_id == chromatography)
                .values(num_pos_standards=len(internal_standards_dict),
                        pos_istd_msp_file=txt_file_path)
        )
    elif polarity == "Negative Mode":
        update_msp_file = (
            sa.update(chromatography_table)
                .where(chromatography_table.c.method_id == chromatography)
                .values(num_neg_standards=len(internal_standards_dict),
                        neg_istd_msp_file=txt_file_path)
        )

    # Execute UPDATE of CSV file location
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


def generate_msdial_parameters_file(chromatography, polarity, msp_file_path, bio_standard=None):

    """
    Uses parameters from user-curated MS-DIAL configuration to create a parameters.txt file for command-line MS-DIAL
    """

    # Get parameters of selected configuration
    if bio_standard is not None:
        df_bio_standards = get_biological_standards()
        df_bio_standards = df_bio_standards.loc[
            (df_bio_standards["chromatography"] == chromatography) & (df_bio_standards["name"] == bio_standard)]
        config_name = df_bio_standards["msdial_config_id"].astype(str).values[0]
    else:
        df_methods = get_chromatography_methods()
        df_methods = df_methods.loc[df_methods["method_id"] == chromatography]
        config_name = df_methods["msdial_config_id"].astype(str).values[0]

    parameters = get_msdial_configuration_parameters(config_name)

    # Create "methods" directory if it does not exist
    methods_directory = os.path.join(os.getcwd(), "methods")
    if not os.path.exists(methods_directory):
        os.makedirs(methods_directory)

    # Name parameters file accordingly
    if bio_standard is not None:
        if polarity == "Positive":
            filename = bio_standard.replace(" ", "_") + "_" + config_name.replace(" ", "_") + "_Parameters_Pos.txt"
        elif polarity == "Negative":
            filename = bio_standard.replace(" ", "_") + "_" + config_name.replace(" ", "_") + "_Parameters_Neg.txt"
    else:
        if polarity == "Positive":
            filename = chromatography.replace(" ", "_") + "_" + config_name.replace(" ", "_") + "_Parameters_Pos.txt"
        elif polarity == "Negative":
            filename = chromatography.replace(" ", "_") + "_" + config_name.replace(" ", "_") + "_Parameters_Neg.txt"

    parameters_file = os.path.join(methods_directory, filename)

    # Some specifications based on polarity / file type for the parameters
    if polarity == "Positive":
        adduct_type = "[M+H]+"
    elif polarity == "Negative":
        adduct_type = "[M-H]-"

    if msp_file_path.endswith(".msp"):
        filepath = "MSP file: " + msp_file_path
    elif msp_file_path.endswith(".txt"):
        filepath = "Text file: " + msp_file_path

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
    with open(parameters_file, "w") as file:
        for line in lines:
            file.write(line)
            if line != "\n":
                file.write("\n")

    # Write path of parameters text file to chromatography method in database
    db_metadata, connection = connect_to_database()
    chromatography_table = sa.Table("chromatography_methods", db_metadata, autoload=True)
    biological_standards_table = sa.Table("biological_standards", db_metadata, autoload=True)

    # For processing biological standard samples
    if bio_standard is not None:
        if polarity == "Positive":
            update_parameter_file = (
                sa.update(biological_standards_table)
                    .where((biological_standards_table.c.chromatography == chromatography)
                           & (biological_standards_table.c.name == bio_standard))
                    .values(pos_parameter_file=parameters_file)
            )
        elif polarity == "Negative":
            update_parameter_file = (
                sa.update(biological_standards_table)
                    .where((biological_standards_table.c.chromatography == chromatography)
                           & (biological_standards_table.c.name == bio_standard))
                    .values(neg_parameter_file=parameters_file)
            )
    # For processing samples with internal standards
    else:
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
            .where(msdial_parameters_table.c.config_name == "Default")
            .values(msdial_directory=msdial_directory)
    )

    # Execute UPDATE to database, then close the connection
    connection.execute(update_parameters)
    connection.execute(update_msdial_location)
    connection.close()


def get_msp_file_paths(chromatography, polarity, bio_standard=None):
    """
    Returns file paths of MSPs for a selected chromatography / polarity (both stored
    in the methods folder upon user upload) for MS-DIAL parameter file generation
    """

    # Connect to database
    engine = sa.create_engine(sqlite_db_location)

    if bio_standard is not None:
        # Get selected biological standard
        query = "SELECT * FROM biological_standards WHERE name = '" + bio_standard + "' AND chromatography='" + chromatography + "'"
        df_biological_standards = pd.read_sql(query, engine)

        # Get file path of MSP in requested polarity
        if polarity == "Positive":
            msp_file_path = df_biological_standards["pos_bio_msp_file"].astype(str).values[0]
        elif polarity == "Negative":
            msp_file_path = df_biological_standards["neg_bio_msp_file"].astype(str).values[0]

    else:
        # Get selected chromatography method
        query = "SELECT * FROM chromatography_methods WHERE method_id='" + chromatography + "'"
        df_methods = pd.read_sql(query, engine)

        # Get file path of MSP in requested polarity
        if polarity == "Positive":
            msp_file_path = df_methods["pos_istd_msp_file"].astype(str).values[0]
        elif polarity == "Negative":
            msp_file_path = df_methods["neg_istd_msp_file"].astype(str).values[0]

    # Return file path
    return msp_file_path


def get_parameter_file_path(chromatography, polarity, biological_standard=None):

    """
    Returns file path of parameters file stored in database
    """

    engine = sa.create_engine(sqlite_db_location)

    if biological_standard is not None:
        query = "SELECT * FROM biological_standards WHERE chromatography='" + chromatography + \
                "' AND name ='" + biological_standard + "'"
    else:
        query = "SELECT * FROM chromatography_methods WHERE method_id='" + chromatography + "'"

    df = pd.read_sql(query, engine)

    if polarity == "Positive":
        parameter_file = df["pos_parameter_file"].astype(str).values[0]
    elif polarity == "Negative":
        parameter_file = df["neg_parameter_file"].astype(str).values[0]

    return parameter_file


def get_msdial_directory(config_id):

    """
    Returns location of MS-DIAL "installation" folder
    """

    return get_msdial_configuration_parameters(config_id)[-1]


def get_internal_standards_dict(chromatography, value_type):

    """
    Returns dictionary of internal standard keys mapped to either m/z or RT values
    """

    engine = sa.create_engine(sqlite_db_location)
    query = "SELECT * FROM internal_standards " + "WHERE chromatography='" + chromatography + "'"
    df_internal_standards = pd.read_sql(query, engine)

    dict = {}
    keys = df_internal_standards["name"].astype(str).tolist()
    values = df_internal_standards[value_type].astype(float).tolist()

    for index, key in enumerate(keys):
        dict[key] = values[index]

    return dict


def get_internal_standards_list(chromatography, polarity):

    """
    Returns list of internal standards for a given chromatography method and polarity
    """

    engine = sa.create_engine(sqlite_db_location)

    query = "SELECT * FROM internal_standards " + \
            "WHERE chromatography='" + chromatography + "' AND polarity='" + polarity + "'"

    df_internal_standards = pd.read_sql(query, engine)
    return df_internal_standards["name"].astype(str).tolist()


def get_targeted_features_list(biological_standard, chromatography, polarity):

    """
    Returns list of metabolite targets for a given biological standard, chromatography, and polarity
    """

    engine = sa.create_engine(sqlite_db_location)

    query = "SELECT * FROM targeted_features " + \
            "WHERE chromatography='" + chromatography + \
            "' AND polarity='" + polarity + \
            "' AND biological_standard ='" + biological_standard + "'"

    df_targeted_features = pd.read_sql(query, engine)
    return df_targeted_features["name"].astype(str).tolist()


def get_biological_standards():

    """
    Returns a tailored DataFrame of the "biological_standards" table
    """

    # Get table from database as a DataFrame
    engine = sa.create_engine(sqlite_db_location)
    df_biological_standards = pd.read_sql("SELECT * FROM biological_standards", engine)
    return df_biological_standards


def get_biological_standards_list():

    """
    Returns a list of biological standards in the database
    """

    df_biological_standards = get_biological_standards()
    return df_biological_standards["name"].astype(str).unique().tolist()


def add_biological_standard(name, identifier):

    """
    Inserts new biological standard into "biological_standards" table
    """

    # Get list of chromatography methods
    chromatography_methods = get_chromatography_methods()["method_id"].tolist()

    # Connect to database and get "biological_standards" table
    db_metadata, connection = connect_to_database()
    biological_standards_table = sa.Table("biological_standards", db_metadata, autoload=True)

    # Insert a biological standard row for each chromatography
    for method in chromatography_methods:
        insert = biological_standards_table.insert().values({
            "name": name,
            "identifier": identifier,
            "chromatography": method,
            "num_pos_features": 0,
            "num_neg_features": 0,
            "msdial_config_id": "Default"
        })
        connection.execute(insert)

    # Close the connection
    connection.close()


def remove_biological_standard(name):

    """
    Deletes biological standard (and corresponding MSPs) from database
    """

    # Connect to database and get relevant tables
    db_metadata, connection = connect_to_database()
    biological_standards_table = sa.Table("biological_standards", db_metadata, autoload=True)
    targeted_features_table = sa.Table("targeted_features", db_metadata, autoload=True)

    # Remove biological standard
    delete_biological_standard = (
        sa.delete(biological_standards_table)
            .where((biological_standards_table.c.name == name))
    )
    connection.execute(delete_biological_standard)

    # Remove targeted features for that biological standard
    delete_targeted_features = (
        sa.delete(targeted_features_table)
            .where((targeted_features_table.c.biological_standard == name))
    )
    connection.execute(delete_targeted_features)

    # Close the connection
    connection.close()


def update_msdial_config_for_bio_standard(biological_standard, chromatography, config_id):

    """
    Updates MS-DIAL configuration for a given biological standard and chromatography method
    """

    # Connect to database and get relevant tables
    db_metadata, connection = connect_to_database()
    biological_standards_table = sa.Table("biological_standards", db_metadata, autoload=True)

    # Update MS-DIAL configuration for biological standard
    update_msdial_config = (
        sa.update(biological_standards_table)
            .where((biological_standards_table.c.name == biological_standard)
                   & (biological_standards_table.c.chromatography == chromatography))
            .values(msdial_config_id=config_id)
    )

    connection.execute(update_msdial_config)
    connection.close()


def get_biological_standard_identifiers(bio_standards=None):

    """
    Returns dictionary of identifiers for a given list of biological standards
    """

    df_bio_standards = get_biological_standards()

    identifiers = {}

    if bio_standards is not None:
        if len(bio_standards) > 0:
            for bio_standard in bio_standards:
                df = df_bio_standards.loc[df_bio_standards["name"] == bio_standard]
                identifier = df["identifier"].astype(str).unique().tolist()[0]
                identifiers[identifier] = bio_standard
    else:
        names = df_bio_standards["name"].astype(str).unique().tolist()
        ids = df_bio_standards["identifier"].astype(str).unique().tolist()
        for index, name in enumerate(names):
            identifiers[ids[index]] = names[index]

    return identifiers


def get_qc_configurations():

    """
    Returns a DataFrame of QC parameter configurations
    """

    engine = sa.create_engine(sqlite_db_location)
    return pd.read_sql("SELECT * FROM qc_parameters", engine)


def get_qc_configurations_list():

    """
    Returns a list of QC parameter configurations
    """

    return get_qc_configurations()["config_name"].astype(str).tolist()


def add_qc_configuration(qc_config_name):

    """
    Adds a new QC configuration to the "qc_parameters" table
    """

    # Connect to database
    db_metadata, connection = connect_to_database()

    # Get QC parameters table
    qc_parameters_table = sa.Table("qc_parameters", db_metadata, autoload=True)

    # Prepare insert of user-inputted run data
    insert_config = qc_parameters_table.insert().values(
        {"config_name": qc_config_name,
         "intensity_dropouts_cutoff": 4,
         "max_rt_shift": 0.1,
         "allowed_delta_rt_trends": 3}
    )

    # Execute INSERT to database, then close the connection
    connection.execute(insert_config)
    connection.close()


def remove_qc_configuration(qc_config_name):

    """
    Deletes QC configuration from the "qc_parameters" table
    """

    # Connect to database
    db_metadata, connection = connect_to_database()

    # Get QC parameters table
    qc_parameters_table = sa.Table("qc_parameters", db_metadata, autoload=True)

    # Prepare DELETE of MS-DIAL configuration
    delete_config = (
        sa.delete(qc_parameters_table)
            .where(qc_parameters_table.c.config_name == qc_config_name)
    )

    # Execute DELETE, then close the connection
    connection.execute(delete_config)
    connection.close()


def get_qc_configuration_parameters(config_name):

    """
    Returns tuple of parameters for a selected QC configuration
    """

    # Get "qc_parameters" table from database as a DataFrame
    df_configurations = get_table("qc_parameters")

    # Get selected configuration
    selected_config = df_configurations.loc[
        df_configurations["config_name"] == config_name]

    selected_config.drop(["id", "config_name"], inplace=True, axis=1)

    # Return parameters of selected configuration as a tuple
    return tuple(selected_config.to_records(index=False)[0])


def update_qc_configuration(config_name, intensity_dropouts_cutoff, max_rt_shift, allowed_delta_rt_trends):

    """
    Updates parameters for a given QC configuration
    """

    # Connect to database
    db_metadata, connection = connect_to_database()

    # Get QC parameters table
    qc_parameters_table = sa.Table("qc_parameters", db_metadata, autoload=True)

    # Prepare insert of user-inputted QC parameters
    update_parameters = (
        sa.update(qc_parameters_table)
            .where(qc_parameters_table.c.config_name == config_name)
            .values(intensity_dropouts_cutoff=intensity_dropouts_cutoff,
                    max_rt_shift=max_rt_shift,
                    allowed_delta_rt_trends=allowed_delta_rt_trends)
    )

    # Execute UPDATE to database, then close the connection
    connection.execute(update_parameters)
    connection.close()


def get_samples_in_run(run_id, sample_type="Both"):

    """
    Returns DataFrame of samples in a given run
    """

    if sample_type == "Sample":
        df = get_table("sample_qc_results")

    elif sample_type == "Biological Standard":
        df = get_table("bio_qc_results")

    elif sample_type == "Both":
        df_samples = get_table("sample_qc_results")
        df_bio_standards = get_table("bio_qc_results")
        df_bio_standards.drop(columns=["biological_standard"], inplace=True)
        df = df_bio_standards.append(df_samples, ignore_index=True)

    return df.loc[df["run_id"] == run_id]


def parse_internal_standard_data(run_id, result_type, polarity):

    """
    Returns JSON-ified DataFrame of samples (as columns) vs. internal standards (as rows)
    """

    # Get relevant QC results table from database
    df_samples = get_samples_in_run(run_id, "Sample")

    # Filter by polarity
    df_samples = df_samples.loc[df_samples["sample_id"].str.contains(polarity)]

    # Get list of results using result type
    sample_ids = df_samples["sample_id"].astype(str).tolist()
    results = df_samples[result_type].tolist()

    # Prepare unified results DataFrame
    df_results = pd.DataFrame()

    # For each JSON-ified result,
    for index, result in enumerate(results):
        # Convert to DataFrame
        df = pd.read_json(result, orient="split")

        # Refactor so that each row is a sample, and each column is an internal standard
        df.rename(columns={df.columns[1]: sample_ids[index]}, inplace=True)
        df = df.transpose()
        df.columns = df.iloc[0]
        df = df.drop(df.index[0])
        df.reset_index(inplace=True)
        df.rename(columns={"index": "Sample"}, inplace=True)

        # Append result to df_results
        df_results = pd.concat([df_results, df], ignore_index=True)

    # Return DataFrame as JSON string
    return df_results.to_json(orient="split")


def parse_biological_standard_data(result_type, polarity, biological_standard):

    """
    Returns JSON-ified DataFrame of instrument runs (as columns) vs. targeted features (as rows)
    """

    # Get relevant QC results table from database
    df_samples = get_table("bio_qc_results")

    # Filter by biological standard type
    df_samples = df_samples.loc[df_samples["biological_standard"] == biological_standard]

    # Filter by polarity
    df_samples = df_samples.loc[df_samples["sample_id"].str.contains(polarity)]

    # Get list of results using result type
    run_ids = df_samples["run_id"].astype(str).tolist()
    results = df_samples[result_type].tolist()

    # Prepare unified results DataFrame
    df_results = pd.DataFrame()

    # For each JSON-ified result,
    for index, result in enumerate(results):
        # Convert to DataFrame
        df = pd.read_json(result, orient="split")

        # Refactor so that each row is a sample, and each column is an internal standard
        df.rename(columns={df.columns[1]: run_ids[index]}, inplace=True)

        # Append result to df_results
        df_results = pd.concat([df_results, df], ignore_index=True)

    # Return DataFrame as JSON string
    return df_results.to_json(orient="split")