import traceback
import warnings
warnings.simplefilter(action="ignore", category=FutureWarning)

import os, io, shutil, time
import hashlib, json, ast
import pandas as pd
import numpy as np
import sqlalchemy as sa
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from sqlalchemy import INTEGER, REAL, TEXT

# Initialize directories
root_directory = os.getcwd()
data_directory = os.path.join(root_directory, "data")
methods_directory = os.path.join(data_directory, "methods")
auth_directory = os.path.join(root_directory, "auth")

# Location of settings SQLite database
settings_database = "sqlite:///data/methods/Settings.db"
settings_db_file = os.path.join(methods_directory, "Settings.db")

# Google Drive authentication files
credentials_file = os.path.join(auth_directory, "credentials.txt")
drive_settings_file = os.path.join(auth_directory, "settings.yaml")
auth_container = [GoogleAuth(settings_file=drive_settings_file)]

"""
The functions defined below operate on two database types:

- One storing instrument run metadata, sample QC results, and biological standard QC results
- The other storing instrument metadata, workspace settings for workspace access, chromatography methods, 
biological standards, QC configurations, and MS-DIAL configurations
  
In addition, this file also contains methods for syncing data and settings with Google Drive.
To get an overview of all functions, please visit the documentation on https://czbiohub.github.io/MS-AutoQC.
"""

def get_database_file(instrument_id, sqlite_conn=False, zip=False):

    """
    Returns database file for a given instrument ID
    """

    if zip:
        filename = instrument_id.replace(" ", "_") + ".zip"
    else:
        filename = instrument_id.replace(" ", "_") + ".db"

    if sqlite_conn:
        return "sqlite:///data/" + filename
    else:
        return os.path.join(data_directory, filename)


def connect_to_database(name):

    """
    Connects to SQLite database of choice
    """

    if name == "Settings":
        database_file = settings_database
    else:
        database_file = get_database_file(instrument_id=name, sqlite_conn=True)

    engine = sa.create_engine(database_file)
    db_metadata = sa.MetaData(bind=engine)
    connection = engine.connect()

    return db_metadata, connection


def create_databases(instrument_id, new_instrument_only=False):

    """
    Initializes SQLite databases for 1) instrument data and 2) workspace settings
    """

    # Create tables for instrument database
    instrument_database = get_database_file(instrument_id=instrument_id, sqlite_conn=True)
    qc_db_engine = sa.create_engine(instrument_database)
    qc_db_metadata = sa.MetaData()

    bio_qc_results = sa.Table(
        "bio_qc_results", qc_db_metadata,
        sa.Column("id", INTEGER, primary_key=True),
        sa.Column("sample_id", TEXT),
        sa.Column("run_id", TEXT),
        sa.Column("precursor_mz", TEXT),
        sa.Column("retention_time", TEXT),
        sa.Column("intensity", TEXT),
        sa.Column("md5", TEXT),
        sa.Column("qc_dataframe", TEXT),
        sa.Column("qc_result", TEXT),
        sa.Column("biological_standard", TEXT),
        sa.Column("position", TEXT)
    )

    runs = sa.Table(
        "runs", qc_db_metadata,
        sa.Column("id", INTEGER, primary_key=True),
        sa.Column("run_id", TEXT),
        sa.Column("chromatography", TEXT),
        sa.Column("acquisition_path", TEXT),
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
        sa.Column("pid", INTEGER),
        sa.Column("drive_id", TEXT),
        sa.Column("sample_status", TEXT),
        sa.Column("job_type", TEXT)
    )

    sample_qc_results = sa.Table(
        "sample_qc_results", qc_db_metadata,
        sa.Column("id", INTEGER, primary_key=True),
        sa.Column("sample_id", TEXT),
        sa.Column("run_id", TEXT),
        sa.Column("position", TEXT),
        sa.Column("md5", TEXT),
        sa.Column("precursor_mz", TEXT),
        sa.Column("retention_time", TEXT),
        sa.Column("intensity", TEXT),
        sa.Column("qc_dataframe", TEXT),
        sa.Column("qc_result", TEXT)
    )

    # If only creating instrument database, save and return here
    if new_instrument_only:
        qc_db_metadata.create_all(qc_db_engine)
        set_device_identity(is_instrument_computer=True, instrument_id=instrument_id)
        return None

    # Create tables for Settings.db
    settings_db_engine = sa.create_engine(settings_database)
    settings_db_metadata = sa.MetaData()

    instruments = sa.Table(
        "instruments", settings_db_metadata,
        sa.Column("id", INTEGER, primary_key=True),
        sa.Column("name", TEXT),
        sa.Column("vendor", TEXT),
        sa.Column("drive_id", TEXT),
        sa.Column("last_modified", TEXT)
    )

    biological_standards = sa.Table(
        "biological_standards", settings_db_metadata,
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
        "chromatography_methods", settings_db_metadata,
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
        "gdrive_users", settings_db_metadata,
        sa.Column("id", INTEGER, primary_key=True),
        sa.Column("name", TEXT),
        sa.Column("email_address", TEXT),
        sa.Column("permission_id", TEXT),
    )

    internal_standards = sa.Table(
        "internal_standards", settings_db_metadata,
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
        "msdial_parameters", settings_db_metadata,
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
        sa.Column("qc_at_least_filter", TEXT)
    )

    email_notifications = sa.Table(
        "email_notifications", settings_db_metadata,
        sa.Column("id", INTEGER, primary_key=True),
        sa.Column("email_address", TEXT),
    )

    qc_parameters = sa.Table(
        "qc_parameters", settings_db_metadata,
        sa.Column("id", INTEGER, primary_key=True),
        sa.Column("config_name", TEXT),
        sa.Column("intensity_dropouts_cutoff", INTEGER),
        sa.Column("library_rt_shift_cutoff", REAL),
        sa.Column("in_run_rt_shift_cutoff", REAL),
        sa.Column("library_mz_shift_cutoff", REAL),
        sa.Column("intensity_enabled", INTEGER),
        sa.Column("library_rt_enabled", INTEGER),
        sa.Column("in_run_rt_enabled", INTEGER),
        sa.Column("library_mz_enabled", INTEGER)
    )

    targeted_features = sa.Table(
        "targeted_features", settings_db_metadata,
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

    workspace = sa.Table(
        "workspace", settings_db_metadata,
        sa.Column("id", INTEGER, primary_key=True),
        sa.Column("slack_bot_token", TEXT),
        sa.Column("slack_channel", TEXT),
        sa.Column("slack_enabled", INTEGER),
        sa.Column("gdrive_folder_id", TEXT),
        sa.Column("methods_zip_file_id", TEXT),
        sa.Column("methods_last_modified", TEXT),
        sa.Column("msdial_directory", TEXT),
        sa.Column("is_instrument_computer", INTEGER),
        sa.Column("instrument_identity", TEXT)
    )

    # Insert tables into database
    settings_db_metadata.create_all(settings_db_engine)

    # Insert default configurations for MS-DIAL and MS-AutoQC
    add_msdial_configuration("Default")
    add_qc_configuration("Default")

    # Initialize workspace metadata
    create_workspace_metadata()

    # Save device identity based on setup values
    set_device_identity(is_instrument_computer, instrument_id)
    return None


def execute_vacuum(database):

    """
    Executes VACUUM command on given database
    """

    db_metadata, connection = connect_to_database(database)
    connection.execute("VACUUM")
    connection.close()


def get_drive_instance():

    """
    Returns user-authenticated Google Drive instance
    """

    return GoogleDrive(auth_container[0])


def launch_google_drive_authentication():

    """
    Launches Google Drive authentication flow and sets authentication instance
    """

    auth_container[0] = GoogleAuth(settings_file=drive_settings_file)
    auth_container[0].LocalWebserverAuth()


def save_google_drive_credentials():

    """
    Saves Google credentials to a credentials.txt file
    """

    auth_container[0].SaveCredentialsFile(credentials_file)


def initialize_google_drive():

    """
    Initializes instance of Google Drive using credentials.txt and settings.yaml in /auth directory
    """

    # Create Google Drive instance
    auth_container[0] = GoogleAuth(settings_file=drive_settings_file)
    gauth = auth_container[0]

    # If no credentials file, make user authenticate
    if not os.path.exists(credentials_file) and is_valid():
        gauth.LocalWebserverAuth()

    # Try to load saved client credentials
    gauth.LoadCredentialsFile(credentials_file)

    # Initialize saved credentials
    if gauth.credentials is not None:

        # Refresh credentials if expired
        if gauth.access_token_expired:
            gauth.Refresh()

        # Otherwise, authorize saved credentials
        else:
            gauth.Authorize()

    # If no saved credentials, make user authenticate again
    elif gauth.credentials is None:
        gauth.LocalWebserverAuth()

    if not os.path.exists(credentials_file) and is_valid():
        gauth.SaveCredentialsFile(credentials_file)

    return os.path.exists(credentials_file)


def is_valid(instrument_id=None):

    """
    Checks that all required tables are present
    """

    # Validate settings database
    settings_db_required_tables = ["biological_standards", "chromatography_methods", "email_notifications", "instruments",
        "gdrive_users", "internal_standards", "msdial_parameters", "qc_parameters", "targeted_features", "workspace"]

    try:
        settings_db_tables = sa.create_engine(settings_database).table_names()
        if len(settings_db_tables) < len(settings_db_required_tables):
            return False
    except:
        return False

    # Validate instrument databases
    instrument_db_required_tables = ["bio_qc_results", "runs", "sample_qc_results"]

    # If given an instrument ID, only validate that instrument's database
    try:
        if instrument_id is not None:
            database = get_database_file(instrument_id, sqlite_conn=True)
            instrument_db_tables = sa.create_engine(database).table_names()
            if len(instrument_db_tables) < len(instrument_db_required_tables):
                return False

        # Otherwise, validate all instrument databases
        else:
            database_files = [file.replace(".db", "") for file in os.listdir(data_directory) if ".db" in file]
            databases = [get_database_file(f, sqlite_conn=True) for f in database_files]

            for database in databases:
                instrument_db_tables = sa.create_engine(database).table_names()
                if len(instrument_db_tables) < len(instrument_db_required_tables):
                    return False
    except:
        return False

    return True


def sync_is_enabled():

    """
    Checks whether Google Drive sync is enabled
    """

    if not is_valid():
        return False

    df_workspace = get_table("Settings", "workspace")
    gdrive_folder_id = df_workspace["gdrive_folder_id"].values[0]
    methods_zip_file_id = df_workspace["methods_zip_file_id"].values[0]

    if gdrive_folder_id is not None and methods_zip_file_id is not None:
        if gdrive_folder_id != "None" and methods_zip_file_id != "None":
            if gdrive_folder_id != "" and methods_zip_file_id != "":
                return True

    return False


def email_notifications_are_enabled():

    """
    Checks whether email notifications are enabled
    """

    if not is_valid():
        return False

    if len(get_table("Settings", "email_notifications")) > 0:
        return True

    return False


def slack_notifications_are_enabled():

    """
    Checks whether Slack notifications are enabled
    """

    if not is_valid():
        return False

    try:
        return bool(get_table("Settings", "workspace")["slack_enabled"].astype(int).tolist()[0])
    except:
        return False


def is_instrument_computer():

    """
    Checks whether user's device is the instrument computer
    """

    return bool(get_table("Settings", "workspace")["is_instrument_computer"].astype(int).tolist()[0])


def get_md5_for_settings_db():

    """
    Returns MD5 checksum for the settings database file
    """

    hash_md5 = hashlib.md5()

    with open(settings_db_file, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()


def settings_were_modified(md5_checksum):

    """
    Checks whether settings database file has been modified
    """

    if md5_checksum != get_md5_for_settings_db():
        return True
    else:
        return False


def zip_database(instrument_id=None, filename=None):

    """
    Compresses instrument database file into a ZIP archive
    """

    if instrument_id is None and filename is None:
        return None

    if filename is not None:
        db_zip_file = os.path.join(data_directory, filename)
        filename = filename.replace(".zip", ".db")

    elif instrument_id is not None:
        db_zip_file = get_database_file(instrument_id, zip=True)
        filename = instrument_id.replace(" ", "_") + ".db"

    file_without_extension = db_zip_file.replace(".zip", "")
    shutil.make_archive(file_without_extension, "zip", data_directory, filename)


def unzip_database(instrument_id=None, filename=None):

    """
    Unzips instrument database file
    """

    if instrument_id is None and filename is None:
        return None

    if instrument_id is not None:
        db_zip_file = get_database_file(instrument_id, zip=True)
    elif filename is not None:
        db_zip_file = os.path.join(data_directory, filename)

    shutil.unpack_archive(db_zip_file, data_directory, "zip")
    os.remove(db_zip_file)


def zip_methods():

    """
    Compresses methods directory into a ZIP archive
    """

    output_directory_and_name = os.path.join(data_directory, "methods.zip").replace(".zip", "")
    shutil.make_archive(output_directory_and_name, "zip", methods_directory)
    return output_directory_and_name + ".zip"


def unzip_methods():

    """
    Unzips methods directory
    """

    input_zip = os.path.join(data_directory, "methods.zip")
    shutil.unpack_archive(input_zip, methods_directory, "zip")
    os.remove(input_zip)


def zip_csv_files(input_directory, output_directory_and_name):

    """
    Compresses CSV files into a ZIP archive
    """

    shutil.make_archive(output_directory_and_name, "zip", input_directory)
    return output_directory_and_name + ".zip"


def unzip_csv_files(input_zip, output_directory):

    """
    Unzips archive of CSV files
    """

    shutil.unpack_archive(input_zip, output_directory, "zip")
    os.remove(input_zip)


def get_table(database_name, table_name):

    """
    Returns table from database as a DataFrame
    """

    if database_name == "Settings":
        database = settings_database
    else:
        database = get_database_file(database_name, sqlite_conn=True)

    engine = sa.create_engine(database)
    return pd.read_sql("SELECT * FROM " + table_name, engine)


def generate_client_settings_yaml(client_id, client_secret):

    """
    Generates a settings.yaml file for Google authentication in /auth directory
    """

    auth_directory = os.path.join(os.getcwd(), "auth")
    if not os.path.exists(auth_directory):
        os.makedirs(auth_directory)

    settings_yaml_file = os.path.join(auth_directory, "settings.yaml")

    lines = [
        "client_config_backend: settings",
        "client_config:",
        "  client_id: " + client_id,
        "  client_secret: " + client_secret,
        "\n",
        "save_credentials: True",
        "save_credentials_backend: file",
        "save_credentials_file: auth/credentials.txt",
        "\n",
        "get_refresh_token: True",
    ]

    with open(settings_yaml_file, "w") as file:
        for line in lines:
            file.write(line)
            if line != "\n" and line != lines[-1]:
                file.write("\n")


def insert_google_drive_ids(instrument_id, gdrive_folder_id, instrument_db_file_id, methods_zip_file_id):

    """
    Inserts Google Drive ID's into corresponding tables for the following:
    1. MS-AutoQC folder (gdrive_folder_id)
    2. Instrument database zip file (instrument_db_file_id)
    3. Methods directory zip file (methods_zip_file_id)
    """

    db_metadata, connection = connect_to_database("Settings")
    instruments_table = sa.Table("instruments", db_metadata, autoload=True)
    workspace_table = sa.Table("workspace", db_metadata, autoload=True)

    # Instruments database
    connection.execute((
        sa.update(instruments_table)
            .where((instruments_table.c.name == instrument_id))
            .values(drive_id=instrument_db_file_id)
    ))

    # MS-AutoQC folder and Methods folder
    connection.execute((
        sa.update(workspace_table)
            .where((workspace_table.c.id == 1))
            .values(gdrive_folder_id=gdrive_folder_id,
                    methods_zip_file_id=methods_zip_file_id)
    ))

    connection.close()


def insert_new_instrument(name, vendor):

    """
    Inserts a new instrument into the "instruments" table
    """

    # Connect to database
    db_metadata, connection = connect_to_database("Settings")

    # Get "instruments" table
    instruments_table = sa.Table("instruments", db_metadata, autoload=True)

    # Prepare insert of new instrument
    insert_instrument = instruments_table.insert().values(
        {"name": name,
         "vendor": vendor}
    )

    # Execute the insert, then close the connection
    connection.execute(insert_instrument)
    connection.close()


def get_instruments_list():

    """
    Returns list of instruments in database
    """

    # Connect to SQLite database
    engine = sa.create_engine(settings_database)

    # Get instruments table as DataFrame
    df_instruments = pd.read_sql("SELECT * FROM instruments", engine)

    # Return list of instruments
    return df_instruments["name"].astype(str).tolist()


def get_instrument(instrument_id):

    """
    Returns record from "instruments" table as a DataFrame for a given instrument
    """

    engine = sa.create_engine(settings_database)
    return pd.read_sql("SELECT * FROM instruments WHERE name = '" + instrument_id + "'", engine)


def get_filenames_from_sequence(sequence):

    """
    Takes sequence file as JSON string and returns filtered DataFrame
    """

    df_sequence = pd.read_json(sequence, orient="split")

    # Filter out preblanks
    df_sequence = df_sequence.loc[
        ~((df_sequence["File Name"].str.contains(r"_BK_", na=False)) &
          (df_sequence["File Name"].str.contains(r"_pre_", na=False)))]

    # Filter out wash and shutdown
    df_sequence = df_sequence.loc[
        ~(df_sequence["File Name"].str.contains(r"_wash_", na=False)) &
        ~(df_sequence["File Name"].str.contains(r"shutdown", na=False))]

    return df_sequence


def insert_new_run(run_id, instrument_id, chromatography, bio_standards, path, sequence, metadata, qc_config_id, job_type):

    """
    1. Inserts a new instrument run into the "runs" table
    2. Inserts sample rows into the "sample_qc_results" table
    3. Inserts biological standard sample rows into the "bio_qc_results" table
    """

    # Get list of samples from sequence
    df_sequence = get_filenames_from_sequence(sequence)

    samples = df_sequence["File Name"].astype(str).tolist()
    positions = df_sequence["Position"].astype(str).tolist()

    num_samples = len(samples)

    # Connect to database
    db_metadata, connection = connect_to_database(instrument_id)

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
         "acquisition_path": path,
         "sequence": sequence,
         "metadata": metadata,
         "status": "Active",
         "samples": num_samples,
         "completed": 0,
         "passes": 0,
         "fails": 0,
         "qc_config_id": qc_config_id,
         "biological_standards": str(bio_standards),
         "job_type": job_type})

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
                 "instrument_id": instrument_id,
                 "run_id": run_id,
                 "position": positions[index]})

        # Prepare insert of the sample row into the "bio_qc_results" table
        else:
            insert_sample = bio_qc_results_table.insert().values(
                {"sample_id": sample,
                 "instrument_id": instrument_id,
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


def get_instrument_run(instrument_id, run_id):

    """
    Returns DataFrame of selected instrument run from "runs" table
    """

    database = get_database_file(instrument_id=instrument_id, sqlite_conn=True)
    engine = sa.create_engine(database)
    query = "SELECT * FROM runs WHERE run_id = '" + run_id + "'"
    df_instrument_run = pd.read_sql(query, engine)
    return df_instrument_run


def get_instrument_run_from_csv(instrument_id, run_id):

    """
    Returns DataFrame of selected instrument run from CSV files during active runs
    """

    id = instrument_id.replace(" ", "_") + "_" + run_id
    run_csv_file = os.path.join(data_directory, id, "csv", "run.csv")
    return pd.read_csv(run_csv_file, index_col=False)


def get_instrument_runs(instrument_id):

    """
    Returns DataFrame of all runs on a given instrument from "runs" table
    """

    database = get_database_file(instrument_id, sqlite_conn=True)
    engine = sa.create_engine(database)
    return pd.read_sql("SELECT * FROM runs", engine)


def delete_instrument_run(instrument_id, run_id):

    """
    Deletes instrument run from all tables in the database
    """

    # Connect to database
    db_metadata, connection = connect_to_database(instrument_id)

    # Get relevant tables
    runs_table = sa.Table("runs", db_metadata, autoload=True)
    sample_qc_results_table = sa.Table("sample_qc_results", db_metadata, autoload=True)
    bio_qc_results_table = sa.Table("bio_qc_results", db_metadata, autoload=True)

    # Delete from each table
    for table in [runs_table, sample_qc_results_table, bio_qc_results_table]:
        connection.execute((
            sa.delete(table).where(runs_table.c.run_id == run_id)
        ))

    # Close the connection
    connection.close()


def get_acquisition_path(instrument_id, run_id):

    """
    Returns acquisition path for a given instrument run
    """

    return get_instrument_run(instrument_id, run_id)["acquisition_path"].astype(str).tolist()[0]


def get_md5(instrument_id, sample_id):

    """
    Returns MD5 checksum for a data file in "sample_qc_results" table
    """

    # Connect to database
    database = get_database_file(name=instrument_id, sqlite_conn=True)
    engine = sa.create_engine(database)

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


def update_md5_checksum(instrument_id, sample_id, md5_checksum):

    """
    Updates MD5 checksum for a data file in "sample_qc_results" table
    """

    # Connect to database
    db_metadata, connection = connect_to_database(instrument_id)

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


def write_qc_results(sample_id, instrument_id, run_id, json_mz, json_rt, json_intensity, qc_dataframe, qc_result, is_bio_standard):

    """
    Updates m/z, RT, and intensity info (as dictionary records) in appropriate table upon MS-DIAL processing completion
    """

    # Connect to database
    db_metadata, connection = connect_to_database(instrument_id)

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
                    qc_dataframe=qc_dataframe,
                    qc_result=qc_result)
    )

    # Execute UPDATE into database, then close the connection
    connection.execute(update_qc_results)
    connection.close()


def get_chromatography_methods():

    """
    Returns DataFrame of chromatography methods
    """

    engine = sa.create_engine(settings_database)
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
    db_metadata, connection = connect_to_database("Settings")

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
         "neg_parameter_file": "",
         "msdial_config_id": "Default"})

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
            "num_neg_features": 0,
            "msdial_config_id": "Default"})
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
    6. Deletes corresponding MSPs from Google Drive (if sync is enabled)
    """

    # Delete corresponding MSPs from "methods" directory
    df = get_table("Settings", "chromatography_methods")
    df = df.loc[df["method_id"] == method_id]

    df2 = get_table("Settings", "biological_standards")
    df2 = df2.loc[df2["chromatography"] == method_id]

    files_to_delete = df["pos_istd_msp_file"].astype(str).tolist() + df["neg_istd_msp_file"].astype(str).tolist() + \
        df2["pos_bio_msp_file"].astype(str).tolist() + df2["neg_bio_msp_file"].astype(str).tolist()

    for file in os.listdir(methods_directory):
        if file in files_to_delete:
            os.remove(os.path.join(methods_directory, file))

    # Connect to database and get relevant tables
    db_metadata, connection = connect_to_database("Settings")
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
    db_metadata, connection = connect_to_database("Settings")
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
    db_metadata, connection = connect_to_database("Settings")

    # Write MSP file to folder, store file path in database (further down in function)
    if not os.path.exists(methods_directory):
        os.makedirs(methods_directory)

    if bio_standard is not None:
        if polarity == "Positive Mode":
            filename = bio_standard.replace(" ", "_") + "_" + chromatography + "_Pos.msp"
        elif polarity == "Negative Mode":
            filename = bio_standard.replace(" ", "_") + "_" + chromatography + "_Neg.msp"
    else:
        if polarity == "Positive Mode":
            filename = chromatography + "_Pos.msp"
        elif polarity == "Negative Mode":
            filename = chromatography + "_Neg.msp"

    msp_file_path = os.path.join(methods_directory, filename)

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
        added_features = []

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
                if "NAME" in feature_data.upper():
                    feature_name = feature_data.split(": ")[-1]
                    if feature_name not in added_features:
                        added_features.append(feature_name)
                        features_dict[feature_index]["Name"] = feature_name
                        continue
                    else:
                        break
                elif "PRECURSORMZ" in feature_data.upper():
                    features_dict[feature_index]["Precursor m/z"] = feature_data.split(": ")[-1]
                    continue
                elif "INCHIKEY" in feature_data.upper():
                    features_dict[feature_index]["INCHIKEY"] = feature_data.split(": ")[-1]
                    continue
                elif "RETENTIONTIME" in feature_data.upper():
                    features_dict[feature_index]["Retention time"] = feature_data.split(": ")[-1]
                    continue

                # Capture MS2 spectrum
                elif "Num Peaks" in feature_data:

                    # Get number of peaks in MS2 spectrum
                    num_peaks = int(feature_data.split(": ")[-1])

                    # Each line in the MSP corresponds to a peak
                    start_index = data_index + 1
                    end_index = data_index + num_peaks + 1

                    # Each peak is represented as a string e.g. "56.04977\t247187"
                    peaks_in_spectrum = []
                    for peak in feature[start_index:end_index]:
                        peaks_in_spectrum.append(peak.replace("\t", ":"))

                    features_dict[feature_index]["MS2 spectrum"] = str(peaks_in_spectrum)
                    break

    features_dict = { key:value for key, value in features_dict.items() if value["Name"] is not None }

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
                            pos_bio_msp_file=filename)
            )
        elif polarity == "Negative Mode":
            update_msp_file = (
                sa.update(biological_standards_table)
                    .where((biological_standards_table.c.chromatography == chromatography)
                           & (biological_standards_table.c.name == bio_standard))
                    .values(num_neg_features=len(features_dict),
                            neg_bio_msp_file=filename)
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
                            pos_istd_msp_file=filename)
            )
        elif polarity == "Negative Mode":
            update_msp_file = (
                sa.update(chromatography_table)
                    .where(chromatography_table.c.method_id == chromatography)
                    .values(num_neg_standards=len(features_dict),
                            neg_istd_msp_file=filename)
            )

        # Execute UPDATE of MSP file location
        connection.execute(update_msp_file)

    # If the corresponding TXT library existed, delete it
    txt_library = os.path.join(methods_directory, filename.replace(".msp", ".txt"))
    os.remove(txt_library) if os.path.exists(txt_library) else None

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
    if not os.path.exists(methods_directory):
        os.makedirs(methods_directory)

    # Name file accordingly
    if polarity == "Positive Mode":
        filename = chromatography + "_Pos.txt"
    elif polarity == "Negative Mode":
        filename = chromatography + "_Neg.txt"

    txt_file_path = os.path.join(methods_directory, filename)

    # Write CSV columns to tab-delimited text file
    df_internal_standards.to_csv(txt_file_path, sep="\t", index=False)

    # Connect to database
    db_metadata, connection = connect_to_database("Settings")

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
                        pos_istd_msp_file=filename)
        )
    elif polarity == "Negative Mode":
        update_msp_file = (
            sa.update(chromatography_table)
                .where(chromatography_table.c.method_id == chromatography)
                .values(num_neg_standards=len(internal_standards_dict),
                        neg_istd_msp_file=filename)
        )

    # Execute UPDATE of CSV file location
    connection.execute(update_msp_file)

    # If the corresponding MSP library existed, delete it
    msp_library = os.path.join(methods_directory, filename.replace(".txt", ".msp"))
    os.remove(msp_library) if os.path.exists(msp_library) else None

    # Close the connection
    connection.close()


def get_msdial_configurations():

    """
    Returns list of user configurations of MS-DIAL parameters
    """

    engine = sa.create_engine(settings_database)
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
    db_metadata, connection = connect_to_database("Settings")
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


def add_msdial_configuration(msdial_config_name):

    """
    Inserts new user configuration of MS-DIAL parameters into the "msdial_parameters" table
    """

    # Connect to database
    db_metadata, connection = connect_to_database("Settings")

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
         "qc_at_least_filter": "True"}
    )

    # Execute INSERT to database, then close the connection
    connection.execute(insert_config)
    connection.close()


def remove_msdial_configuration(msdial_config_name):

    """
    Deletes user configuration of MS-DIAL parameters from the "msdial_parameters" table
    """

    # Connect to database
    db_metadata, connection = connect_to_database("Settings")

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
    engine = sa.create_engine(settings_database)
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
    alignment_rt_factor, alignment_mz_factor, peak_count_filter, qc_at_least_filter):

    """
    Updates parameters of a selected MS-DIAL configuration
    """

    # Connect to database
    db_metadata, connection = connect_to_database("Settings")

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
                    qc_at_least_filter=qc_at_least_filter)
    )

    # Execute UPDATE to database, then close the connection
    connection.execute(update_parameters)
    connection.close()


def get_msp_file_path(chromatography, polarity, bio_standard=None):

    """
    Returns file paths of MSPs for a selected chromatography / polarity (both stored
    in the methods folder upon user upload) for MS-DIAL parameter file generation
    """

    # Connect to database
    engine = sa.create_engine(settings_database)

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

    msp_file_path = os.path.join(methods_directory, msp_file_path)

    # Return file path
    return msp_file_path


def get_parameter_file_path(chromatography, polarity, biological_standard=None):

    """
    Returns file path of parameters file stored in database
    """

    engine = sa.create_engine(settings_database)

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


def get_msdial_directory():

    """
    Returns location of MS-DIAL folder
    """

    return get_table("Settings", "workspace")["msdial_directory"].astype(str).values[0]


def get_msconvert_directory():

    """
    Returns MSConvert.exe function call
    """

    user = get_msdial_directory().split("/")[2]
    msconvert_folder = [f.path for f in os.scandir("C:/Users/" + user + "/AppData/Local/Apps/") if f.is_dir() and "ProteoWizard" in f.name][0]
    return msconvert_folder


def update_msdial_directory(msdial_directory):

    """
    Updates location of MS-DIAL folder
    """

    db_metadata, connection = connect_to_database("Settings")
    workspace_table = sa.Table("workspace", db_metadata, autoload=True)

    update_msdial_directory = (
        sa.update(workspace_table)
            .where(workspace_table.c.id == 1)
            .values(msdial_directory=msdial_directory)
    )

    connection.execute(update_msdial_directory)
    connection.close()


def get_internal_standards_dict(chromatography, value_type):

    """
    Returns dictionary of internal standard keys mapped to either m/z or RT values
    """

    engine = sa.create_engine(settings_database)
    query = "SELECT * FROM internal_standards " + "WHERE chromatography='" + chromatography + "'"
    df_internal_standards = pd.read_sql(query, engine)

    dict = {}
    keys = df_internal_standards["name"].astype(str).tolist()
    values = df_internal_standards[value_type].astype(float).tolist()

    for index, key in enumerate(keys):
        dict[key] = values[index]

    return dict


def get_internal_standards(chromatography, polarity):

    """
    Returns DataFrame of internal standards for a given chromatography method and polarity
    """

    engine = sa.create_engine(settings_database)

    query = "SELECT * FROM internal_standards " + \
            "WHERE chromatography='" + chromatography + "' AND polarity='" + polarity + "'"

    return pd.read_sql(query, engine)


def get_targeted_features(biological_standard, chromatography, polarity):

    """
    Returns DataFrame of metabolite targets for a given biological standard, chromatography, and polarity
    """

    engine = sa.create_engine(settings_database)

    query = "SELECT * FROM targeted_features " + \
            "WHERE chromatography='" + chromatography + \
            "' AND polarity='" + polarity + \
            "' AND biological_standard ='" + biological_standard + "'"

    return pd.read_sql(query, engine)


def get_biological_standards():

    """
    Returns DataFrame of the "biological_standards" table
    """

    # Get table from database as a DataFrame
    engine = sa.create_engine(settings_database)
    df_biological_standards = pd.read_sql("SELECT * FROM biological_standards", engine)
    return df_biological_standards


def get_biological_standards_list():

    """
    Returns list of biological standards in the database
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
    db_metadata, connection = connect_to_database("Settings")
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

    # Delete corresponding MSPs from "methods" directory
    df = get_table("Settings", "biological_standards")
    df = df.loc[df["name"] == name]
    files_to_delete = df["pos_bio_msp_file"].astype(str).tolist() + df["neg_bio_msp_file"].astype(str).tolist()

    for file in os.listdir(methods_directory):
        if name in files_to_delete:
            os.remove(os.path.join(methods_directory, file))

    # Connect to database and get relevant tables
    db_metadata, connection = connect_to_database("Settings")
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
    db_metadata, connection = connect_to_database("Settings")
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

    engine = sa.create_engine(settings_database)
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
    db_metadata, connection = connect_to_database("Settings")

    # Get QC parameters table
    qc_parameters_table = sa.Table("qc_parameters", db_metadata, autoload=True)

    # Prepare insert of user-inputted run data
    insert_config = qc_parameters_table.insert().values(
        {"config_name": qc_config_name,
         "intensity_dropouts_cutoff": 4,
         "library_rt_shift_cutoff": 0.1,
         "in_run_rt_shift_cutoff": 0.05,
         "library_mz_shift_cutoff": 0.005,
         "intensity_enabled": True,
         "library_rt_enabled": True,
         "in_run_rt_enabled": True,
         "library_mz_enabled": True}
    )

    # Execute INSERT to database, then close the connection
    connection.execute(insert_config)
    connection.close()


def remove_qc_configuration(qc_config_name):

    """
    Deletes QC configuration from the "qc_parameters" table
    """

    # Connect to database
    db_metadata, connection = connect_to_database("Settings")

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


def get_qc_configuration_parameters(config_name=None, instrument_id=None, run_id=None):

    """
    Returns DataFrame of parameters for a selected QC configuration
    """

    df_configurations = get_table("Settings", "qc_parameters")

    # Get selected configuration
    if config_name is not None:
        selected_config = df_configurations.loc[df_configurations["config_name"] == config_name]

    elif instrument_id is not None and run_id is not None:
        df_runs = get_table(instrument_id, "runs")
        config_name = df_runs.loc[df_runs["run_id"] == run_id]["qc_config_id"].values[0]
        selected_config = df_configurations.loc[
            df_configurations["config_name"] == config_name]

    selected_config.drop(inplace=True, columns=["id", "config_name"])

    # Probably not the most efficient way to do this...
    for column in ["intensity_enabled", "library_rt_enabled", "in_run_rt_enabled", "library_mz_enabled"]:
        selected_config.loc[selected_config[column] == 1, column] = True
        selected_config.loc[selected_config[column] == 0, column] = False

    # Return parameters of selected configuration as a tuple
    return selected_config


def update_qc_configuration(config_name, intensity_dropouts_cutoff, library_rt_shift_cutoff, in_run_rt_shift_cutoff,
    library_mz_shift_cutoff, intensity_enabled, library_rt_enabled, in_run_rt_enabled, library_mz_enabled):

    """
    Updates parameters for a given QC configuration
    """

    # Connect to database
    db_metadata, connection = connect_to_database("Settings")

    # Get QC parameters table
    qc_parameters_table = sa.Table("qc_parameters", db_metadata, autoload=True)

    # Prepare insert of user-inputted QC parameters
    update_parameters = (
        sa.update(qc_parameters_table)
            .where(qc_parameters_table.c.config_name == config_name)
            .values(intensity_dropouts_cutoff=intensity_dropouts_cutoff,
                    library_rt_shift_cutoff=library_rt_shift_cutoff,
                    in_run_rt_shift_cutoff=in_run_rt_shift_cutoff,
                    library_mz_shift_cutoff=library_mz_shift_cutoff,
                    intensity_enabled=intensity_enabled,
                    library_rt_enabled=library_rt_enabled,
                    in_run_rt_enabled=in_run_rt_enabled,
                    library_mz_enabled=library_mz_enabled)
    )

    # Execute UPDATE to database, then close the connection
    connection.execute(update_parameters)
    connection.close()


def get_samples_in_run(instrument_id, run_id, sample_type="Both"):

    """
    Returns DataFrame of samples in a given run using local database
    """

    if sample_type == "Sample":
        df = get_table(instrument_id, "sample_qc_results")

    elif sample_type == "Biological Standard":
        df = get_table(instrument_id, "bio_qc_results")

    elif sample_type == "Both":
        df_samples = get_table(instrument_id, "sample_qc_results")
        df_bio_standards = get_table(instrument_id, "bio_qc_results")
        df_bio_standards.drop(columns=["biological_standard"], inplace=True)
        df = df_bio_standards.append(df_samples, ignore_index=True)

    return df.loc[df["run_id"] == run_id]


def get_samples_from_csv(instrument_id, run_id, sample_type="Both"):

    """
    Returns DataFrame of samples in a given run using CSV files from Google Drive
    """

    id = instrument_id.replace(" ", "_") + "_" + run_id
    csv_directory = os.path.join(data_directory, id, "csv")

    samples_csv = os.path.join(csv_directory, "samples.csv")
    bio_standards_csv = os.path.join(csv_directory, "bio_standards.csv")

    if sample_type == "Sample":
        df = pd.read_csv(samples_csv, index_col=False)

    elif sample_type == "Biological Standard":
        df = pd.read_csv(bio_standards_csv, index_col=False)

    elif sample_type == "Both":
        df_samples = pd.read_csv(samples_csv, index_col=False)
        df_bio_standards = pd.read_csv(bio_standards_csv, index_col=False)
        df_bio_standards.drop(columns=["biological_standard"], inplace=True)
        df = df_bio_standards.append(df_samples, ignore_index=True)

    return df.loc[df["run_id"] == run_id]


def get_remaining_samples(instrument_id, run_id):

    """
    Returns list of samples remaining in a given instrument run
    """

    # Get last processed sample in run
    df_run = get_instrument_run(instrument_id, run_id)
    latest_sample = df_run["latest_sample"].astype(str).values[0]

    # Get list of samples in run
    samples = get_samples_in_run(instrument_id, run_id, "Both")["sample_id"].astype(str).tolist()

    # Return all samples if beginning of run
    if latest_sample == "None":
        return samples

    # Get index of latest sample
    latest_sample_index = samples.index(latest_sample)

    # Return list of samples starting at latest sample
    return samples[latest_sample_index:len(samples)]


def get_unprocessed_samples(instrument_id, run_id):

    """
    For an active run, returns:
    1. list of samples that were not processed due to error / runtime termination
    2. the most recent sample that was being monitored / processed
    """

    # Get samples in run
    df_samples = get_samples_in_run(instrument_id, run_id, "Both")

    # Get list of samples in run
    samples = df_samples["sample_id"].astype(str).tolist()

    # Construct dictionary of unprocessed samples in instrument run
    df_unprocessed_samples = df_samples.loc[df_samples["qc_result"].isnull()]
    unprocessed_samples = df_unprocessed_samples["sample_id"].astype(str).tolist()

    # Get acquisition path, data files, and data file extension
    acquisition_path = get_acquisition_path(instrument_id, run_id)
    extension = get_data_file_type(instrument_id)
    directory_files = os.listdir(acquisition_path)
    data_files = [file.split(".")[0] for file in directory_files if file.split(".")[0] in unprocessed_samples]

    # Mark acquired data files
    df_unprocessed_samples.loc[
        df_unprocessed_samples["sample_id"].isin(data_files), "found"] = "Found"
    unprocessed_samples = df_unprocessed_samples.dropna(subset=["found"])["sample_id"].astype(str).tolist()

    # Get current sample
    current_sample = unprocessed_samples[-1]
    del unprocessed_samples[-1]

    # Return as tuple
    return unprocessed_samples, current_sample


def get_current_sample(instrument_id, run_id):

    """
    Returns the current sample being monitored / processed
    """

    # Get latest sample in run
    df_run = get_instrument_run(instrument_id, run_id)
    latest_sample = df_run["latest_sample"].astype(str).values[0]

    # Return second sample if beginning of run
    if latest_sample == "None":
        return samples[1]


def parse_internal_standard_data(instrument_id, run_id, result_type, polarity, status, as_json=True):

    """
    Returns JSON-ified DataFrame of samples (as rows) vs. internal standards (as columns)
    """

    # Get relevant QC results table from database
    if status == "Complete" or status == "Processing":
        df_samples = get_samples_in_run(instrument_id, run_id, "Sample")
    elif status == "Active":
        df_samples = get_samples_from_csv(instrument_id, run_id, "Sample")

    # Filter by polarity
    df_samples = df_samples.loc[df_samples["sample_id"].str.contains(polarity)]
    sample_ids = df_samples["sample_id"].astype(str).tolist()

    # Return None if results are None
    if status == "Processing":
        if len(df_samples[result_type].dropna()) == 0:
            return None

    # Initialize DataFrame with individual records of sample data
    results = df_samples[result_type].astype(str).tolist()
    results = [ast.literal_eval(result) if result != "None" else {} for result in results]
    df_results = pd.DataFrame(results)
    df_results.drop(columns=["Name"], inplace=True)
    df_results["Sample"] = sample_ids

    # Return DataFrame as JSON string
    if as_json:
        return df_results.to_json(orient="records")
    else:
        return df_results


def parse_biological_standard_data(instrument_id, run_id, result_type, polarity, biological_standard, status, as_json=True):

    """
    Returns JSON-ified DataFrame of instrument runs (as columns) vs. targeted features (as rows)
    """

    # Get relevant QC results table from database
    if status == "Complete" or status == "Processing":
        df_samples = get_table(instrument_id, "bio_qc_results")
    elif status == "Active":
        id = instrument_id.replace(" ", "_") + "_" + run_id
        bio_standards_csv = os.path.join(data_directory, id, "csv", "bio_standards.csv")
        df_samples = pd.read_csv(bio_standards_csv, index_col=False)

    # Filter by biological standard type
    df_samples = df_samples.loc[df_samples["biological_standard"] == biological_standard]

    # Filter by polarity
    df_samples = df_samples.loc[df_samples["sample_id"].str.contains(polarity)]

    # Filter by instrument
    df_runs = get_table(instrument_id, "runs")
    chromatography = df_runs.loc[df_runs["run_id"] == run_id]["chromatography"].values[0]

    # Filter by chromatography
    run_ids = df_runs.loc[df_runs["chromatography"] == chromatography]["run_id"].astype(str).tolist()
    df_samples = df_samples.loc[df_samples["run_id"].isin(run_ids)]
    run_ids = df_samples["run_id"].astype(str).tolist()

    # Initialize DataFrame with individual records of sample data
    results = df_samples[result_type].fillna('{}').tolist()
    results = [ast.literal_eval(result) for result in results]
    df_results = pd.DataFrame(results)
    df_results["Name"] = run_ids

    # Return DataFrame as JSON string
    if as_json:
        return df_results.to_json(orient="records")
    else:
        return df_results


def parse_internal_standard_qc_data(instrument_id, run_id, polarity, result_type, status, as_json=True):

    """
    Returns JSON-ified DataFrame of samples (as rows) vs. internal standards (as columns)
    """

    # Get relevant QC results table from database
    if status == "Complete" or status == "Processing":
        df_samples = get_samples_in_run(instrument_id, run_id, "Sample")
    elif status == "Active":
        df_samples = get_samples_from_csv(instrument_id, run_id, "Sample")

    # Filter by polarity
    df_samples = df_samples.loc[df_samples["sample_id"].str.contains(polarity)]

    # For results DataFrame, each index corresponds to the result type
    get_result_index = {
        "Delta m/z": 0,
        "Delta RT": 1,
        "In-run delta RT": 2,
        "Intensity dropout": 3,
        "Warnings": 4,
        "Fails": 5
    }

    # Get list of results using result type
    sample_ids = df_samples["sample_id"].astype(str).tolist()
    results = df_samples["qc_dataframe"].fillna('[{}, {}, {}, {}, {}, {}]').astype(str).tolist()

    type_index = get_result_index[result_type]
    results = [ast.literal_eval(result)[type_index] for result in results]
    df_results = pd.DataFrame(results)
    df_results.drop(columns=["Name"], inplace=True)
    df_results["Sample"] = sample_ids

    # Return DataFrame as JSON string
    if as_json:
        return df_results.to_json(orient="records")
    else:
        return df_results


def get_workspace_users_list():

    """
    Returns a list of users that have access to the MS-AutoQC workspace
    """

    return get_table("Settings", "gdrive_users")["email_address"].astype(str).tolist()


def add_user_to_workspace(email_address):

    """
    Gives user access to workspace in Google Drive, stores email in database
    """

    if email_address in get_workspace_users_list():
        return "User already exists"

    # Get Google Drive instance
    drive = get_drive_instance()

    # Get ID of MS-AutoQC folder in Google Drive
    gdrive_folder_id = get_drive_folder_id()

    if gdrive_folder_id is not None:
        # Add user access by updating permissions
        folder = drive.CreateFile({"id": gdrive_folder_id})
        permission = folder.InsertPermission({
            "type": "user",
            "role": "writer",
            "value": email_address})

        # Insert user email address in "gdrive_users" table
        db_metadata, connection = connect_to_database("Settings")
        gdrive_users_table = sa.Table("gdrive_users", db_metadata, autoload=True)

        insert_user_email = gdrive_users_table.insert().values(
            {"name": permission["name"],
             "email_address": email_address,
             "permission_id": permission["id"]})

        connection.execute(insert_user_email)
        connection.close()

    else:
        return "Error"


def delete_user_from_workspace(email_address):

    """
    Removes user access to workspace in Google Drive, deletes email in database
    """

    if email_address not in get_workspace_users_list():
        return "User does not exist"

    # Get Google Drive instance
    drive = get_drive_instance()

    # Get ID of MS-AutoQC folder in Google Drive
    gdrive_folder_id = get_drive_folder_id()

    if gdrive_folder_id is not None:
        # Get permission ID of user from database
        folder = drive.CreateFile({"id": gdrive_folder_id})
        df_gdrive_users = get_table("Settings", "gdrive_users")
        df_gdrive_users = df_gdrive_users.loc[df_gdrive_users["email_address"] == email_address]
        permission_id = df_gdrive_users["permission_id"].astype(str).values[0]

        # Delete user access by updating permissions
        folder.DeletePermission(permission_id)

        # Delete user email address in "gdrive_users" table
        db_metadata, connection = connect_to_database("Settings")
        gdrive_users_table = sa.Table("gdrive_users", db_metadata, autoload=True)

        delete_user_email = (
            sa.delete(gdrive_users_table)
                .where((gdrive_users_table.c.email_address == email_address))
        )

        connection.execute(delete_user_email)
        connection.close()

    else:
        return "Error"


def get_qc_results(instrument_id, sample_list, is_bio_standard=False):

    """
    Returns DataFrame of QC results for a given sample list
    TODO: Fix for active instrument runs
    """

    if len(sample_list) == 0:
        return pd.DataFrame()

    database = get_database_file(instrument_id=instrument_id, sqlite_conn=True)
    engine = sa.create_engine(database)

    sample_list = str(sample_list).replace("[", "(").replace("]", ")")

    if is_bio_standard:
        query = "SELECT sample_id, qc_result FROM bio_qc_results WHERE sample_id in " + sample_list
    else:
        query = "SELECT sample_id, qc_result FROM sample_qc_results WHERE sample_id in " + sample_list

    return pd.read_sql(query, engine)


def create_workspace_metadata():

    """
    Creates row in "workspace" table to store metadata
    """

    db_metadata, connection = connect_to_database("Settings")
    workspace_table = sa.Table("workspace", db_metadata, autoload=True)
    connection.execute(workspace_table.insert().values({"id": 1}))
    connection.close()


def get_device_identity():

    """
    Returns device identity
    """

    return get_table("Settings", "workspace")["instrument_identity"].astype(str).tolist()[0]


def set_device_identity(is_instrument_computer, instrument_id):

    """
    Indicates whether the user's device is the instrument PC or not
    """

    if not is_instrument_computer:
        instrument_id = "Shared user"

    db_metadata, connection = connect_to_database("Settings")
    workspace_table = sa.Table("workspace", db_metadata, autoload=True)

    update_identity = (
        sa.update(workspace_table)
            .where(workspace_table.c.id == 1)
            .values(
                is_instrument_computer=is_instrument_computer,
                instrument_identity=instrument_id
        )
    )

    connection.execute(update_identity)
    connection.close()


def run_is_on_instrument_pc(instrument_id, run_id):

    """
    Validates that the current device is the instrument PC on which the run was started
    """

    instrument_id = get_instrument_run(instrument_id, run_id)["instrument_id"].astype(str).tolist()[0]
    device_identity = get_table("Settings", "workspace")["instrument_identity"].astype(str).tolist()[0]

    if instrument_id == device_identity:
        return True
    else:
        return False


def update_slack_bot_token(slack_bot_token):

    """
    Updates Slack bot user OAuth 2.0 token in "workspace" table
    """

    db_metadata, connection = connect_to_database("Settings")
    workspace_table = sa.Table("workspace", db_metadata, autoload=True)

    update_slack_bot_token = (
        sa.update(workspace_table)
            .where(workspace_table.c.id == 1)
            .values(slack_bot_token=slack_bot_token)
    )

    connection.execute(update_slack_bot_token)
    connection.close()


def get_slack_bot_token():

    """
    Returns Slack bot token stored in database
    """

    return get_table("Settings", "workspace")["slack_bot_token"].astype(str).values[0]


def update_slack_channel(slack_channel, notifications_enabled):

    """
    Updates Slack channel registered for notifications
    """

    db_metadata, connection = connect_to_database("Settings")
    workspace_table = sa.Table("workspace", db_metadata, autoload=True)

    update_slack_channel = (
        sa.update(workspace_table)
            .where(workspace_table.c.id == 1)
            .values(
                slack_channel=slack_channel.replace("#", ""),
                slack_enabled=notifications_enabled)
    )

    connection.execute(update_slack_channel)
    connection.close()


def get_slack_channel():

    """
    Returns Slack channel registered for notifications
    """

    return get_table("Settings", "workspace")["slack_channel"].astype(str).values[0]


def get_slack_notifications_toggled():

    """
    Returns Slack notification toggled setting
    """

    try:
        return get_table("Settings", "workspace")["slack_enabled"].astype(int).tolist()[0]
    except:
        return None


def get_email_notifications_list():

    """
    Returns list of emails registered for MS-AutoQC notifications
    """

    return get_table("Settings", "email_notifications")["email_address"].astype(str).tolist()


def register_email_for_notifications(email_address):

    """
    Inserts email address into "email_notifications" table
    """

    db_metadata, connection = connect_to_database("Settings")
    email_notifications_table = sa.Table("email_notifications", db_metadata, autoload=True)

    insert_email_address = email_notifications_table.insert().values({
        "email_address": email_address
    })

    connection.execute(insert_email_address)
    connection.close()


def delete_email_from_notifications(email_address):

    """
    Deletes email address from "email_notifications" table
    """

    db_metadata, connection = connect_to_database("Settings")
    email_notifications_table = sa.Table("email_notifications", db_metadata, autoload=True)

    delete_email_address = (
        sa.delete(email_notifications_table)
            .where((email_notifications_table.c.email_address == email_address))
    )

    connection.execute(delete_email_address)
    connection.close()


def get_completed_samples_count(instrument_id, run_id, status):

    """
    Returns tuple containing count for completed samples and total samples in a given run
    """

    if status == "Active" and sync_is_enabled():
        df_instrument_run = get_instrument_run_from_csv(instrument_id, run_id)
    else:
        df_instrument_run = get_instrument_run(instrument_id, run_id)

    completed = df_instrument_run["completed"].astype(int).tolist()[0]
    total_samples = df_instrument_run["samples"].astype(int).tolist()[0]
    return (completed, total_samples)


def get_run_progress(instrument_id, run_id, status):

    """
    Returns progress of instrument run as a percentage of samples completed
    """

    completed, total_samples = get_completed_samples_count(instrument_id, run_id, status)
    percent_complete = (completed / total_samples) * 100
    return round(percent_complete, 1)


def update_sample_counters_for_run(instrument_id, run_id, qc_result, latest_sample):

    """
    Increments "completed" count, as well as "pass" and "fail" counts accordingly
    """

    df_instrument_run = get_instrument_run(instrument_id, run_id)
    completed = df_instrument_run["completed"].astype(int).tolist()[0] + 1
    passes = df_instrument_run["passes"].astype(int).tolist()[0]
    fails = df_instrument_run["fails"].astype(int).tolist()[0]

    if qc_result == "Pass" or qc_result == "Warning":
        passes = passes + 1
    elif qc_result == "Fail":
        fails = fails + 1

    db_metadata, connection = connect_to_database(instrument_id)
    instrument_runs_table = sa.Table("runs", db_metadata, autoload=True)

    update_status = (
        sa.update(instrument_runs_table)
            .where(instrument_runs_table.c.run_id == run_id)
            .values(
                completed=completed,
                passes=passes,
                fails=fails,
                latest_sample=latest_sample
        )
    )

    connection.execute(update_status)
    connection.close()


def mark_run_as_completed(instrument_id, run_id):

    """
    Marks instrument run status as completed
    """

    db_metadata, connection = connect_to_database(instrument_id)
    instrument_runs_table = sa.Table("runs", db_metadata, autoload=True)

    update_status = (
        sa.update(instrument_runs_table)
            .where(instrument_runs_table.c.run_id == run_id)
            .values(status="Complete")
    )

    connection.execute(update_status)
    connection.close()


def skip_sample(instrument_id, run_id):

    """
    Skip sample (use if MS-DIAL gets stuck processing a corrupted file)
    """

    # Get next sample
    samples = get_remaining_samples(instrument_id, run_id)
    next_sample = samples[1]

    # Set latest sample to next sample
    db_metadata, connection = connect_to_database(instrument_id)
    instrument_runs_table = sa.Table("runs", db_metadata, autoload=True)

    connection.execute((
        sa.update(instrument_runs_table)
            .where(instrument_runs_table.c.run_id == run_id)
            .values(latest_sample=next_sample)
    ))

    connection.close()


def store_pid(instrument_id, run_id, pid):

    """
    Store acquisition listener process ID to allow termination later
    """

    db_metadata, connection = connect_to_database(instrument_id)
    instrument_runs_table = sa.Table("runs", db_metadata, autoload=True)

    update_pid = (
        sa.update(instrument_runs_table)
            .where(instrument_runs_table.c.run_id == run_id)
            .values(pid=pid)
    )

    connection.execute(update_pid)
    connection.close()


def get_pid(instrument_id, run_id):

    """
    Retrieves acquisition listener process ID from "runs" table
    """

    try:
        return get_instrument_run(instrument_id, run_id)["pid"].astype(int).tolist()[0]
    except:
        return None


def upload_to_google_drive(file_dict):

    """
    Uploads files to MS-AutoQC folder in Google Drive
    Input: dictionary with key-value structure { filename : file path }
    Output: dictionary with key-value structure { filename : Google Drive ID }
    """

    # Get Google Drive instance
    drive = get_drive_instance()

    # Get Google Drive ID for the MS-AutoQC folder
    folder_id = get_drive_folder_id()

    # Store Drive ID's of uploaded file(s)
    drive_ids = {}

    # Validate Google Drive folder ID
    if folder_id is not None:
        if folder_id != "None" and folder_id != "":

            # Upload each file to Google Drive
            for filename in file_dict.keys():
                if os.path.exists(file_dict[filename]):
                    metadata = {
                        "title": filename,
                        "parents": [{"id": folder_id}],
                    }
                    file = drive.CreateFile(metadata=metadata)
                    file.SetContentFile(file_dict[filename])
                    file.Upload()

                    drive_ids[file["title"]] = file["id"]

    return drive_ids


def upload_qc_results(instrument_id, run_id):
    
    """
    Uploads QC results for a given run to Google Drive as a CSV file
    """

    id = instrument_id.replace(" ", "_") + "_" + run_id

    # Get Google Drive instance
    drive = get_drive_instance()

    # Define file names and file paths
    run_filename = "run.csv"
    samples_csv_filename = "samples.csv"
    bio_standards_csv_filename = "bio_standards.csv"

    run_directory = os.path.join(data_directory, id)
    if not os.path.exists(run_directory):
        os.makedirs(run_directory)

    csv_directory = os.path.join(run_directory, "csv")
    if not os.path.exists(csv_directory):
        os.makedirs(csv_directory)

    run_csv_path = os.path.join(csv_directory, run_filename)
    samples_csv_path = os.path.join(csv_directory, samples_csv_filename)
    bio_standards_csv_path = os.path.join(csv_directory, bio_standards_csv_filename)

    # Convert sample and biological standard QC results from database into CSV files
    df_run = get_instrument_run(instrument_id, run_id)
    df_run.to_csv(run_csv_path, index=False)

    df_samples = get_samples_in_run(instrument_id=instrument_id, run_id=run_id, sample_type="Sample")
    if len(df_samples) > 0:
        df_samples.to_csv(samples_csv_path, index=False)

    df_bio_standards = get_table(instrument_id, "bio_qc_results")
    if len(df_bio_standards) > 0:
        df_bio_standards.to_csv(bio_standards_csv_path, index=False)

    # Compress CSV files into a ZIP archive for faster upload
    zip_filename = id + ".zip"
    zip_file_path = zip_csv_files(
        input_directory=csv_directory, output_directory_and_name=os.path.join(run_directory, id))

    zip_file = {zip_filename: zip_file_path}

    # Get Google Drive ID for the CSV files ZIP archive
    zip_file_drive_id = get_instrument_run(instrument_id, run_id)["drive_id"].tolist()[0]

    # Update existing ZIP archive in Google Drive
    if zip_file_drive_id is not None:

        file = drive.CreateFile({
            "id": zip_file_drive_id,
            "title": zip_filename,
        })

        # Execute upload
        file.SetContentFile(zip_file_path)
        file.Upload()

    # If zip file Drive ID does not exist,
    else:

        # Upload CSV files ZIP archive to Google Drive for first time
        drive_id = upload_to_google_drive(zip_file)[zip_filename]

        # Store Drive ID of ZIP file in local database
        db_metadata, connection = connect_to_database(instrument_id)
        runs_table = sa.Table("runs", db_metadata, autoload=True)

        connection.execute((
            sa.update(runs_table)
                .where(runs_table.c.run_id == run_id)
                .values(drive_id=drive_id)
        ))

        connection.close()


def download_qc_results(instrument_id, run_id):

    """
    Downloads CSV files of QC results from Google Drive and stores in data directory
    """

    id = instrument_id.replace(" ", "_") + "_" + run_id

    # Get Google Drive instance
    drive = get_drive_instance()

    # Initialize directories
    run_directory = os.path.join(data_directory, id)
    if not os.path.exists(run_directory):
        os.makedirs(run_directory)

    csv_directory = os.path.join(run_directory, "csv")
    if not os.path.exists(csv_directory):
        os.makedirs(csv_directory)

    # Zip file
    zip_filename = id + ".zip"
    zip_file_path = os.path.join(run_directory, zip_filename)

    # Get Google Drive folder ID
    gdrive_folder_id = get_drive_folder_id()

    # Find and download ZIP archive of CSV files from Google Drive
    for file in drive.ListFile({"q": "'" + gdrive_folder_id + "' in parents and trashed=false"}).GetList():
        if file["title"] == zip_filename:
            os.chdir(run_directory)
            file.GetContentFile(file["title"])
            os.chdir(root_directory)
            break

    # Unzip archive
    unzip_csv_files(zip_file_path, csv_directory)

    # Define and return file paths
    run_csv = os.path.join(csv_directory, "run.csv")
    samples_csv = os.path.join(csv_directory, "samples.csv")
    bio_standards_csv_file = os.path.join(csv_directory, "bio_standards.csv")

    return (run_csv, samples_csv, bio_standards_csv_file)


def get_drive_folder_id():

    """
    Returns Google Drive ID for the MS-AutoQC folder
    """

    return get_table("Settings", "workspace")["gdrive_folder_id"].values[0]


def get_database_drive_id(instrument_id):

    """
    Returns Drive ID for a given instrument's database
    """

    df = get_table("Settings", "instruments")
    return df.loc[df["name"] == instrument_id]["drive_id"].values[0]


def upload_database(instrument_id, sync_settings=False):

    """
    Uploads database file and methods directory to Google Drive
    """

    # Get Google Drive ID's for the MS-AutoQC folder and database file
    gdrive_folder_id = get_drive_folder_id()
    instrument_db_file_id = get_database_drive_id(instrument_id)

    # Get Google Drive instance
    drive = get_drive_instance()

    # Ensure that another device is not currently uploading to Google Drive
    if not safe_to_upload(gdrive_folder_id):
        return None

    # Vacuum database to optimize size
    execute_vacuum(instrument_id)

    # Send upload signal
    send_sync_signal(gdrive_folder_id)

    # Upload methods directory to Google Drive
    if sync_settings == True:
        upload_methods()

    # Upload database to Google Drive
    if gdrive_folder_id is not None and instrument_db_file_id is not None:

        # Upload zipped database
        zip_database(instrument_id=instrument_id)
        file = drive.CreateFile(
            {"id": instrument_db_file_id, "title": instrument_id.replace(" ", "_") + ".zip"})
        file.SetContentFile(get_database_file(instrument_id, zip=True))
        file.Upload()

        # Save modifiedDate of database file
        remember_last_modified(database=instrument_id, modified_date=file["modifiedDate"])

    else:
        return None

    # Indicate that uploading is complete
    remove_sync_signal(gdrive_folder_id)

    return time.strftime("%H:%M:%S")


def download_database(instrument_id, sync_settings=False):

    """
    Downloads database file from Google Drive (for users signed in to workspace externally from instrument PC)
    """

    db_zip_file = instrument_id.replace(" ", "_") + ".zip"

    # If the database was not modified by another instrument, skip download (for instruments only)
    if is_instrument_computer():
        if not database_was_modified(instrument_id):
            return None

    # Get Google Drive instance
    drive = get_drive_instance()

    # Get Google Drive ID's for the MS-AutoQC folder and database file
    gdrive_folder_id = get_drive_folder_id()
    instrument_db_file_id = get_instrument(instrument_id)["drive_id"].values[0]

    # If Google Drive folder is found, look for database next
    if gdrive_folder_id is not None and instrument_db_file_id is not None:

        # Download newly added / modified MSP files in MS-AutoQC > methods
        if sync_settings == True:
            download_methods(skip_check=True)

        try:
            for file in drive.ListFile({"q": "'" + gdrive_folder_id + "' in parents and trashed=false"}).GetList():
                if file["title"] == db_zip_file:

                    # Download and unzip database
                    os.chdir(data_directory)                        # Change to data directory
                    file.GetContentFile(file["title"])              # Download database and get file ID
                    os.chdir(root_directory)                        # Return to root directory
                    unzip_database(instrument_id=instrument_id)     # Unzip database

                    # Save modifiedDate of database file
                    remember_last_modified(database=instrument_id, modified_date=file["modifiedDate"])

        except Exception as error:
            print("Error downloading database from Google Drive:", error)
            return None
    else:
        return None

    return time.strftime("%H:%M:%S")


def upload_methods():

    """
    Uploads methods directory ZIP archive to Google Drive
    """

    df_workspace = get_table("Settings", "workspace")
    methods_zip_file_id = df_workspace["methods_zip_file_id"].values[0]

    # Vacuum database to optimize size
    execute_vacuum("Settings")

    # Get Google Drive instance
    drive = get_drive_instance()

    # Upload methods ZIP archive to Google Drive
    if methods_zip_file_id is not None:

        # Upload zipped database
        methods_zip_file = zip_methods()
        file = drive.CreateFile({"id": methods_zip_file_id, "title": "methods.zip"})
        file.SetContentFile(methods_zip_file)
        file.Upload()

        # Save modifiedDate of methods ZIP file
        remember_last_modified(database="Settings", modified_date=file["modifiedDate"])

    else:
        return None


def download_methods(skip_check=False):

    """
    Downloads methods directory ZIP archive from Google Drive
    """

    # If the database was not modified by another instrument, skip download (for instruments only)
    if not skip_check:
        if is_instrument_computer():
            if not database_was_modified("Settings"):
                return None

    # Get device identity
    instrument_bool = is_instrument_computer()
    device_identity = get_device_identity()

    # Get Google Drive instance
    drive = get_drive_instance()

    # Get Google Drive folder ID
    gdrive_folder_id = get_drive_folder_id()

    try:
        # Download and unzip methods directory
        for file in drive.ListFile({"q": "'" + gdrive_folder_id + "' in parents and trashed=false"}).GetList():
            if file["title"] == "methods.zip":
                os.chdir(data_directory)                # Change to data directory
                file.GetContentFile(file["title"])      # Download methods ZIP archive
                os.chdir(root_directory)                # Return to root directory
                unzip_methods()                         # Unzip methods directory

                # Save modifiedDate of methods directory
                remember_last_modified(database="Settings", modified_date=file["modifiedDate"])

    except Exception as error:
        print("Error downloading methods from Google Drive:", error)
        return None

    # Update user device identity
    set_device_identity(is_instrument_computer=instrument_bool, instrument_identity=device_identity)
    return time.strftime("%H:%M:%S")


def remember_last_modified(database, modified_date):

    """
    Stores last modified time of database file in Google Drive (after upload)
    """

    db_metadata, connection = connect_to_database("Settings")
    instruments_table = sa.Table("instruments", db_metadata, autoload=True)
    workspace_table = sa.Table("workspace", db_metadata, autoload=True)

    if database == "Settings":
        connection.execute((
            sa.update(workspace_table)
                .where((workspace_table.c.id == 1))
                .values(methods_last_modified=modified_date)
        ))
    else:
        connection.execute((
            sa.update(instruments_table)
            .where((instruments_table.c.name == database))
            .values(last_modified=modified_date)
        ))

    connection.close()


def database_was_modified(database_name):

    """
    Returns True if workspace file was modified by another instrument PC in Google Drive, and False if not
    """

    # Get Google Drive folder ID from database
    gdrive_folder_id = get_drive_folder_id()

    # Compare "last modified" values
    if database_name == "Settings":
        local_last_modified = get_table("Settings", "workspace")["methods_last_modified"].values[0]
        filename = "methods.zip"
    else:
        local_last_modified = get_instrument(database_name)["last_modified"].values[0]
        filename = database_name.replace(" ", "_") + ".zip"

    # Get Google Drive instance
    drive = get_drive_instance()

    drive_last_modified = None
    for file in drive.ListFile({"q": "'" + gdrive_folder_id + "' in parents and trashed=false"}).GetList():
        if file["title"] == filename:
            drive_last_modified = file["modifiedDate"]
            break

    if local_last_modified == drive_last_modified:
        return False
    else:
        return True


def send_sync_signal(folder_id):

    """
    Uploads empty file to signal that an instrument PC is syncing to Google Drive
    """

    # Get Google Drive instance
    drive = get_drive_instance()

    try:
        drive.CreateFile(metadata={"title": "Syncing", "parents": [{"id": folder_id}]}).Upload()
        return True
    except:
        return False


def safe_to_upload(folder_id):

    """
    Returns False if another device is currently uploading to Google Drive, else True
    """

    # Get Google Drive instance
    drive = get_drive_instance()

    for file in drive.ListFile({"q": "'" + folder_id + "' in parents and trashed=false"}).GetList():
        if file["title"] == "Syncing":
            return False

    return True


def remove_sync_signal(folder):

    """
    Uploads empty file to signal that an instrument PC is syncing to Google Drive
    """

    # Get Google Drive instance
    drive = get_drive_instance()

    try:
        for file in drive.ListFile({"q": "'" + folder + "' in parents and trashed=false"}).GetList():
            if file["title"] == "Syncing":
                file.Delete()
        return True
    except:
        return False


def delete_active_run_csv_files(instrument_id, run_id):

    """
    Checks for and deletes CSV files from Google Drive at the end of an active instrument run
    """

    id = instrument_id.replace(" ", "_") + "_" + run_id + ".zip"

    # Find zip archive of CSV files in Google Drive and delete it
    drive = get_drive_instance()
    gdrive_folder_id = get_drive_folder_id()

    if gdrive_folder_id is not None:
        drive_file_list = drive.ListFile({"q": "'" + gdrive_folder_id + "' in parents and trashed=false"}).GetList()
        for file in drive_file_list:
            if file["title"] == id:
                file.Delete()
                break

    # Delete zip archive from /data
    csv_directory = os.path.join(data_directory, id, "csv")
    shutil.rmtree(csv_directory)

    # Delete Drive ID from database
    db_metadata, connection = connect_to_database(instrument_id)
    runs_table = sa.Table("runs", db_metadata, autoload=True)

    connection.execute((
        sa.update(runs_table)
            .where(runs_table.c.run_id == run_id)
            .values(drive_id=None)
    ))

    connection.close()


def sync_on_run_completion(instrument_id, run_id):

    """
    Syncs database with Google Drive at the end of an active instrument run.
    1. Ensure another instrument is not syncing
    2. Send sync signal
    3. If modified, download up-to-date database
    4. Merge active run CSV files into database
    5. Upload database to Google Drive
    6. Delete active run CSV files
    """

    # Get Google Drive instance and folder ID
    drive = get_drive_instance()
    gdrive_folder_id = get_drive_folder_id()

    # Ensure another instrument is not uploading or syncing (give 3 attempts)
    while not safe_to_upload(gdrive_folder_id):
        time.sleep(15)
        if not safe_to_upload(gdrive_folder_id):
            time.sleep(15)
            if not safe_to_upload(gdrive_folder_id):
                return False
        break

    # Send sync signal
    try:
        send_sync_signal(gdrive_folder_id)
    except Exception as error:
        print("sync_on_run_completion() – Error sending sync signal:", error)
        return None

    # If modified, download up-to-date database
    try:
        download_database()
    except Exception as error:
        print("sync_on_run_completion() – Error downloading database during sync:", error)
        return None

    # Remove sync signal for upload
    try:
        remove_sync_signal(gdrive_folder_id)
    except Exception as error:
        print("sync_on_run_completion() – Error removing sync signal", error)

    # Upload database to Google Drive
    try:
        upload_database(instrument_id)
    except Exception as error:
        print("sync_on_run_completion() – Error uploading database during sync", error)
        return None

    # Delete active run CSV files
    try:
        delete_active_run_csv_files(instrument_id, run_id)
    except Exception as error:
        print("sync_on_run_completion() – Error deleting CSV files after sync", error)
        return None


def get_data_file_type(instrument_id):

    """
    Returns expected data file extension based on instrument vendor type (incomplete)
    """

    engine = sa.create_engine(settings_database)
    df_instruments = pd.read_sql("SELECT * FROM instruments WHERE name='" + instrument_id + "'", engine)
    vendor = df_instruments["vendor"].astype(str).tolist()[0]

    if vendor == "Thermo Fisher":
        return "raw"
    elif vendor == "Agilent":
        return "d"
    elif vendor == "Bruker":
        return "baf"
    elif vendor == "Waters":
        return "raw"
    elif vendor == "Sciex":
        return "wiff2"


def is_completed_run(instrument_id, run_id):

    """
    Returns True if the job is for a completed run, and False if job is for an active run
    """

    try:
        job_type = get_instrument_run(instrument_id, run_id)["job_type"].astype(str).values[0]
        if job_type == "completed":
            return True
        else:
            return False
    except:
        print("Could not get MS-AutoQC job type.")
        traceback.print_exc()
        return False


def delete_temp_directory(instrument_id, run_id):

    """
    Deletes temporary data file directory in local app directory
    """

    # Delete temporary data file directory
    try:
        id = instrument_id.replace(" ", "_") + "_" + run_id
        temp_directory = os.path.join(data_directory, id)
        if os.path.exists(temp_directory):
            shutil.rmtree(temp_directory)
    except:
        print("Could not delete temporary data directory.")


def pipeline_valid(module=None):

    """
    Validates that MSConvert and MS-DIAL dependencies are installed
    """

    try:
        msconvert_installed = os.path.exists(os.path.join(get_msconvert_directory(), "msconvert.exe"))
    except:
        msconvert_installed = False

    try:
        msdial_installed = os.path.exists(os.path.join(get_msdial_directory(), "MsdialConsoleApp.exe"))
    except:
        msdial_installed = False

    if module == "msdial":
        return msdial_installed
    elif module == "msconvert":
        return msconvert_installed
    else:
        return msconvert_installed and msdial_installed