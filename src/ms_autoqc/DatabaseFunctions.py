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
import base64
from email.message import EmailMessage
import google.auth as google_auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Set ms_autoqc/src as the working directory
src_folder = os.path.dirname(os.path.realpath(__file__))
os.chdir(src_folder)

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
alt_credentials = os.path.join(auth_directory, "email_credentials.txt")
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
    Returns database file for a given instrument ID.

    Args:
        instrument_id (str):
            Instrument ID that specifies which database file to retrieve
        sqlite_conn (bool, default False):
            Whether to receive the path for establishing a SQLite connection
        zip (bool, default False):
            Whether to receive the path of the database file in the local app directory

    Returns:
        str: Path for the database file
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
    Establishes a connection to a SQLite database of choice

    Args:
        name (str):
            Name of the database, either "Settings" or an instrument ID

    Returns:
        sqlalchemy.MetaData:
            A container object that consists of different features of a database being described
        sqlalchemy.Connection:
            An object that represents a single DBAPI connection, and always emits SQL statements within
            the context of a transaction block
    """

    if name == "Settings":
        database_file = settings_database
    else:
        database_file = get_database_file(instrument_id=name, sqlite_conn=True)

    engine = sa.create_engine(database_file)
    db_metadata = sa.MetaData(bind=engine)
    connection = engine.connect()

    return db_metadata, connection


def create_databases(instrument_id, new_instrument=False):

    """
    Initializes SQLite databases for 1) instrument data and 2) workspace settings.

    Creates the following tables in the instrument database: "runs", "bio_qc_results", "sample_qc_results".

    Creates the following tables in the settings database: "biological_standards", "chromatography_methods",
    "email_notifications", "instruments", "gdrive_users", "internal_standards", "msdial_parameters", "qc_parameters",
    "targeted_features", "workspace".

    Args:
        instrument_id (str):
            Instrument ID to name the new database ("Thermo QE 1" becomes "Thermo_QE_1.db")
        new_instrument (bool, default False):
            Whether a new instrument database is being added to a workspace, or whether a new
            instrument database AND settings database are being created for the first time

    Returns:
        None
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
        sa.Column("polarity", TEXT),
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
        sa.Column("polarity", TEXT),
        sa.Column("position", TEXT),
        sa.Column("md5", TEXT),
        sa.Column("precursor_mz", TEXT),
        sa.Column("retention_time", TEXT),
        sa.Column("intensity", TEXT),
        sa.Column("qc_dataframe", TEXT),
        sa.Column("qc_result", TEXT)
    )

    qc_db_metadata.create_all(qc_db_engine)

    # If only creating instrument database, save and return here
    if new_instrument:
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
    set_device_identity(is_instrument_computer=True, instrument_id=instrument_id)
    return None


def execute_vacuum(database):

    """
    Executes VACUUM command on the database of choice.

    Args:
        database (str): name of the database, either "Settings" or Instrument ID

    Returns:
        None
    """

    db_metadata, connection = connect_to_database(database)
    connection.execute("VACUUM")
    connection.close()


def get_drive_instance():

    """
    Returns user-authenticated Google Drive instance.
    """

    return GoogleDrive(auth_container[0])


def launch_google_drive_authentication():

    """
    Launches Google Drive authentication flow and sets authentication instance.
    """

    auth_container[0] = GoogleAuth(settings_file=drive_settings_file)
    auth_container[0].LocalWebserverAuth()


def save_google_drive_credentials():

    """
    Saves Google credentials to a credentials.txt file.
    """

    auth_container[0].SaveCredentialsFile(credentials_file)


def initialize_google_drive():

    """
    Initializes instance of Google Drive using credentials.txt and settings.yaml in /auth directory

    Args:
        None

    Returns:
        bool: Whether the Google client credentials file (in the "auth" directory) exists.
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
        save_google_drive_credentials()

    # Makes small modification for emails (for usage with Google's google.auth)
    if not os.path.exists(alt_credentials):
        data = None
        with open(credentials_file, "r") as file:
            data = json.load(file)
            data["type"] = "authorized_user"
        with open(alt_credentials, "w") as file:
            json.dump(data, file)

    return os.path.exists(credentials_file)


def is_valid(instrument_id=None):

    """
    Checks that all required tables in all databases (or a single database of choice) are present.

    Args:
        instrument_id (str, default None):
            Specified if validating a specific database

    Returns:
        None
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
            database_files = [file.replace(".db", "") for file in os.listdir(data_directory) if ".db" in file and "journal.db" not in file]
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
    Checks whether Google Drive sync is enabled simply by querying whether Google Drive ID's exist in the database.

    Typically used for separating sync-specific functionality.

    Returns:
        bool: Whether Google Drive sync is enabled or not
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
    Checks whether email notifications are enabled.

    Returns True if databases are valid, Google Drive sync is enabled, and if email addresses were
    registered by user in Settings > General. Returns False if any condition is not met.

    Returns:
        bool: True if email notifications are enabled, False if not
    """

    if not is_valid():
        return False

    if not sync_is_enabled():
        return False

    if len(get_table("Settings", "email_notifications")) > 0:
        return True

    return False


def slack_notifications_are_enabled():

    """
    Checks whether Slack notifications are enabled.

    Returns True if user enabled Slack notifications in Settings > General, and False if not.

    Returns:
        bool: True if Slack notifications are enabled, False if not
    """

    if not is_valid():
        return False

    try:
        return bool(get_table("Settings", "workspace")["slack_enabled"].astype(int).tolist()[0])
    except:
        return False


def is_instrument_computer():

    """
    Checks whether user's device is the instrument computer.

    This is specified during setup. If the user created a new instrument, or signed in as an instrument device, then
    this will return True. If the user signed in to their workspace from a non-instrument device, this will return False.

    Typically used to organize / hide UI functions for instrument and non-instrument devices
    that MS-AutoQC is installed on.

    Returns:
        True if device is instrument computer, False if not
    """

    return bool(get_table("Settings", "workspace")["is_instrument_computer"].astype(int).tolist()[0])


def get_md5_for_settings_db():

    """
    Calculates and returns MD5 checksum for the settings database file.

    Typically used for checking whether the user changed settings and prompting a Google Drive sync (if sync is enabled).

    Returns:
        An MD5 checksum of /data/methods/Settings.db
    """

    hash_md5 = hashlib.md5()

    with open(settings_db_file, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()


def settings_were_modified(md5_checksum):

    """
    Checks whether settings database file has been modified.

    This is done by comparing the checksum computed when Settings were opened (given as a parameter)
    with the checksum computed when Settings were closed (in this function call).

    Args:
        md5_checksum (str):
            An MD5 checksum of /data/methods/Settings.db that was computed when the user opened Settings in the app

    Returns:
        bool: True if checksums don't match, False if checksums match.
    """

    if md5_checksum != get_md5_for_settings_db():
        return True
    else:
        return False


def zip_database(instrument_id=None, filename=None):

    """
    Compresses instrument database file into a ZIP archive in /data directory.

    Used for fast downloads / uploads over network connections to Google Drive (if Google Drive sync is enabled).

    The zip archive is accessible by filename and path in the /data directory. For example, zipping
    the database for "Thermo QE 1" will generate a zip file with path "../data/Thermo_QE_1.zip".

    Args:
        instrument_id (str, default None):
            If specified, selects a database to zip by instrument ID (ex: "Thermo QE 1")
        filename (str, default None):
            If specified, selects a database to zip by filename (ex: "Thermo_QE_1.zip")

    Returns:
        None
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
    Unzips ZIP archive containing instrument database file and deletes the archive when complete.

    Args:
        instrument_id (str, default None):
            If specified, selects a database to zip by instrument ID (ex: "Thermo QE 1")
        filename (str, default None):
            If specified, selects a database to zip by filename (ex: "Thermo_QE_1.zip")

    Returns:
        None
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
    Compresses methods directory into a ZIP archive in /data directory.

    Returns:
        Path for zip archive of methods directory (ex: "../data/methods.zip")
    """

    output_directory_and_name = os.path.join(data_directory, "methods.zip").replace(".zip", "")
    shutil.make_archive(output_directory_and_name, "zip", methods_directory)
    return output_directory_and_name + ".zip"


def unzip_methods():

    """
    Unzips ZIP archive containing methods directory and deletes the archive when complete.
    """

    input_zip = os.path.join(data_directory, "methods.zip")
    shutil.unpack_archive(input_zip, methods_directory, "zip")
    os.remove(input_zip)


def zip_csv_files(input_directory, output_directory_and_name):

    """
    Compresses CSV files into a ZIP archive in /data directory.

    Used for fast upload of instrument run data to Google Drive during an active instrument run (if Google Drive sync is enabled).

    Args:
        input_directory (str):
            The temporary directory for files pertaining to an instrument run, denoted as "Instrument_ID_Run_ID".
            For example, a job with ID "BRDE001" created under instrument with ID "Thermo QE 1" would have its files
            stored in "/data/Thermo_QE_1_BRDE001".
        output_directory_and_name (str):
            Essentially, the file path for the ZIP archive (ex: "/data/Instrument_ID_Run_ID").

    Returns:
        Path for zip archive of CSV files with instrument run data (ex: "../data/Instrument_ID_Run_ID.zip")
    """

    shutil.make_archive(output_directory_and_name, "zip", input_directory)
    return output_directory_and_name + ".zip"


def unzip_csv_files(input_zip, output_directory):

    """
    Unzips ZIP archive of CSV files and deletes the archive upon completion.
    """

    shutil.unpack_archive(input_zip, output_directory, "zip")
    os.remove(input_zip)


def get_table(database_name, table_name):

    """
    Retrieves table from database as a pandas DataFrame object.

    TODO: Improve this function to accept column and record queries

    Args:
        database_name (str):
            The database to query, using instrument ID or "Settings"
        table_name (str):
            The table to retrieve

    Returns:
        DataFrame of table.
    """

    if database_name == "Settings":
        database = settings_database
    else:
        database = get_database_file(database_name, sqlite_conn=True)

    engine = sa.create_engine(database)
    return pd.read_sql("SELECT * FROM " + table_name, engine)


def generate_client_settings_yaml(client_id, client_secret):

    """
    Generates a settings.yaml file for Google authentication in the /auth directory.

    Client ID and client secret are generated and provided by the user in the Google Cloud Console.

    See: https://docs.iterative.ai/PyDrive2/oauth/#automatic-and-custom-authentication-with-settings-yaml

    Args:
        client_id (str):
            The Client ID of the MS-AutoQC application, generated and provided by the user
        client_secret (str):
            The Client Secret of the MS-AutoQC application, generated and provided by the user
    Returns:
        None
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
        "\n",
        "oauth_scope:",
        "  - https://www.googleapis.com/auth/drive",
        "  - https://www.googleapis.com/auth/gmail.send",
        "  - https://www.googleapis.com/auth/userinfo.email"
    ]

    with open(settings_yaml_file, "w") as file:
        for line in lines:
            file.write(line)
            if line != "\n" and line != lines[-1]:
                file.write("\n")


def insert_google_drive_ids(instrument_id, gdrive_folder_id, instrument_db_file_id, methods_zip_file_id):

    """
    Inserts Google Drive ID's into corresponding tables to enable Google Drive sync.

    This function is called when a user creates a new instrument in their workspace.

    The ID's for the following files / folders in Google Drive are stored in the database:
    1. MS-AutoQC folder
    2. Instrument database zip file
    3. Methods directory zip file

    Args:
        instrument_id (str):
            Instrument ID
        gdrive_folder_id (str):
            Google Drive ID for the MS-AutoQC folder (found in the user's root directory in Drive)
        instrument_db_file_id (str):
            Google Drive ID for the instrument database ZIP file
        methods_zip_file_id (str):
            Google Drive ID for the methods directory ZIP file

    Returns:
        None
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
    Inserts a new instrument into the "instruments" table in the Settings database.

    The name is the instrument ID, and the vendor is one of 5 options: Thermo Fisher, Agilent, Bruker, Sciex, and Waters.

    Args:
        name (str):
            Instrument ID
        vendor (str):
            Instrument vendor

    Returns:
        None
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
    Returns list of instruments in database.
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

    Args:
        instrument_id (str): Instrument ID

    Returns:
        DataFrame containing the name, vendor, and drive_id for the given instrument
    """

    engine = sa.create_engine(settings_database)
    return pd.read_sql("SELECT * FROM instruments WHERE name = '" + instrument_id + "'", engine)


def get_filenames_from_sequence(sequence, vendor="Thermo Fisher"):

    """
    Filters preblanks, washes, and shutdown injections from sequence file, and simultaneously assigns
    polariy to each sample based on presence of "Pos" or "Neg" in Instrument Method column.

    This function is called upon starting a new QC job.

    TODO: Adapt this function for other instrument vendors.
    TODO: Check the method filename, not entire file path, for "Pos" and "Neg".
        A folder containing "Pos" or "Neg" will give incorrect polarity assignments.

    Args:
        sequence (str):
            The acquisition sequence file, encoded as a JSON string in "split" format
        vendor (str):
            The instrument vendor (see to-do statements)

    Returns:
        DataFrame of acquisition sequence, with preblanks / washes / shutdowns filtered out and polarities assigned
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

    # Derive polarity from instrument method filename
    df_sequence.loc[df_sequence["Instrument Method"].str.contains(r"Pos", na=False), "Polarity"] = "Pos"
    df_sequence.loc[df_sequence["Instrument Method"].str.contains(r"Neg", na=False), "Polarity"] = "Neg"

    return df_sequence


def get_polarity_for_sample(instrument_id, run_id, sample_id, status):

    """
    Returns polarity for a given sample.

    TODO: Loading hundreds of rows of data before querying for one sample is massively inefficient.
        This function was written in haste and can be easily implemented in a much better way.

    Args:
        instrument_id (str): Instrument ID
        run_id (str): Instrument run ID (job ID)
        sample_id (str): Sample ID
        status (str): Job status

    Returns:
        Polarity for the given sample, as either "Pos" or "Neg".
    """

    if get_device_identity() != instrument_id and sync_is_enabled():
        if status == "Complete":
            df = get_samples_in_run(instrument_id, run_id, "Both")
        elif status == "Active":
            df = get_samples_from_csv(instrument_id, run_id, "Both")
    else:
        df = get_samples_in_run(instrument_id, run_id, "Both")

    try:
        polarity = df.loc[df["sample_id"] == sample_id]["polarity"].astype(str).values[0]
    except:
        print("Could not find polarity for sample in database.")
        polarity = "Neg" if "Neg" in sample_id else "Pos"

    return polarity


def insert_new_run(run_id, instrument_id, chromatography, bio_standards, path, sequence, metadata, qc_config_id, job_type):

    """
    Initializes sample records in database for a new QC job.

    Performs the following functions:
        1. Inserts a record for the new instrument run into the "runs" table
        2. Inserts sample rows into the "sample_qc_results" table
        3. Inserts biological standard sample rows into the "bio_qc_results" table

    Args:
        run_id (str):
            Instrument run ID (job ID)
        instrument_id (str):
            Instrument ID
        chromatography (str):
            Chromatography method
        bio_standards (str):
            Biological standards
        path (str):
            Data acquisition path
        sequence (str):
            Acquisition sequence table, as JSON string in "records" format
        metadata (str):
            Sample metadata table, as JSON string in "records" format
        qc_config_id (str):
            Name of QC configuration
        job_type (str):
            Either "completed" or "active"

    Returns:
        None
    """

    # Get list of samples from sequence
    df_sequence = get_filenames_from_sequence(sequence)

    samples = df_sequence["File Name"].astype(str).tolist()
    polarities = df_sequence["Polarity"].astype(str).tolist()
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
                 "run_id": run_id,
                 "polarity": polarities[index],
                 "position": positions[index]})

        # Prepare insert of the sample row into the "bio_qc_results" table
        else:
            insert_sample = bio_qc_results_table.insert().values(
                {"sample_id": sample,
                 "run_id": run_id,
                 "polarity": polarities[index],
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
    Returns DataFrame of given instrument run from "runs" table.

    Args:
        instrument_id (str): Instrument ID
        run_id (str): Run ID

    Returns:
        DataFrame containing record for instrument run
    """

    database = get_database_file(instrument_id=instrument_id, sqlite_conn=True)
    engine = sa.create_engine(database)
    query = "SELECT * FROM runs WHERE run_id = '" + run_id + "'"
    df_instrument_run = pd.read_sql(query, engine)
    return df_instrument_run


def get_instrument_run_from_csv(instrument_id, run_id):

    """
    Returns DataFrame of selected instrument run from CSV files during active instrument runs.

    This function is called when a user views an active instrument run from an external device
    (to prevent downloading / uploading the database file with each sample acquisition).

    Args:
        instrument_id (str): Instrument ID
        run_id (str): Run ID

    Returns:
        DataFrame containing record for instrument run
    """

    id = instrument_id.replace(" ", "_") + "_" + run_id
    run_csv_file = os.path.join(data_directory, id, "csv", "run.csv")
    return pd.read_csv(run_csv_file, index_col=False)


def get_instrument_runs(instrument_id, as_list=False):

    """
    Returns DataFrame of all runs on a given instrument from "runs" table

    Args:
        instrument_id (str):
            Instrument ID
        as_list (str, default False):
            If True, returns only a list of names of instrument runs (jobs)

    Returns:
        DataFrame containing records for instrument runs (QC jobs) for the given instrument
    """

    database = get_database_file(instrument_id, sqlite_conn=True)
    engine = sa.create_engine(database)
    df = pd.read_sql("SELECT * FROM runs", engine)

    if as_list:
        return df["run_id"].astype(str).tolist()
    else:
        return df


def delete_instrument_run(instrument_id, run_id):

    """
    Deletes all records for an instrument run (QC job) from the database.

    Args:
        instrument_id (str): Instrument ID
        run_id (str): Run ID

    Returns:
        None
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
            sa.delete(table).where(table.c.run_id == run_id)
        ))

    # Close the connection
    connection.close()


def get_acquisition_path(instrument_id, run_id):

    """
    Retrieves acquisition path for a given instrument run.

    Args:
        instrument_id (str): Instrument ID
        run_id (str): Run ID

    Returns:
        Acquisition path for the given instrument run
    """

    return get_instrument_run(instrument_id, run_id)["acquisition_path"].astype(str).tolist()[0]


def get_md5(instrument_id, sample_id):

    """
    Returns MD5 checksum for a data file in "sample_qc_results" table.

    Used for comparing MD5 checksums during active instrument runs.

    TODO: This function will return incorrect results if two different instrument runs
        have samples with the same sample ID. It needs to include "run_id" in the SQL query.

    Args:
        instrument_id (str): Instrument ID
        sample_id (str): Sample ID

    Returns:
        MD5 checksum stored for the data file.
    """

    # Connect to database
    database = get_database_file(instrument_id, sqlite_conn=True)
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
    Updates MD5 checksum for a data file during sample acquisition.

    TODO: This function will return incorrect results if two different instrument runs
        have samples with the same sample ID. It needs to include "run_id" in the SQL query.

    Args:
        instrument_id (str):
            Instrument ID
        sample_id (str):
            Sample ID (filename) of data file
        md5_checksum (str):
            MD5 checksum for the sample data file

    Returns:
        None
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
    Writes QC results (as dictionary records) to sample record upon MS-DIAL processing completion.

    QC results consist of m/z, RT, and intensity data for internal standards (or targeted metabolites in biological standards),
    as well as a DataFrame containing delta m/z, delta RT, in-run delta RT, warnings, and fails (qc_dataframe) and overall QC result
    (which will be "Pass" or "Fail").

    The data is encoded as dictionary in "records" format: [{'col1': 1, 'col2': 0.5}, {'col1': 2, 'col2': 0.75}].
    This dictionary is cast to a string before being passed to this function.

    TODO: Update names of arguments from json_x to record_x, as the data is no longer encoded as JSON strings.
        The data is now encoded in "records" format as a string.

    Args:
        sample_id (str):
            Sample ID
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (Job ID)
        json_mz (str):
            String dict of internal standard m/z data in "records" format
        json_rt (str):
            String dict of internal standard RT data in "records" format
        json_intensity (str):
            String dict of internal standard intensity data in "records" format
        qc_dataframe (str):
            String dict of various QC data in "records" format
        qc_result (str):
            QC result for sample, either "Pass" or "Fail"
        is_bio_standard (bool):
            Whether the sample is a biological standard

    Returns:
        None
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
    Returns DataFrame of chromatography methods from the Settings database.
    """

    engine = sa.create_engine(settings_database)
    df_methods = pd.read_sql("SELECT * FROM chromatography_methods", engine)
    return df_methods


def get_chromatography_methods_list():

    """
    Returns list of chromatography method ID's from the Settings database.
    """

    df_methods = get_chromatography_methods()
    return df_methods["method_id"].astype(str).tolist()


def insert_chromatography_method(method_id):

    """
    Inserts new chromatography method in the "chromatography_methods" table of the Settings database.

    Args:
        method_id (str): Name of the chromatography method

    Returns:
        None
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
    Deletes chromatography method and all associated records from the Settings database.

    Details:
        1. Removes chromatography method in "chromatography_methods" table
        2. Removes method from "biological_standards" table
        3. Removes associated internal standards from "internal_standards" table
        4. Removes associated targeted features from "targeted_features" table
        5. Deletes corresponding MSPs from folders
        6. Deletes corresponding MSPs from Google Drive (if sync is enabled)

    Args:
        method_id (str): Name of the chromatography method

    Returns:
        None
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
    Updates MS-DIAL configuration for a given chromatography method.

    This MS-DIAL configuration will be used to generate a parameters file
    for processing samples run with this chromatography method.

    Args:
        chromatography (str):
            Chromatography method ID (name)
        config_id (str):
            MS-DIAL configuration ID (name)

    Returns:
        None
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
    Parses compounds from MSP into the Settings database.

    This function writes features from an MSP file into the "internal_standards" or "targeted_features" table,
    and inserts location of pos/neg MSP files into "chromatography_methods" table.

    TODO: The MSP/TXT libraries have standardized names; there is no need to store the filename in the database.

    Args:
        msp_file (io.StringIO):
            In-memory text-stream file object
        chromatography (str):
            Chromatography method ID (name)
        polarity (str):
            Polarity for which MSP should be used for ("Positive Mode" or "Negative Mode")
        bio_standard (str, default None):
            Parses MSP and applies to biological standard within a chromatography-polarity combination

    Returns:
        None
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
    Parses compounds from a CSV file into the Settings database.

    Parses compounds from a CSV into the "internal_standards" table, and stores
    the location of the pos/neg TXT files in "chromatography_methods" table.

    TODO: The MSP/TXT libraries have standardized names; there is no need to store the filename in the database.

    Args:
        csv_file (io.StringIO):
            In-memory text-stream file object
        chromatography (str):
            Chromatography method ID (name)
        polarity (str):
            Polarity for which MSP should be used for ("Positive Mode" or "Negative Mode")

    Returns:
        None
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
    Returns list of user configurations of MS-DIAL parameters from Settings database.
    """

    engine = sa.create_engine(settings_database)
    df_msdial_configurations = pd.read_sql("SELECT * FROM msdial_parameters", engine)
    return df_msdial_configurations["config_name"].astype(str).tolist()


def generate_msdial_parameters_file(chromatography, polarity, msp_file_path, bio_standard=None):

    """
    Uses parameters from user-curated MS-DIAL configuration to create a parameters.txt file for MS-DIAL.

    TODO: Currently, this function is only called upon a new job setup. To allow changes during a QC job,
        this function should be called every time the user makes a configuration save in Settings > MS-DIAL Configurations.

    Args:
        chromatography (str):
            Chromatography method ID (name)
        polarity (str):
            Polarity ("Positive" or "Negative")
        msp_file_path (str):
            MSP library file path
        bio_standard (str, default None):
            Specifies that the parameters file is for a biological standard

    Returns:
        None
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
    Inserts new user configuration of MS-DIAL parameters into the "msdial_parameters" table in Settings database.

    Args:
        msdial_config_name (str): MS-DIAL configuration ID

    Returns:
        None
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
    Deletes user configuration of MS-DIAL parameters from the "msdial_parameters" table.

    Args:
        msdial_config_name (str): MS-DIAL configuration ID

    Returns:
        None
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


def get_msdial_configuration_parameters(msdial_config_name, parameter=None):

    """
    Returns tuple of parameters defined for a selected MS-DIAL configuration.

    TODO: The MS-DIAL configuration is returned as a tuple for a concise implementation of get_msdial_parameters_for_config()
        in the DashWebApp module. While convenient there, this function is not optimal for maintainability. Should return
        the entire DataFrame record instead.

    See update_msdial_configuration() for details on parameters.

    Args:
        msdial_config_name (str):
            MS-DIAL configuration ID
        parameter (str, default None):
            If specified, returns only the value for the given parameter

    Returns:
        Tuple of parameters for the given MS-DIAL configuration, or single parameter value.
    """

    # Get "msdial_parameters" table from database as a DataFrame
    engine = sa.create_engine(settings_database)
    df_configurations = pd.read_sql("SELECT * FROM msdial_parameters", engine)

    # Get selected configuration
    selected_config = df_configurations.loc[
        df_configurations["config_name"] == msdial_config_name]

    selected_config.drop(["id", "config_name"], inplace=True, axis=1)

    if parameter is not None:
        return selected_config[parameter].values[0]
    else:
        return tuple(selected_config.to_records(index=False)[0])


def update_msdial_configuration(config_name, rt_begin, rt_end, mz_begin, mz_end, ms1_centroid_tolerance,
    ms2_centroid_tolerance, smoothing_method, smoothing_level, mass_slice_width, min_peak_width, min_peak_height,
    post_id_rt_tolerance, post_id_mz_tolerance, post_id_score_cutoff, alignment_rt_tolerance, alignment_mz_tolerance,
    alignment_rt_factor, alignment_mz_factor, peak_count_filter, qc_at_least_filter):

    """
    Updates and saves changes of all parameters for a selected MS-DIAL configuration.

    For details on MS-DIAL parameters, see: https://mtbinfo-team.github.io/mtbinfo.github.io/MS-DIAL/tutorial#section-2-3

    Args:
        config_name (str):
            Name / ID of MS-DIAL configuration
        rt_begin (int):
            Minimum retention time in RT range for analysis range
        rt_end (int):
            Maximum retention time in RT range for analysis
        mz_begin (float):
            Minimum precursor mass in m/z range for analysis range
        mz_end (float):
            Maximum precursor mass in m/z range for analysis range
        ms1_centroid_tolerance (float):
            MS1 centroid tolerance
        ms2_centroid_tolerance (float):
            MS2 centroid tolerance
        smoothing_method (str):
            Peak smoothing method for peak detection
        smoothing_level (int):
            Peak smoothing level
        mass_slice_width (float):
            Mass slice width
        min_peak_width (int):
            Minimum peak width threshold
        min_peak_height (int):
            Minimum peak height threshold
        post_id_rt_tolerance (float):
            Post-identification retention time tolerance
        post_id_mz_tolerance (float):
            Post-identification precursor m/z tolerance
        post_id_score_cutoff (int):
            Similarity score cutoff after peak identification
        alignment_rt_tolerance (float):
            Post-alignment retention time tolerance
        alignment_mz_tolerance (float):
            Post-alignment precursor m/z tolerance
        alignment_rt_factor (float):
            Post-alignment retention time factor
        alignment_mz_factor (float):
            Post-alignment precursor m/z tolerance
        peak_count_filter (int):
            Peak count filter
        qc_at_least_filter (str):
            QC at least filter

    Returns:
        None
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
    in the methods folder upon user upload) for MS-DIAL parameter file generation.

    TODO: Once added to workspace, MSP / TXT library file names are standardized. No need to store / retrieve from database.
        Get the file path using the filename e.g. return directory + chromatography + "_" + polarity + ".msp".

    Args:
        chromatography (str):
            Chromatography method ID
        polarity (str):
            Polarity, either "Positive" or "Negative"
        bio_standard (str, default None):
            Name of biological standard

    Returns:
        MSP / TXT library file path.
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
    Returns file path of parameters file stored in database.

    TODO: Once generated, MS-DIAL parameter filenames are standardized. No need to store / retrieve from database.
        Get the file path using the filename e.g. return directory + chromatography + "_" + polarity + "_Parameters.txt".

    Args:
        chromatography (str):
            Chromatography method ID
        polarity (str):
            Polarity, either "Positive" or "Negative"
        bio_standard (str, default None):
            Name of biological standard

    Returns:
        File path for MS-DIAL parameters.txt file.
    """

    engine = sa.create_engine(settings_database)

    if biological_standard is not None:
        query = "SELECT * FROM biological_standards WHERE chromatography='" + chromatography + \
                "' AND name ='" + biological_standard + "'"
    else:
        query = "SELECT * FROM chromatography_methods WHERE method_id='" + chromatography + "'"

    df = pd.read_sql(query, engine)

    if polarity == "Pos":
        parameter_file = df["pos_parameter_file"].astype(str).values[0]
    elif polarity == "Neg":
        parameter_file = df["neg_parameter_file"].astype(str).values[0]

    return parameter_file


def get_msdial_directory():

    """
    Returns location of MS-DIAL directory.
    """

    return get_table("Settings", "workspace")["msdial_directory"].astype(str).values[0]


def get_msconvert_directory():

    """
    Returns location of MSConvert directory.

    This function uses the MS-DIAL directory path to retrieve user ID, which it then uses to
    retrieve the path for MSConvert.exe in C:/Users/<username>/AppData/Local/Apps.

    TODO: There is probably a better way to implement this.

    Returns:
        Location of MSConvert directory in C:/Users/<username>/AppData/Local/Apps/ProteoWizard.
    """

    user = get_msdial_directory().replace("\\", "/").split("/")[2]
    msconvert_folder = [f.path for f in os.scandir("C:/Users/" + user + "/AppData/Local/Apps/") if f.is_dir() and "ProteoWizard" in f.name][0]
    return msconvert_folder


def update_msdial_directory(msdial_directory):

    """
    Updates location of MS-DIAL directory, stored in "workspace" table of the Settings database.

    Args:
        msdial_directory (str): New MS-DIAL directory location

    Returns:
        None
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
    Returns dictionary of internal standard keys mapped to either m/z or RT values.

    This function is used to establish a y-axis range for internal standard retention time plots.
    See load_istd_rt_plot() in the PlotGeneration module.

    TODO: This function needs to filter for polarity!

    Args:
        chromatography (str):
            Chromatography method to retrieve internal standards for
        value_type (str):
            Data type ("precursor_mz", "retention_time", "ms2_spectrum")

    Returns:
        Dictionary with key-value pairs of { internal_standard: value_type }
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
    Returns DataFrame of internal standards for a given chromatography method and polarity.

    Args:
        chromatography (str):
            Chromatography method ID
        polarity (str):
            Polarity (either "Pos" or "Neg")

    Returns:
        DataFrame of "internal_standards" table from Settings database, filtered by chromatography and polarity.
    """

    if polarity == "Pos":
        polarity = "Positive Mode"
    elif polarity == "Neg":
        polarity = "Negative Mode"

    engine = sa.create_engine(settings_database)

    query = "SELECT * FROM internal_standards " + \
            "WHERE chromatography='" + chromatography + "' AND polarity='" + polarity + "'"

    return pd.read_sql(query, engine)


def get_targeted_features(biological_standard, chromatography, polarity):

    """
    Returns DataFrame of metabolite targets for a given biological standard, chromatography, and polarity.

    Args:
        biological_standard (str):
            Name of biological standard
        chromatography (str):
            Chromatography method ID (name)
        polarity (str):
            Polarity (either "Pos" or "Neg")

    Returns:
        DataFrame of "targeted_features" table from Settings database, filtered by chromatography and polarity.
    """

    if polarity == "Pos":
        polarity = "Positive Mode"
    elif polarity == "Neg":
        polarity = "Negative Mode"

    engine = sa.create_engine(settings_database)

    query = "SELECT * FROM targeted_features " + \
            "WHERE chromatography='" + chromatography + \
            "' AND polarity='" + polarity + \
            "' AND biological_standard ='" + biological_standard + "'"

    return pd.read_sql(query, engine)


def get_biological_standards():

    """
    Returns DataFrame of the "biological_standards" table from the Settings database.
    """

    # Get table from database as a DataFrame
    engine = sa.create_engine(settings_database)
    df_biological_standards = pd.read_sql("SELECT * FROM biological_standards", engine)
    return df_biological_standards


def get_biological_standards_list():

    """
    Returns list of biological standards from the Settings database.
    """

    df_biological_standards = get_biological_standards()
    return df_biological_standards["name"].astype(str).unique().tolist()


def add_biological_standard(name, identifier):

    """
    Creates new biological standard with name and identifier.

    The biological standard identifier is a text substring used to distinguish between sample and biological standard.
    MS-AutoQC checks filenames in the sequence for this identifier to process samples accordingly.

    Args:
        name (str):
            Name of biological standard
        identifier (str):
            String identifier in filename for biological standard

    Returns:
        None
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
    Deletes biological standard and corresponding MSPs from Settings database.

    Args:
        name (str): Name of the biological standard

    Returns:
        None
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
    Updates MS-DIAL configuration for given biological standard and chromatography method combination.

    Args:
        biological_standard (str):
            Name of the biological standard
        chromatography (str):
            Chromatography method
        config_id (str):
            Name of MS-DIAL configuration to set for this biological standard - chromatography combination

    Returns:
        None
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
    Returns dictionary of identifiers for a given list of biological standards.

    If no list is provided, returns dict of identifiers for all biological standards.

    Args:
        bio_standards (list, default None): List of biological standards

    Returns:
        Dictionary with key-value pairs of { identifier: biological_standard }
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
    Returns DataFrame of "qc_parameters" table from Settings database.
    """

    engine = sa.create_engine(settings_database)
    return pd.read_sql("SELECT * FROM qc_parameters", engine)


def get_qc_configurations_list():

    """
    Returns list of names of QC configurations from Settings database.
    """

    return get_qc_configurations()["config_name"].astype(str).tolist()


def add_qc_configuration(qc_config_name):

    """
    Adds a new QC configuration to the "qc_parameters" table in the Settings database.

    Args:
        qc_config_name (str): Name of the QC configuration

    Returns:
        None
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
    Deletes QC configuration from the "qc_parameters" table in the Settings database.

    Args:
        qc_config_name (str): Name of the QC configuration

    Returns:
        None
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
    Returns DataFrame of parameters for a selected QC configuration.

    The DataFrame has columns for each parameter, as well as for whether the parameter is enabled.

    Args:
        config_name (str, default None):
            Name of QC configuration
        instrument_id (str, default None):
            Instrument ID (name)
        run_id (str, default None):
            Instrument run ID (job ID)

    Returns:
        DataFrame of parameters for QC configuration.
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
    Updates parameters for the given QC configuration.

    Due to the database schema, booleans are stored as integers: 0 for False and 1 for True. They need to be
    cast back to booleans in get_qc_configuration_parameters(). A schema change would remove the bloat.

    Args:
        config_name (str):
            Name of QC configuration
        intensity_dropouts_cutoff (int):
            Minimum number of internal standard intensity dropouts to constitute a QC fail
        library_rt_shift_cutoff (float):
            Maximum shift from library RT values to constitute a QC fail
        in_run_rt_shift_cutoff (float):
            Maximum shift from in-run RT values to constitute a QC fail
        library_mz_shift_cutoff (float):
            Maximum shift from library m/z values to constitute a QC fail
        intensity_enabled (bool):
            Enables / disables QC check for intensity dropout cutoffs
        library_rt_enabled (bool):
            Enables / disables QC check for library RT shifts
        in_run_rt_enabled (bool):
            Enables / disables QC check for in-run RT shifts
        library_mz_enabled (bool):
            Enables / disables QC check for library m/z shifts

    Returns:
        None
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
    Returns DataFrame of samples for a given instrument run from instrument database.

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)
        sample_type (str):
            Sample type, either "Sample" or "Biological Standard" or "Both"

    Returns:
        DataFrame of sample tables for a given instrument run.
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
    Returns DataFrame of samples in a given run using CSV files from Google Drive.

    CSV files of the run metadata, samples, and biological standards tables are stored
    in the ../data/Instrument_ID_Run_ID/csv directory, and removed on job completion.

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)
        sample_type (str):
            Sample type, either "Sample" or "Biological Standard" or "Both"

    Returns:
        DataFrame of samples for a given instrument run.
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

    df = df.loc[df["run_id"] == run_id]

    try:
        df.drop(columns=["id"], inplace=True)
    finally:
        return df


def get_next_sample(sample_id, instrument_id, run_id):

    """
    Returns sample following the given sample, or None if last sample.

    Args:
        sample_id (str):
            Sample ID
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)

    Returns:
        str: The next sample in the instrument run after the given sample ID, or None if last sample.
    """

    # Get list of samples in run
    samples = get_samples_in_run(instrument_id, run_id, "Both")["sample_id"].astype(str).tolist()

    # Find sample in list
    sample_index = samples.index(sample_id)
    next_sample_index = sample_index + 1

    # Return next sample
    if next_sample_index != len(samples):
        return samples[next_sample_index]
    else:
        return None


def get_remaining_samples(instrument_id, run_id):

    """
    Returns list of samples remaining in a given instrument run (QC job).

    TODO: This function should just return the samples with null values in the "qc_result" column.
        The "latest_sample" value in the "runs" table may be unreliable.

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)

    Returns:
        list: List of samples remaining in a QC job.
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
    For an active run, returns 1) a list of samples that were not processed due to error / runtime termination,
    and 2) the current sample being monitored / processed.

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)

    Returns:
        tuple: List of unprocessed samples for the given instrument run, and current sample being monitored / processed.
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
    if len(unprocessed_samples) > 0:
        current_sample = unprocessed_samples[-1]
        del unprocessed_samples[-1]
    else:
        current_sample = None

    # Return as tuple
    return unprocessed_samples, current_sample


def get_current_sample(instrument_id, run_id):

    """
    Returns the current sample being monitored / processed.

    TODO: The "latest_sample" is the last sample to be processed. Nomenclature needs to be updated in many places.

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)

    Returns:
        str: Current sample being monitored / processed.
    """

    # Get latest sample in run
    df_run = get_instrument_run(instrument_id, run_id)
    latest_sample = df_run["latest_sample"].astype(str).values[0]

    # Return second sample if beginning of run
    if latest_sample == "None":
        return samples[1]


def parse_internal_standard_data(instrument_id, run_id, result_type, polarity, load_from, as_json=True):

    """
    Parses data from database into JSON-ified DataFrame for samples (as rows) vs. internal standards (as columns).

    Data is stored in a column (for example, "retention_time") as a single-record string dict with the following structure:

    | Sample     | iSTD 1 | iSTD 2 | ... |
    | ---------- | ------ | ------ | ... |
    | SAMPLE_001 | 1.207  | 1.934  | ... |

    These records are concatenated together with this function using pd.DataFrame(), which is 100x faster than pd.concat().

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)
        result_type (str):
            Column in sample_qc_results table to parse (either "retention_time" or "precursor_mz" or "intensity")
        polarity (str):
            Polarity ("Pos" or "Neg")
        load_from (str):
            Specifies whether to load data from CSV file (during Google Drive sync of active run) or instrument database
        as_json (bool, default True):
            Whether to return table as JSON string or as DataFrame

    Returns:
        DataFrame of samples (rows) vs. internal standards (columns) as JSON string.
    """

    # Get relevant QC results table from database
    if load_from == "database" or load_from == "processing":
        df_samples = get_samples_in_run(instrument_id, run_id, "Sample")
    elif load_from == "csv":
        df_samples = get_samples_from_csv(instrument_id, run_id, "Sample")

    # Filter by polarity
    df_samples = df_samples.loc[df_samples["polarity"] == polarity]
    sample_ids = df_samples["sample_id"].astype(str).tolist()

    # Return None if results are None
    if load_from == "processing":
        if len(df_samples[result_type].dropna()) == 0:
            return None

    # Initialize DataFrame with individual records of sample data
    results = df_samples[result_type].astype(str).tolist()
    results = [ast.literal_eval(result) if result != "None" and result != "nan" else {} for result in results]
    df_results = pd.DataFrame(results)
    df_results.drop(columns=["Name"], inplace=True)
    df_results["Sample"] = sample_ids

    # Return DataFrame as JSON string
    if as_json:
        return df_results.to_json(orient="records")
    else:
        return df_results


def parse_biological_standard_data(instrument_id, run_id, result_type, polarity, biological_standard, load_from, as_json=True):

    """
    Parses biological standard data into JSON-ified DataFrame of targeted features (as columns) vs. instrument runs (as rows).

    The bio_qc_results table in the instrument database is first filtered by biological standard, chromatography, and polarity.
    Then, the sample name is replaced with the instrument run it was associated with.

    Data is stored in a column (for example, "intensity") as a single-record string dict with the following structure:

    | Name                | Metabolite 1 | Metabolite 2 | ... |
    | ------------------- | ------------ | ------------ | ... |
    | INSTRUMENT_RUN_001  | 13597340     | 53024853     | ... |

    These records are concatenated together with this function using pd.DataFrame(), which is 100x faster than pd.concat().

    | Name                | Metabolite 1 | Metabolite 2 | ... |
    | ------------------- | ------------ | ------------ | ... |
    | INSTRUMENT_RUN_001  | 13597340     | 53024853     | ... |
    | INSTRUMENT_RUN_002  | 23543246     | 102030406    | ... |
    | ...                 | ...          | ...          | ... |

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)
        result_type (str):
            Column in bio_qc_results table to parse (either "retention_time" or "precursor_mz" or "intensity")
        polarity (str):
            Polarity ("Pos" or "Neg")
        biological_standard (str):
            Name of biological standard
        load_from (str):
            Specifies whether to load data from CSV file (during Google Drive sync of active run) or instrument database
        as_json (bool, default True):
            Whether to return table as JSON string or as DataFrame

    Returns:
        JSON-ified DataFrame of targeted features for a biological standard (columns) vs. instrument runs (rows).
    """

    # Get relevant QC results table from database
    if load_from == "database":
        df_samples = get_table(instrument_id, "bio_qc_results")
    elif load_from == "csv":
        id = instrument_id.replace(" ", "_") + "_" + run_id
        bio_standards_csv = os.path.join(data_directory, id, "csv", "bio_standards.csv")
        df_samples = pd.read_csv(bio_standards_csv, index_col=False)

    # Filter by biological standard type
    df_samples = df_samples.loc[df_samples["biological_standard"] == biological_standard]

    # Filter by polarity
    df_samples = df_samples.loc[df_samples["polarity"] == polarity]

    # Filter by instrument
    df_runs = get_table(instrument_id, "runs")
    chromatography = df_runs.loc[df_runs["run_id"] == run_id]["chromatography"].values[0]

    # Filter by chromatography
    run_ids = df_runs.loc[df_runs["chromatography"] == chromatography]["run_id"].astype(str).tolist()
    df_samples = df_samples.loc[df_samples["run_id"].isin(run_ids)]
    run_ids = df_samples["run_id"].astype(str).tolist()

    # Initialize DataFrame with individual records of sample data
    results = df_samples[result_type].fillna('{}').tolist()
    results = [ast.literal_eval(result) if result != "None" and result != "nan" else {} for result in results]
    df_results = pd.DataFrame(results)
    df_results["Name"] = run_ids

    # Return DataFrame as JSON string
    if as_json:
        return df_results.to_json(orient="records")
    else:
        return df_results


def parse_internal_standard_qc_data(instrument_id, run_id, polarity, result_type, load_from, as_json=True):

    """
    Parses QC data into JSON-ified DataFrame for samples (as rows) vs. internal standards (as columns).

    The QC DataFrame is stored in the "qc_dataframe" column as a single-record string dict with the following structure:

    | Sample     | Delta m/z | Delta RT | In-run delta RT | Warnings | Fails |
    | ---------- | --------- | -------- | --------------- | -------- | ----- |
    | SAMPLE_001 | 0.000001  | 0.001    | 0.00001         | None     | None  |

    These records are concatenated together with this function using pd.DataFrame(), which is 100x faster than pd.concat().

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)
        polarity (str):
            Polarity ("Pos" or "Neg")
        result_type (str):
            Column in sample_qc_results table to parse (either "retention_time" or "precursor_mz" or "intensity")
        load_from (str):
            Specifies whether to load data from CSV file (during Google Drive sync of active run) or instrument database
        as_json (bool, default True):
            Whether to return table as JSON string or as DataFrame

    Returns:
        JSON-ified DataFrame of QC data for samples (as rows) vs. internal standards (as columns).
    """

    # Get relevant QC results table from database
    if load_from == "database" or load_from == "processing":
        df_samples = get_samples_in_run(instrument_id, run_id, "Sample")
    elif load_from == "csv":
        df_samples = get_samples_from_csv(instrument_id, run_id, "Sample")

    # Filter by polarity
    df_samples = df_samples.loc[df_samples["polarity"] == polarity]

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
    Returns a list of users that have access to the MS-AutoQC workspace.
    """

    return get_table("Settings", "gdrive_users")["email_address"].astype(str).tolist()


def add_user_to_workspace(email_address):

    """
    Gives user access to workspace in Google Drive and stores email address in database.

    Access is granted by sharing the MS-AutoQC folder in Google Drive with the user's Google account.

    Args:
        email_address (str): Email address for Google account to grant access to workspace.

    Returns:
        None
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
    Removes user access to workspace in Google Drive and deletes email from database.

    Args:
        email_address (str): Email address for Google account whose access will to be revoked.

    Returns:
        None
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
    Returns DataFrame of QC results for a given sample list.

    TODO: This function will break if samples in different runs have the same sample ID. Add run ID filter.

    Args:
        instrument_id (str):
            Instrument ID
        sample_list (list):
            List of samples to query
        is_bio_standard (bool, default False):
            Whether the list is biological standards (True) or samples (False)

    Returns:
        DataFrame of QC results for a given sample list.
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
    Creates record in "workspace" table to store various metadata.
    """

    db_metadata, connection = connect_to_database("Settings")
    workspace_table = sa.Table("workspace", db_metadata, autoload=True)
    connection.execute(workspace_table.insert().values({"id": 1}))
    connection.close()


def get_device_identity():

    """
    Returns device identity (either an Instrument ID or "Shared user").
    """

    return get_table("Settings", "workspace")["instrument_identity"].astype(str).tolist()[0]


def set_device_identity(is_instrument_computer, instrument_id):

    """
    Indicates whether the user's device is the instrument PC or not.

    Args:
        is_instrument_computer (bool):
            Whether the device is an instrument computer or not
        instrument_id (str):
            Instrument ID (if None, set to "Shared user")

    Returns:
        None
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
    Validates that the current device is the instrument PC on which the run was started.

    TODO: Use this function in PlotGeneration and DashWebApp module.

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID

    Returns:
        True if instrument run was started on the current device, and False if not.
    """

    instrument_id = get_instrument_run(instrument_id, run_id)["instrument_id"].astype(str).tolist()[0]
    device_identity = get_table("Settings", "workspace")["instrument_identity"].astype(str).tolist()[0]

    if instrument_id == device_identity:
        return True
    else:
        return False


def update_slack_bot_token(slack_bot_token):

    """
    Updates Slack bot user OAuth 2.0 token in "workspace" table of Settings database.

    For details on the Slack API, see: https://slack.dev/python-slack-sdk/

    Args:
        slack_bot_token (str): Slack bot user OAuth token

    Returns:
        None
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
    Returns Slack bot token stored in "workspace" table of Settings database.
    """

    return get_table("Settings", "workspace")["slack_bot_token"].astype(str).values[0]


def update_slack_channel(slack_channel, notifications_enabled):

    """
    Updates Slack channel registered for notifications in "workspace" table of Settings database.

    Args:
        slack_channel (str):
            Slack channel to post messages to
        notifications_enabled (bool):
            Whether to send Slack notifications for QC warnings and fails

    Returns:
        None
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
    Returns Slack channel registered for notifications.
    """

    return get_table("Settings", "workspace")["slack_channel"].astype(str).values[0]


def get_slack_notifications_toggled():

    """
    Returns Slack notification toggle setting.
    """

    try:
        return get_table("Settings", "workspace")["slack_enabled"].astype(int).tolist()[0]
    except:
        return None


def get_email_notifications_list(as_string=False):

    """
    Returns list of emails registered for email notifications for QC warnings and fails.

    Args:
        as_string (bool, default False):
            Whether to return the list as a string (for Gmail API) or as list object (for display in Settings page)

    Returns:
        List of emails registered for QC warning/fail notifications.
    """

    email_list = get_table("Settings", "email_notifications")["email_address"].astype(str).tolist()

    if as_string:
        email_list_string = ""

        for email in email_list:
            email_list_string += email
            if email != email_list[-1]:
                email_list_string += ","

        return email_list_string

    else:
        return email_list


def register_email_for_notifications(email_address):

    """
    Inserts email address into "email_notifications" table in Settings database.

    Args:
        email_address (str): Email address to register for notifications.

    Returns:
        None
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
    Deletes email address from "email_notifications" table in Settings database.

    Args:
        email_address (str): Email address to unsubscribe from notifications.

    Returns:
        None
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
    Returns tuple containing count for completed samples and total samples in a given instrument run.

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)
        status (str):
            Instrument run (QC job) status, either "Active" or "Complete"

    Returns:
        Tuple with number of completed samples and total samples for a given instrument run.
    """

    if status == "Active" and sync_is_enabled():
        if get_device_identity() == instrument_id:
            df_instrument_run = get_instrument_run(instrument_id, run_id)
        else:
            df_instrument_run = get_instrument_run_from_csv(instrument_id, run_id)
    else:
        df_instrument_run = get_instrument_run(instrument_id, run_id)

    completed = df_instrument_run["completed"].astype(int).tolist()[0]
    total_samples = df_instrument_run["samples"].astype(int).tolist()[0]
    return (completed, total_samples)


def get_run_progress(instrument_id, run_id, status):

    """
    Returns progress of instrument run as a percentage of samples completed.

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)
        status (str):
            Instrument run (QC job) status, either "Active" or "Complete"

    Returns:
        float: Percent of samples processed for the given instrument run.
    """

    completed, total_samples = get_completed_samples_count(instrument_id, run_id, status)
    percent_complete = (completed / total_samples) * 100
    return round(percent_complete, 1)


def update_sample_counters_for_run(instrument_id, run_id, latest_sample):

    """
    Increments "completed" count, as well as "pass" and "fail" counts accordingly.

    TODO: The "latest_sample" is the last sample to be processed / completed.
        Nomenclature should be updated for clarity.

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)
        latest_sample (str):
            Last sample to be processed

    Returns:
        None
    """

    df = get_samples_in_run(instrument_id, run_id, "Both")

    try:
        passes = int(df["qc_result"].value_counts()["Pass"])
    except:
        passes = 0

    try:
        warnings = int(df["qc_result"].value_counts()["Warning"])
    except:
        warnings = 0

    try:
        fails = int(df["qc_result"].value_counts()["Fail"])
    except:
        fails = 0

    completed = passes + fails

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
    Marks instrument run status as completed.

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)

    Returns:
        None
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
    Skips sample by setting "latest_sample" value for instrument run to the next sample.

    This function was used after restarting the acquisition listener when MS-DIAL got stuck processing a corrupted file.
    Now that MS-DIAL runs in the background, it is deprecated and should be removed.

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)

    Returns:
        None
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
    Stores acquisition listener subprocess ID to allow for checkup and termination.

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)
        pid (str):
            Process ID for acquisition listener subprocess

    Returns:
        None
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
    Retrieves acquisition listener process ID from "runs" table in Settings database.

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)

    Returns:
        None
    """

    try:
        return get_instrument_run(instrument_id, run_id)["pid"].astype(int).tolist()[0]
    except:
        return None


def upload_to_google_drive(file_dict):

    """
    Uploads files to MS-AutoQC folder in Google Drive.

    Args:
        file_dict (dict):
            Dictionary with key-value structure { filename : file path }

    Returns:
        dict: Dictionary with key-value structure { filename : Google Drive ID }
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
    Uploads QC results for a given instrument run to Google Drive as CSV files.

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)

    Returns:
        None
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
    Downloads CSV files of QC results from Google Drive and stores in /data directory.

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)

    Returns:
        tuple: Paths of run.csv, samples.csv, and bio_standards.csv, respectively.
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
    Returns Google Drive ID for the MS-AutoQC folder (found in user's root Drive directory).
    """

    return get_table("Settings", "workspace")["gdrive_folder_id"].values[0]


def get_database_drive_id(instrument_id):

    """
    Returns Google Drive ID for a given instrument's database.

    Args:
        instrument_id (str): Instrument ID

    Returns:
        str: Google Drive ID for the instrument database ZIP archive.
    """

    df = get_table("Settings", "instruments")
    return df.loc[df["name"] == instrument_id]["drive_id"].values[0]


def upload_database(instrument_id, sync_settings=False):

    """
    Uploads database file and methods directory to Google Drive as ZIP archives.

    Args:
        instrument_id (str):
            Instrument ID for the instrument database to upload
        sync_settings (bool, default False):
            Whether to upload methods directory as well

    Returns:
        str: Timestamp upon upload completion.
    """

    # Get Google Drive ID's for the MS-AutoQC folder and database file
    gdrive_folder_id = get_drive_folder_id()
    instrument_db_file_id = get_database_drive_id(instrument_id)

    # Get Google Drive instance
    drive = get_drive_instance()

    # Vacuum database to optimize size
    execute_vacuum(instrument_id)

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

    return time.strftime("%H:%M:%S")


def download_database(instrument_id, sync_settings=False):

    """
    Downloads instrument database ZIP file from Google Drive.

    This function is called when accessing an instrument database from a device other than the given instrument.

    Args:
        instrument_id (str):
            Instrument ID for the instrument database to download
        sync_settings (bool, default False):
            Whether to download methods directory as well

    Returns:
        str: Timestamp upon download completion.
    """

    db_zip_file = instrument_id.replace(" ", "_") + ".zip"

    # If the database was not modified by another instrument, skip download (for instruments only)
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
    Uploads methods directory ZIP archive to Google Drive.
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
    Downloads methods directory ZIP archive from Google Drive.

    Args:
        skip_check (bool, default False): If True, skips checking whether database was modified

    Returns:
        None
    """

    # If the database was not modified by another instrument, skip download (for instruments only)
    if not skip_check:
        if not database_was_modified("Settings"):
            return None

    # Get device identity
    instrument_bool = is_instrument_computer()
    device_identity = get_device_identity()

    # Get MS-DIAL directory
    try:
        msdial_directory = get_msdial_directory()
    except:
        msdial_directory = None

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

    # Update MS-DIAL directory
    update_msdial_directory(msdial_directory)

    # Update user device identity
    set_device_identity(is_instrument_computer=instrument_bool, instrument_id=device_identity)
    return time.strftime("%H:%M:%S")


def remember_last_modified(database, modified_date):

    """
    Stores last modified time of database file in Google Drive.

    This function is called after file upload, and used for comparison before download.

    Args:
        database (str):
            Name of database (either Instrument ID or "Settings")
        modified_date (str):
            Modified date of file uploaded to Google Drive

    Returns:
        None
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
    Returns True if workspace file was modified by another instrument PC in Google Drive, and False if not.

    Args:
        database_name (str): Name of database

    Returns:
        Returns True if workspace file was modified by another instrument PC in Google Drive, and False if not.
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
    Uploads empty file to signal that an instrument PC is syncing to Google Drive.

    TODO: This method is deprecated. Please remove if no plans for usage.

    Args:
        folder_id (str): Google Drive folder ID

    Returns:
        bool: True if sync signal was sent, False if not.
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
    Returns False if another device is currently uploading to Google Drive, else True.

    TODO: This method is deprecated. Please remove if no plans for usage.

    Args:
        folder_id (str): Google Drive folder ID

    Returns:
        bool: False if another device is currently uploading to Google Drive, True if not.
    """

    # Get Google Drive instance
    drive = get_drive_instance()

    for file in drive.ListFile({"q": "'" + folder_id + "' in parents and trashed=false"}).GetList():
        if file["title"] == "Syncing":
            return False

    return True


def remove_sync_signal(folder_id):

    """
    Removes empty signal file to signal that an instrument PC has completed syncing to Google Drive.

    TODO: This method is deprecated. Please remove if no plans for usage.

    Args:
        folder_id (str): Google Drive folder ID

    Returns:
        bool: True if sync signal was removed, False if not.
    """

    # Get Google Drive instance
    drive = get_drive_instance()

    try:
        for file in drive.ListFile({"q": "'" + folder_id + "' in parents and trashed=false"}).GetList():
            if file["title"] == "Syncing":
                file.Delete()
        return True
    except:
        return False


def delete_active_run_csv_files(instrument_id, run_id):

    """
    Checks for and deletes CSV files from Google Drive at the end of an active instrument run.

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)

    Returns:
        None
    """

    id = instrument_id.replace(" ", "_") + "_" + run_id

    # Find zip archive of CSV files in Google Drive and delete it
    drive = get_drive_instance()
    gdrive_folder_id = get_drive_folder_id()

    if gdrive_folder_id is not None:
        drive_file_list = drive.ListFile({"q": "'" + gdrive_folder_id + "' in parents and trashed=false"}).GetList()
        for file in drive_file_list:
            if file["title"] == id + ".zip":
                file.Delete()
                break

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

    Performs the following actions:
        1. Upload database to Google Drive
        2. Delete active run CSV files

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)

    Returns:
        None
    """

    # Get Google Drive instance and folder ID
    drive = get_drive_instance()
    gdrive_folder_id = get_drive_folder_id()

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
    Returns expected data file extension based on instrument vendor type.

    TODO: Modify this function as needed when adding support for other instrument vendors.

    Args:
        instrument_id (str): Instrument ID

    Returns:
        Data file extension for instrument vendor.
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
    Returns True if the given QC job is for a completed run, and False if for an active run.

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)

    Returns:
        bool: True if the job is for a completed run, and False if job is for an active run.
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
    Deletes temporary data file directory in local app directory.

    This function is called at the end of an instrument run (QC job).

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)

    Returns:
        None
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
    Validates that MSConvert and MS-DIAL dependencies are installed.

    This function is called during job setup validation.

    Args:
        module (str, default None): If specified, only validates given module.

    Returns:
        bool: Whether MSConvert.exe and MsdialConsoleApp.exe exist.
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


def send_email(subject, message_body):

    """
    Sends email using Google authenticated credentials.

    This function is called for QC warnings and fails if:
        1. Google Drive sync is enabled
        2. Email addresses are registered for notifications

    Args:
        subject (str):
            Subject of email
        message_body (str):
            Body of email

    Returns:
        On success, an email.message.EmailMessage object.
    """

    try:
        credentials = google_auth.load_credentials_from_file(alt_credentials)[0]

        service = build("gmail", "v1", credentials=credentials)
        message = EmailMessage()

        message.set_content(message_body)

        message["Subject"] = subject
        message["To"] = get_email_notifications_list(as_string=True)

        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        create_message = { "raw": encoded_message }

        send_message = (service.users().messages().send(userId="me", body=create_message).execute())

    except Exception as error:
        traceback.print_exc()
        send_message = None

    return send_message