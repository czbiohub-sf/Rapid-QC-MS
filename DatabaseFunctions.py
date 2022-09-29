import pandas as pd
import sqlalchemy as sa
import AutoQCProcessing as qc

sqlite_db_location = "sqlite:///data/QC Database.db"

def get_instruments():

    """
    Returns list of instruments in database
    """

    # Connect to SQLite database
    engine = sa.create_engine("sqlite:///data/QC Database.db")

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
    engine = sa.create_engine(sqlite_db_location)
    db_metadata = sa.MetaData(bind=engine)
    connection = engine.connect()

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
    samples = qc.get_filenames_from_sequence(sequence)

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
    engine = sa.create_engine(sqlite_db_location)
    db_metadata = sa.MetaData(bind=engine)
    connection = engine.connect()

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