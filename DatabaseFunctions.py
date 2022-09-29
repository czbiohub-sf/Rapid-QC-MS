import sqlalchemy as sa

def insert_new_run(run_id, instrument_id, chromatography, sequence, metadata, msdial_config_id):

    """
    Inserts a new instrument run into database
    """

    # Connect to database
    engine = sa.create_engine('sqlite:///assets/QC Database.db')
    db_metadata = sa.MetaData(bind=engine)
    connection = engine.connect()

    # Get instrument runs table
    runs_table = sa.Table("runs", db_metadata, autoload=True)

    # Prepare insert of user-inputted run data
    run_insert = runs_table.insert().values(
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
         "msdial_config_id": msdial_config_id}
    )

    # Execute INSERT to database
    connection.execute(run_insert)