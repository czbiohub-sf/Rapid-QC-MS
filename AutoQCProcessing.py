import pandas as pd

def convert_sequence_to_json(sequence_contents, vendor_software="Thermo Xcalibur"):

    """
    Converts sequence table to JSON string for database storage
    """

    # Select columns from sequence using correct vendor software nomenclature
    if vendor_software == "Thermo Xcalibur":
        df_sequence = pd.read_csv(sequence_contents, index_col=False)
        df_sequence.columns = df_sequence.iloc[0]
        df_sequence = df_sequence.drop(df_sequence.index[0])
        df_sequence = df_sequence[["File Name", "Path", "Instrument Method", "Position", "Inj Vol", "L1 Study"]]

    # Convert DataFrames to JSON strings
    sequence = df_sequence.to_json(orient="split")
    return sequence


def convert_metadata_to_json(metadata_contents):

    """
    Converts sequence and metadata files to JSON strings for database storage
    """

    # Select columns from metadata
    df_metadata = pd.read_csv(metadata_contents, index_col=False)
    df_metadata = df_metadata[["Filename", "Name from collaborator", "Sample Name", "Species", "Matrix",
                               "Growth-Harvest Conditions", "Treatment"]]

    # Convert DataFrames to JSON strings
    metadata = df_metadata.to_json(orient="split")
    return metadata