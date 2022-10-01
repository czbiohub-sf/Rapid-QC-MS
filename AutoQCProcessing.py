import os, time
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

    # Convert DataFrames to JSON strings
    sequence = df_sequence.to_json(orient="split")
    return sequence


def convert_metadata_to_json(metadata_contents):

    """
    Converts sequence and metadata files to JSON strings for database storage
    """

    # Select columns from metadata
    df_metadata = pd.read_csv(metadata_contents, index_col=False)

    # Convert DataFrames to JSON strings
    metadata = df_metadata.to_json(orient="split")
    return metadata


def get_filenames_from_sequence(sequence):

    """
    Takes sequence file as JSON string and returns list of filenames
    """

    df_sequence = pd.read_json(sequence, orient="split")
    samples = df_sequence["File Name"].astype(str).tolist()
    return samples


def run_msconvert(path, filename, msconvert_output_folder):

    """
    Converts data files in closed vendor format to open mzML format
    """

    # Run MSConvert Docker container and allow 10 seconds for conversion
    command = "docker run --rm -e WINEDEBUG=-all -v " \
            + path.replace(" ", "\ ") \
            + ":/data chambm/pwiz-skyline-i-agree-to-the-vendor-licenses wine msconvert /data/" + filename

    os.system(command)
    time.sleep(10)

    mzml_file_path = path + filename.split(".")[0] + ".mzml"
    print(mzml_file_path)
    return mzml_file_path


def run_msdial_processing(msdial_path, msdial_parameter_file, msdial_output_folder):

    """
    Processes data files using MS-DIAL command line tools
    """

    os.chdir(msdial_path)

    command = "./MsdialConsoleApp lcmsdda -i '" + msdial_output_folder \
              + "' -o '" + msdial_output_folder \
              + "' -m '" + msdial_parameter_file + "' -p"

    os.system(command)


def autoqc_sample(sample_peak_list, standards_msp):

    """
    Returns DataFrames with m/z, RT, and intensity info for each internal standard
    """

    # df_standards = pd.read_csv(standards_msp, sep='\t', engine='python', skip_blank_lines=True)
    df_peak_list = pd.read_csv(sample_peak_list, sep='\t', engine='python', skip_blank_lines=True)

    print(df_peak_list)


def process_data_file(filename, path, run_id, is_bio_standard):

    """
    1. Convert data file to mzML format using MSConvert
    2. Process data file using MS-DIAL and user-defined parameter configuration
    3. Write QC results to "sample_qc_results" table in SQLite database
    4. Upload CSV file with QC results (as JSON) to Google Drive
    """

    output_folder = "/" + run_id

    # TODO: Get these from the database
    msdial_location = ""
    msdial_parameters = ""

    # TODO: Run MSConvert
    mzml_file = run_msconvert(path, filename, output_folder)

    # TODO: Run MS-DIAL
    peak_list = run_msdial_processing(mzml_file, msdial_location, msdial_parameters, output_folder)

    # TODO: Get m/z, RT, and intensity dataframes
    if not is_bio_standard:
        qc_results = autoqc_sample()
    else:
        qc_results = autoqc_bio_standard()

    # TODO: Write QC results to database and upload to Google Drive
    return qc_results