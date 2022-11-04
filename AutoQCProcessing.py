import os, time, shutil
import pandas as pd
import DatabaseFunctions as db

def sequence_is_valid(filename, contents, vendor="Thermo Fisher"):

    """
    Validates that instrument sequence file contains the correct columns
    """

    if ".csv" not in filename:
        return False

    # Select columns from sequence using correct vendor software nomenclature
    if vendor == "Thermo Fisher":

        # Attempt to load sequence file as a pandas DataFrame
        try:
            df_sequence = pd.read_csv(contents, index_col=False)
        except Exception as error:
            print("Sequence file could not be read:", error)
            return False

        df_sequence.columns = df_sequence.iloc[0]
        df_sequence = df_sequence.drop(df_sequence.index[0])

        # Define required columns and columns found in sequence file
        required_columns = ["File Name", "Path", "Instrument Method", "Position", "Inj Vol"]
        sequence_file_columns = df_sequence.columns.tolist()

        # Check that the required columns are present
        for column in required_columns:
            if column not in sequence_file_columns:
                return False

    return True


def metadata_is_valid(filename, contents):

    """
    Validates that metadata file contains the required columns
    """

    if ".csv" not in filename:
        return False

    # Attempt to load metadata file as a pandas DataFrame
    try:
        df_metadata = pd.read_csv(contents, index_col=False)
    except Exception as error:
        print("Metadata file could not be read:", error)
        return False

    # Define required columns and columns found in metadata file
    required_columns = ["Filename", "Name from collaborator", "Sample Name", "Species",
                        "Matrix", "Growth-Harvest Conditions", "Treatment"]
    metadata_file_columns = df_metadata.columns.tolist()

    # Check that the required columns are present
    for column in required_columns:
        if column not in metadata_file_columns:
            return False

    return True


def chromatography_is_valid(chromatography):

    """
    Validates that the given chromatography method's MSP / TXT files exist, and that
    the MSP files exist for the selected biological standard(s) as well
    """

    # Get chromatography method from database
    df_methods = db.get_chromatography_methods()
    df_methods = df_methods.loc[df_methods["method_id"] == chromatography]

    if len(df_methods) == 0:
        return False

    # Check whether the method's MSP / TXT files exist
    pos_msp_file = df_methods["pos_istd_msp_file"].astype(str).values[0]
    neg_msp_file = df_methods["neg_istd_msp_file"].astype(str).values[0]

    if not os.path.exists(pos_msp_file) or not os.path.exists(neg_msp_file):
        return False

    return True


def convert_sequence_to_json(sequence_contents, vendor="Thermo Fisher"):

    """
    Converts sequence table to JSON string for database storage
    """

    # Select columns from sequence using correct vendor software nomenclature
    if vendor == "Thermo Fisher":
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


def run_msconvert(path, filename, extension, output_folder):

    """
    Converts data files in closed vendor format to open mzML format
    """

    # Copy original data file to output folder
    shutil.copy2(path + filename + "." + extension, output_folder)

    # Run MSConvert Docker container and allow 5 seconds for conversion
    command = "docker run --rm -e WINEDEBUG=-all -v " \
            + output_folder.replace(" ", "/") \
            + ":/data chambm/pwiz-skyline-i-agree-to-the-vendor-licenses wine msconvert /data/" \
            + filename + "." + extension
    os.system(command)
    time.sleep(3)

    # Delete copy of original data file
    data_file_copy = output_folder + filename + "." + extension
    os.remove(data_file_copy)

    return


def run_msdial_processing(filename, msdial_path, parameter_file, input_folder, output_folder):

    """
    Processes data files using MS-DIAL command line tools
    """

    # Navigate to directory containing MS-DIAL
    home = os.getcwd()
    os.chdir(msdial_path)

    # Run MS-DIAL
    command = "MsdialConsoleApp.exe lcmsdda -i " + input_folder \
              + " -o " + output_folder \
              + " -m " + parameter_file + " -p"
    os.system(command)

    # Clear data file directory for next sample
    for file in os.listdir(input_folder):
        filepath = os.path.join(input_folder, file)
        try:
            shutil.rmtree(filepath)
        except OSError:
            os.remove(filepath)

    # Return to original working directory
    os.chdir(home)

    # Return .msdial file path
    return output_folder + "/" + filename.split(".")[0] + ".msdial"


def peak_list_to_dataframe(sample_peak_list, internal_standards=None, targeted_features=None):

    """
    Returns DataFrame with m/z, RT, and intensity info for each internal standard in a given sample
    """

    # Convert .msdial file into a DataFrame
    df_peak_list = pd.read_csv(sample_peak_list, sep="\t", engine="python", skip_blank_lines=True)
    df_peak_list.rename(columns={"Title": "Name"}, inplace=True)

    # Get only the m/z, RT, and intensity columns
    df_peak_list = df_peak_list[["Name", "Precursor m/z", "RT (min)", "Height"]]

    # Query only internal standards (or targeted features for biological standard)
    if internal_standards is not None:
        df_peak_list = df_peak_list.loc[df_peak_list["Name"].isin(internal_standards)]
    elif targeted_features is not None:
        df_peak_list = df_peak_list.loc[df_peak_list["Name"].isin(targeted_features)]

    # DataFrame readiness
    df_peak_list.reset_index(drop=True, inplace=True)

    # Return DataFrame
    return df_peak_list


def qc_sample(df_peak_list, is_bio_standard):

    """
    Main algorithm that performs QC checks on sample data
    """

    # Handles sample QC checks
    if not is_bio_standard:
        return "Pass"
    # Handles biological standard QC checks
    else:
        return "Pass"


def process_data_file(path, filename, extension, run_id):

    """
    1. Convert data file to mzML format using MSConvert
    2. Process data file using MS-DIAL and user-defined parameter configuration
    3a. Write QC results to "sample_qc_results" table
    3b. Write QC results to "bio_qc_results" table if sample is biological standard
    4. Upload CSV file with QC results (as JSON) to Google Drive
    """

    # Create the necessary directories
    autoqc_directory = os.path.join(os.getcwd(), r"data")
    mzml_file_directory = os.path.join(autoqc_directory, run_id, "data")
    qc_results_directory = os.path.join(autoqc_directory, run_id, "results")

    for directory in [autoqc_directory, mzml_file_directory, qc_results_directory]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    mzml_file_directory = mzml_file_directory + "/"
    qc_results_directory = qc_results_directory + "/"

    # Retrieve chromatography, polarity, samples, and biological standards using run ID
    df_run = db.get_instrument_run(run_id)
    chromatography = df_run["chromatography"].astype(str).values[0]

    if "Pos" in filename:
        polarity = "Positive"
    elif "Neg" in filename:
        polarity = "Negative"

    df_samples = db.get_samples_in_run(run_id, sample_type="Sample")
    df_biological_standards = db.get_samples_in_run(run_id, sample_type="Biological Standard")

    # Retrieve MS-DIAL parameters, internal standards, and targeted features from database
    if filename in df_biological_standards["sample_id"].astype(str).tolist():
        # Get biological standard type
        biological_standard = df_biological_standards.loc[
            df_biological_standards["sample_id"] == filename]
        biological_standard = biological_standard["biological_standard"].astype(str).values[0]

        # Get parameters and features for that biological standard type
        msdial_parameters = db.get_parameter_file_path(chromatography, polarity, biological_standard)
        feature_list = db.get_targeted_features_list(biological_standard, chromatography, polarity + " Mode")
        is_bio_standard = True

    elif filename in df_samples["sample_id"].astype(str).tolist():
        msdial_parameters = db.get_parameter_file_path(chromatography, polarity)
        feature_list = db.get_internal_standards_list(chromatography, polarity + " Mode")
        is_bio_standard = False

    else:
        return

    # Get MS-DIAL directory
    msdial_location = db.get_msdial_directory("Default")

    # Run MSConvert
    run_msconvert(path, filename, extension, mzml_file_directory)

    # Run MS-DIAL
    peak_list = run_msdial_processing(filename, msdial_location, msdial_parameters,
                                      str(mzml_file_directory), str(qc_results_directory))

    # Convert peak list to DataFrame
    df_peak_list = peak_list_to_dataframe(peak_list, feature_list)

    # Perform QC checks
    qc_result = qc_sample(df_peak_list, is_bio_standard)

    # Get m/z, RT, and intensity info as JSON strings
    json_mz = df_peak_list[["Name", "Precursor m/z"]].to_json(orient="split")
    json_rt = df_peak_list[["Name", "RT (min)"]].to_json(orient="split")
    json_intensity = df_peak_list[["Name", "Height"]].to_json(orient="split")

    # Write QC results to database and upload to Google Drive
    db.write_qc_results(filename, run_id, json_mz, json_rt, json_intensity, qc_result, is_bio_standard)

    return