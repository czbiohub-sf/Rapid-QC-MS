import os, time, shutil
import pandas as pd
import DatabaseFunctions as db

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


def run_msconvert(path, filename, output_folder):

    """
    Converts data files in closed vendor format to open mzML format
    """

    # Run MSConvert Docker container and allow 5 seconds for conversion
    command = "docker run --rm -e WINEDEBUG=-all -v " \
            + path.replace(" ", "\ ") \
            + ":/data chambm/pwiz-skyline-i-agree-to-the-vendor-licenses wine msconvert /data/" + filename

    os.system(command)

    # Wait 5 seconds
    time.sleep(5)

    # Get newly-generate mzml file
    mzml_file = path + filename.split(".")[0] + ".mzml"

    # Copy to output folder
    shutil.copy2(mzml_file, output_folder)

    return


def run_msdial_processing(filename, msdial_path, parameter_file, input_folder, output_folder):

    """
    Processes data files using MS-DIAL command line tools
    """

    # Navigate to directory containing MS-DIAL
    os.chdir(msdial_path)

    # Run MS-DIAL
    command = "MsdialConsoleApp.exe lcmsdda -i " + input_folder \
              + " -o " + output_folder \
              + " -m " + parameter_file + " -p"

    os.system(command)

    # Clear data file directory for next sample


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


def process_data_file(filename, path, run_id, is_bio_standard):

    """
    1. Convert data file to mzML format using MSConvert
    2. Process data file using MS-DIAL and user-defined parameter configuration
    3. Write QC results to "sample_qc_results" table in SQLite database
    4. Upload CSV file with QC results (as JSON) to Google Drive
    """

    if not path.endswith("/"):
        path = path + "/"

    data_file_directory = "/" + run_id + "/data/"
    qc_results_directory = "/" + run_id + "/data/"

    # TODO: Get these from the database
    msdial_location = ""
    msdial_parameters = ""
    internal_standards = ""
    targeted_features = ""
    qc_result = "pass"

    # Run MSConvert
    run_msconvert(path, filename, data_file_directory)

    # Run MS-DIAL
    peak_list = run_msdial_processing(filename, msdial_location, msdial_parameters,
                                      data_file_directory, qc_results_directory)

    # Convert peak list to DataFrame
    if is_bio_standard:
        df_peak_list = peak_list_to_dataframe(peak_list, targeted_features)
    else:
        df_peak_list = peak_list_to_dataframe(peak_list, internal_standards)

    # TODO: Perform QC checks
    # qc_result = run_autoqc(df_peak_list, is_bio_standard)

    # Get m/z, RT, and intensity info as JSON strings
    json_mz = df_peak_list[["Name", "Precursor m/z"]].to_json(orient="split")
    json_rt = df_peak_list[["Name", "RT (min)"]].to_json(orient="split")
    json_intensity = df_peak_list[["Name", "Height"]].to_json(orient="split")

    # Write QC results to database and upload to Google Drive
    db.write_qc_results(filename, run_id, json_mz, json_rt, json_intensity, qc_result)

    return qc_result