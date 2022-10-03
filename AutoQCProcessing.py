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


def run_msconvert(path, filename, extension, output_folder):

    """
    Converts data files in closed vendor format to open mzML format
    """

    # Copy original data file to output folder
    shutil.copy2(path, output_folder)

    # Run MSConvert Docker container and allow 5 seconds for conversion
    command = "docker run --rm -e WINEDEBUG=-all -v " \
            + output_folder.replace(" ", "/") \
            + ":/data chambm/pwiz-skyline-i-agree-to-the-vendor-licenses wine msconvert /data/" \
            + filename + "." + extension

    # Give MSConvert 5 seconds to run
    os.system(command)
    time.sleep(5)

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

    # Give MS-DIAL 10 seconds to run
    os.system(command)
    time.sleep(10)

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


def process_data_file(path, filename, extension, run_id):

    """
    1. Convert data file to mzML format using MSConvert
    2. Process data file using MS-DIAL and user-defined parameter configuration
    3. Write QC results to "sample_qc_results" table in SQLite database
    4. Upload CSV file with QC results (as JSON) to Google Drive
    """

    # Create directories
    autoqc_directory = os.path.join(os.getcwd(), r"data")
    mzml_file_directory = os.path.join(autoqc_directory, run_id, "data")
    qc_results_directory = os.path.join(autoqc_directory, run_id, "results")

    for directory in [autoqc_directory, mzml_file_directory, qc_results_directory]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    mzml_file_directory = mzml_file_directory + "/"
    qc_results_directory = qc_results_directory + "/"

    # TODO: Get these from the database
    msdial_location = "C:/Users/eliaslab/Documents/MSDIAL"
    msdial_parameters = "C:/Users/eliaslab/Downloads/AutoQC_Test_Files/DDA_HILIC_Pos/" \
                        + "Msdial_lcms_ddaParamBCDEditedUpdatedPositive.txt"
    internal_standards = ["1_Methionine_d8", "1_1_Methylnicotinamide_d3", "1_Creatinine_d3", "1_Carnitine_d3",
                          "1_Acetylcarnitine_d3", "1_TMAO_d9", "1_Choline_d9", "1_Glutamine_d5", "1_CUDA",
                          "1_Glutamic Acid_d3", "1_Arginine_d7", "1_Alanine_d3", "1_Valine d8", "1_Tryptophan d5",
                          "1_Serine d3", "1_Lysine d8", "1_Phenylalanine d8", "1_Hippuric acid d5"]
    targeted_features = ""
    qc_result = "pass"
    is_bio_standard = False

    # Run MSConvert
    run_msconvert(path, filename, extension, mzml_file_directory)

    # Run MS-DIAL
    peak_list = run_msdial_processing(filename, msdial_location, msdial_parameters,
                                      str(mzml_file_directory), str(qc_results_directory))

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