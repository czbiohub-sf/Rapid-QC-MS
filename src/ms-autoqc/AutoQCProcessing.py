import warnings
warnings.simplefilter(action="ignore", category=FutureWarning)

import os, time, shutil, subprocess, psutil, traceback
import pandas as pd
import numpy as np
import DatabaseFunctions as db
import SlackNotifications as slack_bot

pd.options.mode.chained_assignment = None

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
            print("Sequence file could not be read.")
            traceback.print_exc()
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
        print("Metadata file could not be read.")
        traceback.print_exc()
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
    pos_msp_file = db.get_msp_file_path(chromatography, "Positive")
    neg_msp_file = db.get_msp_file_path(chromatography, "Negative")

    if not os.path.isfile(pos_msp_file) or not os.path.isfile(neg_msp_file):
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


def run_msconvert(path, filename, extension, output_folder):

    """
    Converts data files in closed vendor format to open mzML format
    """

    # Copy original data file to output folder
    shutil.copy2(path + filename + "." + extension, output_folder)

    user = db.get_msdial_directory().split("/")[2]
    msconvert_exe = '"C:/Users/' + user + '/AppData/Local/Apps/ProteoWizard 3.0.23009.e04bf0d 64-bit/msconvert.exe" '

    # Run MSConvert.exe
    command = msconvert_exe + output_folder + filename + "." + extension + " -o " + output_folder
    os.system(command)
    time.sleep(1)

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


def peak_list_to_dataframe(sample_peak_list, df_features):

    """
    Returns DataFrame with m/z, RT, and intensity info for each internal standard in a given sample
    """

    # Convert .msdial file into a DataFrame
    df = pd.read_csv(sample_peak_list, sep="\t", engine="python", skip_blank_lines=True)
    df.rename(columns={"Title": "Name"}, inplace=True)

    # Get only the m/z, RT, and intensity columns
    df = df[["Name", "Precursor m/z", "RT (min)", "Height"]]

    # Query only internal standards (or targeted features for biological standard)
    feature_list = df_features["name"].astype(str).tolist()
    df = df.loc[df["Name"].isin(feature_list)]

    # Get duplicate annotations in a DataFrame
    df_duplicates = df[df.duplicated(subset=["Name"], keep=False)]

    # Remove duplicates from peak list DataFrame
    df = df[~(df.duplicated(subset=["Name"], keep=False))]

    # Handle duplicate annotations
    if len(df_duplicates) > 0:

        # Get list of annotations that have duplicates in the peak list
        annotations = df_duplicates[~df_duplicates.duplicated(subset=["Name"])]["Name"].tolist()

        # For each unique feature, choose the annotation that best matches library m/z and RT values
        for annotation in annotations:

            # Get all duplicate annotations for that feature
            df_annotation = df_duplicates[df_duplicates["Name"] == annotation]
            df_feature_in_library = df_features.loc[df_features["name"] == annotation]

            # Calculate delta m/z and delta RT
            df_annotation["Delta m/z"] = df_annotation["Precursor m/z"].astype(float) - df_feature_in_library["precursor_mz"].astype(float).values[0]
            df_annotation["Delta RT"] = df_annotation["RT (min)"].astype(float) - df_feature_in_library["retention_time"].astype(float).values[0]

            # Absolute values
            df_annotation["Delta m/z"] = df_annotation["Delta m/z"].abs()
            df_annotation["Delta RT"] = df_annotation["Delta RT"].abs()

            # Append the annotation with the lowest delta RT and delta m/z to the peak list DataFrame
            df_rectified = df_annotation.loc[
                (df_annotation["Delta m/z"] == df_annotation["Delta m/z"].min()) &
                (df_annotation["Delta RT"] == df_annotation["Delta RT"].min())]

            # If there is no "best" feature with the lowest delta RT and lowest delta m/z, choose the lowest delta RT
            if len(df_rectified) == 0:
                df_rectified = df_annotation.loc[
                    df_annotation["Delta RT"] == df_annotation["Delta RT"].min()]

            # If the RT's are exactly the same, choose the feature between them with the lowest delta m/z
            if len(df_rectified) > 1:
                df_rectified = df_rectified.loc[
                    df_rectified["Delta m/z"] == df_rectified["Delta m/z"].min()]

            # If they both have the same delta m/z, choose the feature between them with the greatest intensity
            if len(df_rectified) > 1:
                df_rectified = df_rectified.loc[
                    df_rectified["Height"] == df_rectified["Height"].max()]

            # If at this point there's still duplicates for some reason, just choose the first one
            if len(df_rectified) > 1:
                df_rectified = df_rectified[:1]

            # Append "correct" annotation to peak list DataFrame
            df = df.append(df_rectified, ignore_index=True)

    # DataFrame readiness before return
    try:
        df.drop(columns=["Delta m/z", "Delta RT"], inplace=True)
    finally:
        df.reset_index(drop=True, inplace=True)
        return df


def qc_sample(instrument_id, run_id, polarity, df_peak_list, df_features, is_bio_standard):

    """
    Main algorithm that performs QC checks on sample data
    """

    polarity = polarity.replace("itive", "").replace("ative", "")

    # Handles sample QC checks
    if not is_bio_standard:

        # Handle duplicate features by picking the one with the highest intensity
        # Note: if strict RT and m/z tolerances are specified in the MS-DIAL configuration, this should not be a problem
        duplicates_list = df_peak_list.loc[df_peak_list.duplicated(subset="Name")]["Name"].tolist()

        # If duplicates are found,
        if len(duplicates_list) > 0:

            # For each duplicate feature,
            for duplicate in duplicates_list:

                # Index the duplicates of that feature and keep the one with the highest intensity
                df_duplicates = df_peak_list.loc[df_peak_list["Name"] == duplicate]

                duplicates_to_drop = df_peak_list.loc[
                    (df_peak_list["Name"] == duplicate) & (df_peak_list["Height"] != df_duplicates["Height"].max())]

                df_peak_list.drop(duplicates_to_drop.index, inplace=True)

        # Refactor internal standards DataFrame
        df_features = df_features.rename(
            columns={"name": "Name",
                     "chromatography": "Chromatography",
                     "polarity": "Polarity",
                     "precursor_mz": "Library m/z",
                     "retention_time": "Library RT",
                     "ms2_spectrum": "Library MS2",
                     "inchikey": "Library INCHIKEY"})

        # Get delta RT and delta m/z values for each internal standard
        df_compare = pd.merge(df_features, df_peak_list, on="Name")
        df_compare["Delta RT"] = df_compare["RT (min)"].astype(float) - df_compare["Library RT"].astype(float)
        df_compare["Delta m/z"] = df_compare["Precursor m/z"].astype(float) - df_compare["Library m/z"].astype(float)

        # Get in-run RT average for each internal standard
        df_compare["In-run RT average"] = np.nan
        df_run_retention_times = db.parse_internal_standard_data(instrument_id, run_id, "retention_time", polarity, "Processing", False)

        if df_run_retention_times is not None:
            # Calculate in-run RT average for each internal standard
            for internal_standard in df_run_retention_times.columns:
                if internal_standard == "Sample":
                    continue
                in_run_average = df_run_retention_times[internal_standard].dropna().astype(float).mean()
                df_compare.loc[df_compare["Name"] == internal_standard, "In-run RT average"] = in_run_average

            # Compare each internal standard RT to in-run RT average
            df_compare["In-run delta RT"] = df_compare["RT (min)"].astype(float) - df_compare["In-run RT average"].astype(float)
        else:
            df_compare["In-run delta RT"] = np.nan

        # Prepare final DataFrame
        qc_dataframe = df_compare[["Name", "Delta m/z", "Delta RT", "In-run delta RT"]]

        # Count internal standard intensity dropouts
        qc_dataframe["Intensity dropout"] = 0
        qc_dataframe["Warnings"] = ""
        qc_dataframe["Fails"] = ""
        for feature in df_features["Name"].astype(str).tolist():
            if feature not in df_peak_list["Name"].astype(str).tolist():
                row = {"Name": feature,
                       "Delta m/z": np.nan,
                       "Delta RT": np.nan,
                       "In-run delta RT": np.nan,
                       "Intensity dropout": 1,
                       "Warnings": "",
                       "Fails": ""}
                qc_dataframe = qc_dataframe.append(row, ignore_index=True)

        # Determine pass / fail based on user criteria
        qc_config = db.get_qc_configuration_parameters(instrument_id=instrument_id, run_id=run_id)
        qc_result = "Pass"

        # QC of internal standard intensity dropouts
        if qc_config["intensity_enabled"].values[0] == 1:

            # Mark fails
            qc_dataframe.loc[qc_dataframe["Intensity dropout"].astype(int) == 1, "Fails"] = "Missing"

            # Count intensity dropouts
            intensity_dropouts = qc_dataframe["Intensity dropout"].astype(int).sum()
            intensity_dropouts_cutoff = qc_config["intensity_dropouts_cutoff"].astype(int).values[0]

            # Compare number of intensity dropouts to user-defined cutoff
            if intensity_dropouts >= intensity_dropouts_cutoff:
                qc_result = "Fail"

            if qc_result != "Fail":
                if intensity_dropouts > len(intensity_dropouts_cutoff) / 1.33:
                    qc_result = "Warning"

        # QC of internal standard RT's against library RT's
        if qc_config["library_rt_enabled"].values[0] == 1:

            # Check if delta RT's are outside of user-defined cutoff
            library_rt_shift_cutoff = qc_config["library_rt_shift_cutoff"].astype(float).values[0]

            # Mark fails
            fails = qc_dataframe["Delta RT"].abs() > library_rt_shift_cutoff
            qc_dataframe.loc[fails, "Fails"] = "RT"

            # Mark warnings
            warnings = ((library_rt_shift_cutoff / 1.5) < qc_dataframe["Delta RT"].abs()) & \
                       ((qc_dataframe["Delta RT"].abs()) < library_rt_shift_cutoff)
            qc_dataframe.loc[warnings, "Warnings"] = "RT"

            if len(qc_dataframe.loc[fails]) >= len(qc_dataframe) / 2:
                qc_result = "Fail"
            else:
                if len(qc_dataframe.loc[warnings]) > len(qc_dataframe) / 2 and qc_result != "Fail":
                    qc_result = "Warning"

        # QC of internal standard RT's against in-run RT average
        if qc_config["in_run_rt_enabled"].values[0] == 1 and df_run_retention_times is not None:

            # Check if in-run delta RT's are outside of user-defined cutoff
            in_run_rt_shift_cutoff = qc_config["in_run_rt_shift_cutoff"].astype(float).values[0]

            # Mark fails
            fails = qc_dataframe["In-run delta RT"].abs() > in_run_rt_shift_cutoff
            qc_dataframe.loc[fails, "Fails"] = "In-Run RT"

            # Mark warnings
            warnings = ((in_run_rt_shift_cutoff / 1.25) < qc_dataframe["In-run delta RT"].abs()) & \
                       (qc_dataframe["In-run delta RT"].abs() < in_run_rt_shift_cutoff)
            qc_dataframe.loc[warnings, "Warnings"] = "In-Run RT"

            if len(qc_dataframe.loc[fails]) >= len(qc_dataframe) / 2:
                qc_result = "Fail"
            else:
                if len(qc_dataframe.loc[warnings]) > len(qc_dataframe) / 2 and qc_result != "Fail":
                    qc_result = "Warning"

        # QC of internal standard precursor m/z against library m/z
        if qc_config["library_mz_enabled"].values[0] == 1:

            # Check if delta m/z's are outside of user-defined cutoff
            library_mz_shift_cutoff = qc_config["library_mz_shift_cutoff"].astype(float).values[0]

            # Mark fails
            fails = qc_dataframe["Delta m/z"].abs() > library_mz_shift_cutoff
            qc_dataframe.loc[fails, "Fails"] = "m/z"

            # Mark warnings
            warnings = ((library_mz_shift_cutoff / 1.25) < qc_dataframe["Delta m/z"].abs()) & \
                       (qc_dataframe["Delta m/z"].abs() < library_mz_shift_cutoff)
            qc_dataframe.loc[warnings, "Warnings"] = "m/z"

            if len(qc_dataframe.loc[fails]) >= len(qc_dataframe) / 2:
                qc_result = "Fail"
            else:
                if len(qc_dataframe.loc[warnings]) > len(qc_dataframe) / 2 and qc_result != "Fail":
                    qc_result = "Warning"

    # Handles biological standard QC checks
    else:
        qc_dataframe = pd.DataFrame()
        qc_result = "Pass"

    return qc_dataframe, qc_result


def convert_to_json(sample_id, df_peak_list, qc_dataframe):

    """
    Format DataFrames as JSON strings, with features as columns and relevant data in rows
    """

    # m/z, RT, intensity
    df_mz = df_peak_list[["Name", "Precursor m/z"]]
    df_rt = df_peak_list[["Name", "RT (min)"]]
    df_intensity = df_peak_list[["Name", "Height"]]

    df_mz = df_mz.rename(columns={"Precursor m/z": sample_id})
    df_rt = df_rt.rename(columns={"RT (min)": sample_id})
    df_intensity = df_intensity.rename(columns={"Height": sample_id})

    df_mz = df_mz.set_index("Name").transpose()
    df_rt = df_rt.set_index("Name").transpose()
    df_intensity = df_intensity.set_index("Name").transpose()

    json_mz = df_mz.to_json(orient="split")
    json_rt = df_rt.to_json(orient="split")
    json_intensity = df_intensity.to_json(orient="split")

    # QC results
    for column in qc_dataframe.columns:
        if column != "Name":
            qc_dataframe.rename(columns={column: sample_id + " " + column}, inplace=True)
    qc_dataframe = qc_dataframe.set_index("Name").transpose()
    json_qc = qc_dataframe.to_json(orient="split")

    return json_mz, json_rt, json_intensity, json_qc


def process_data_file(path, filename, extension, instrument_id, run_id):

    """
    1. Convert data file to mzML format using MSConvert
    2. Process data file using MS-DIAL and user-defined parameter configuration
    3. Load data into pandas DataFrame and execute AutoQC algorithm
    4. Write QC results to "sample_qc_results" or "bio_qc_results" table accordingly
    5. Write results to SQLite database
    """

    id = instrument_id.replace(" ", "_") + "_" + run_id

    # Create the necessary directories
    data_directory = os.path.join(os.getcwd(), r"data")
    mzml_file_directory = os.path.join(data_directory, id, "data")
    qc_results_directory = os.path.join(data_directory, id, "results")

    for directory in [data_directory, mzml_file_directory, qc_results_directory]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    mzml_file_directory = mzml_file_directory + "/"
    qc_results_directory = qc_results_directory + "/"

    # Retrieve chromatography, polarity, samples, and biological standards using run ID
    df_run = db.get_instrument_run(instrument_id, run_id)
    chromatography = df_run["chromatography"].astype(str).values[0]

    if "Pos" in filename:
        polarity = "Positive"
    elif "Neg" in filename:
        polarity = "Negative"

    df_samples = db.get_samples_in_run(instrument_id, run_id, sample_type="Sample")
    df_biological_standards = db.get_samples_in_run(instrument_id, run_id, sample_type="Biological Standard")

    # Retrieve MS-DIAL parameters, internal standards, and targeted features from database
    if filename in df_biological_standards["sample_id"].astype(str).tolist():

        # Get biological standard type
        biological_standard = df_biological_standards.loc[
            df_biological_standards["sample_id"] == filename]
        biological_standard = biological_standard["biological_standard"].astype(str).values[0]

        # Get parameters and features for that biological standard type
        msdial_parameters = db.get_parameter_file_path(chromatography, polarity, biological_standard)
        df_features = db.get_targeted_features(biological_standard, chromatography, polarity + " Mode")
        is_bio_standard = True

    elif filename in df_samples["sample_id"].astype(str).tolist():
        msdial_parameters = db.get_parameter_file_path(chromatography, polarity)
        df_features = db.get_internal_standards(chromatography, polarity + " Mode")
        is_bio_standard = False

    else:
        return

    # Get MS-DIAL directory
    msdial_directory = db.get_msdial_directory()

    # Run MSConvert
    run_msconvert(path, filename, extension, mzml_file_directory)

    # Run MS-DIAL
    try:
        peak_list = run_msdial_processing(filename, msdial_directory, msdial_parameters,
            str(mzml_file_directory), str(qc_results_directory))
    except:
        print("Failed to run MS-DIAL.")
        traceback.print_exc()
        return

    # Convert peak list to DataFrame
    try:
        df_peak_list = peak_list_to_dataframe(peak_list, df_features)
    except:
        print("Failed to convert peak list to DataFrame.")
        traceback.print_exc()
        return

    # Execute AutoQC algorithm
    try:
        qc_dataframe, qc_result = qc_sample(instrument_id, run_id, polarity, df_peak_list, df_features, is_bio_standard)
    except:
        print("Failed to execute AutoQC algorithm.")
        traceback.print_exc()
        return

    # Send Slack notification (if they are enabled)
    try:
        if db.slack_notifications_are_enabled():
            if qc_result != "Pass":
                alert = "QC " + qc_result + ": " + filename
                slack_bot.send_message(alert)
    except:
        print("Failed to send Slack notification.")
        traceback.print_exc()

    # Convert m/z, RT, and intensity data to JSON strings
    try:
        json_mz, json_rt, json_intensity, json_qc = convert_to_json(sample_id, df_peak_list, qc_dataframe)
    except:
        print("Failed to convert DataFrames to JSON format.")
        traceback.print_exc()
        return

    try:
        # Write QC results to database and upload to Google Drive
        db.write_qc_results(filename, run_id, json_mz, json_rt, json_intensity, json_qc, qc_result, is_bio_standard)

        # Update sample counters to trigger dashboard update
        db.update_sample_counters_for_run(instrument_id=instrument_id, run_id=run_id, qc_result=qc_result, latest_sample=filename)

        # If sync is enabled, upload the QC results to Google Drive
        if db.sync_is_enabled():
            db.upload_qc_results(instrument_id, run_id)

    except:
        print("Failed to write QC results to database.")
        traceback.print_exc()
        return

    # Delete MS-DIAL result file
    # try:
    #     os.remove(qc_results_directory + filename + ".msdial")
    # except Exception as error:
    #     print("Failed to remove MS-DIAL result file.")
    #     traceback.print_exc()
    #     return


def listener_is_running(pid):

    """
    Check if acquisition listener subprocess is still running
    """

    if pid is None:
        return False

    time.sleep(1)

    try:
        if psutil.Process(pid).status() == "running":
            return True
        else:
            return False
    except Exception as error:
        return False


def kill_acquisition_listener(pid):

    """
    Kill acquisition listener subprocess using the pid
    """

    try:
        return psutil.Process(pid).kill()
    except Exception as error:
        print("Error killing acquisition listener.")
        traceback.print_exc()