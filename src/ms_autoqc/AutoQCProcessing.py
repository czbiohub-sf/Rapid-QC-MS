import warnings
warnings.simplefilter(action="ignore", category=FutureWarning)

import os, time, shutil, psutil, traceback
import pandas as pd
import numpy as np
import ms_autoqc.DatabaseFunctions as db
import ms_autoqc.SlackNotifications as slack_bot

pd.options.mode.chained_assignment = None

def sequence_is_valid(filename, contents, vendor="Thermo Fisher"):

    """
    Validates that instrument sequence file contains the correct columns.

    TODO: Add support for other mass spectrometry instrument vendors here.

    Args:
        filename (str):
            Acquisition sequence file name
        contents (io.StringIO):
            Acquisition sequence as in-memory file object
        vendor (str, default "Thermo Fisher"):
            Instrument vendor for parsing sequence

    Returns:
        True if sequence table is valid, otherwise False.
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
    Validates that sample metadata file contains the required columns.

    Args:
        filename (str):
            Metadata file name
        contents (io.StringIO):
            Metadata file as in-memory file object

    Returns:
        True if metadata table is valid, otherwise False.
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


def chromatography_valid(chromatography):

    """
    Validates that MSP / TXT files for the given chromatography method exist.

    TODO: Per Brian, some labs don't run in both polarities. Will need to make this function flexible for that.

    Args:
        chromatography (str): Chromatography method ID to validate

    Returns:
        True if chromatography method files exist, False if not.
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


def biological_standards_valid(chromatography, biological_standards):

    """
    Validates that the given list of biological standards have MSP files.

    Args:
        chromatography (str):
            Chromatography method ID to validate
        biological_standards (list):
            List of biological standards to validate

    Returns:
        True if all MSP / TXT files exist, False if not.
    """

    # Query the provided biological standards from the database
    df = db.get_biological_standards()
    df = df.loc[df["chromatography"] == chromatography]
    df = df.loc[df["name"].isin(biological_standards)]

    # Check whether the method's MSP / TXT files exist, and return False if they don't
    pos_msp_files = df["pos_bio_msp_file"].astype(str).tolist()
    neg_msp_files = df["neg_bio_msp_file"].astype(str).tolist()
    msp_files = pos_msp_files + neg_msp_files

    for file in msp_files:
        file_path = os.path.join(os.getcwd(), "data", "methods", file)
        if not os.path.isfile(file_path):
            return False

    return True


def convert_sequence_to_json(sequence_contents, vendor="Thermo Fisher"):

    """
    Converts sequence table to JSON string for database storage.

    TODO: Convert to "records" orient instead. Much faster to load data using pd.DataFrame(json.loads(json_string))
        instead of pd.read_json(json_string, orient="split").

    Args:
        sequence_contents (io.StringIO):
            Acquisition file as in-memory file object
        vendor (str, default "Thermo Fisher"):
            Instrument vendor for parsing sequence

    Returns:
        JSON string of acquisition sequence DataFrame in "split" format.
    """

    # Select columns from sequence using correct vendor software nomenclature
    if vendor == "Thermo Fisher":
        df_sequence = pd.read_csv(sequence_contents, index_col=False)
        df_sequence.columns = df_sequence.iloc[0]
        df_sequence = df_sequence.drop(df_sequence.index[0])

    # Convert DataFrames to JSON strings
    return df_sequence.to_json(orient="split")


def convert_metadata_to_json(metadata_contents):

    """
    Converts sequence and metadata files to JSON strings for database storage.

    TODO: Convert to "records" orient instead. Much faster to load data using pd.DataFrame(json.loads(json_string))
        instead of pd.read_json(json_string, orient="split").

    Args:
        metadata_contents (io.StringIO): Metadata file as in-memory file object

    Returns:
        JSON string of sample metadata DataFrame in "split" format.
    """

    # Select columns from metadata
    df_metadata = pd.read_csv(metadata_contents, index_col=False)

    # Convert DataFrames to JSON strings
    return df_metadata.to_json(orient="split")


def run_msconvert(path, filename, extension, output_folder):

    """
    Makes a copy of data file and converts it from instrument vendor format to open mzML format.

    This function runs msconvert.exe in a background process. It checks every second for 30 seconds if the
    mzML file was created, and if it hangs, will terminate the msconvert subprocess and return None.

    TODO: As MS-AutoQC has evolved, some arguments for this function have become redundant.
        The output folder is always fixed, so this parameter should be removed.

    Args:
        path (str):
            Data acquisition path (with "/" at the end)
        filename (str):
            Name of sample data file
        extension (str):
            Data file extension, derived from instrument vendor
        output_folder (str):
            Output directory for mzML file – this is always ../data/instrument_id_run_id/data

    Returns:
        File path for mzML file (*.mzml)
    """

    # Remove files in output folder (if any)
    try:
        for file in os.listdir(output_folder):
            os.remove(output_folder + file)
    except Exception as error:
        print(error)
    finally:
        # Copy original data file to output folder
        shutil.copy2(path + filename + "." + extension, output_folder)

    # Get MSConvert.exe
    try:
        msconvert_folder = db.get_msconvert_directory()
        msconvert_exe = '"' + msconvert_folder + '/msconvert.exe" '
    except:
        print("Failed to locate MSConvert.exe!")
        traceback.print_exc()
        return None

    # Run MSConvert in a subprocess
    command = msconvert_exe + output_folder + filename + "." + extension + " -o " + output_folder
    process = psutil.Popen(command)
    pid = process.pid

    # Check every second for 30 seconds if mzML file was created; if process hangs, terminate and return None
    for index in range(31):
        if not subprocess_is_running(pid):
            break
        else:
            if index != 30:
                time.sleep(1)
            else:
                kill_subprocess(pid)
                return None

    # Delete copy of original data file
    data_file_copy = output_folder + filename + "." + extension
    os.remove(data_file_copy)

    # Return mzML file path to indicate success
    return output_folder + filename + ".mzml"


def run_msdial_processing(filename, msdial_path, parameter_file, input_folder, output_folder):

    """
    Processes data file (in mzML format) using the MS-DIAL console app.

    TODO: As MS-AutoQC has evolved, some arguments for this function have become redundant.
        The input and output folders are fixed, so these parameters should be removed.

    Args:
        filename (str):
            Name of sample data file
        msdial_path (str):
            Path for directory storing MSDialConsoleApp.exe
        parameter_file (str):
            Path for parameters.txt file, stored in /methods directory
        input_folder (str):
            Input folder – this is always ../data/instrument_id_run_id/data
        output_folder (str):
            Output folder – this is always ../data/instrument_id_run_id/results

    Returns:
        File path for MS-DIAL result file (*.msdial)
    """

    # Navigate to directory containing MS-DIAL
    home = os.getcwd()
    os.chdir(msdial_path)

    # Run MS-DIAL in a subprocess
    command = "MsdialConsoleApp.exe lcmsdda -i " + input_folder \
              + " -o " + output_folder \
              + " -m " + parameter_file + " -p"
    process = psutil.Popen(command)
    pid = process.pid

    # Check every second for 30 seconds if process was completed; if process hangs, return None
    for index in range(31):
        if not subprocess_is_running(pid):
            break
        else:
            if index != 30:
                time.sleep(1)
            else:
                kill_subprocess(pid)
                os.chdir(home)
                return None

    # Clear data file directory for next sample
    for file in os.listdir(input_folder):
        filepath = os.path.join(input_folder, file)
        try:
            shutil.rmtree(filepath)
        except OSError:
            os.remove(filepath)

    # Return to original working directory
    os.chdir(home)

    # MS-DIAL output filename
    msdial_result = output_folder + "/" + filename.split(".")[0] + ".msdial"

    # Return .msdial file path
    return msdial_result


def peak_list_to_dataframe(sample_peak_list, df_features):

    """
    Filters duplicates and poor annotations from MS-DIAL peak table and creates DataFrame storing
    m/z, RT, and intensity data for each internal standard (or targeted metabolite) in the sample.

    TODO: Describe duplicate handling in more detail in this docstring.

    Args:
        sample_peak_list (str):
            File path for MS-DIAL peak table, an .msdial file located in /data/instrument_id_run_id/results
        df_features (DataFrame):
            An m/z - RT table derived from internal standard (or biological standard) MSP library in database

    Returns:
        DataFrame with m/z, RT, and intensity data for each internal standard / targeted metabolite in the sample.
    """

    # Convert .msdial file into a DataFrame
    df = pd.read_csv(sample_peak_list, sep="\t", engine="python", skip_blank_lines=True)
    df.rename(columns={"Title": "Name"}, inplace=True)

    # Get only the m/z, RT, and intensity columns
    df = df[["Name", "Precursor m/z", "RT (min)", "Height", "MSMS spectrum"]]

    # Query only internal standards (or targeted features for biological standard)
    feature_list = df_features["name"].astype(str).tolist()
    without_ms2_feature_list = ["w/o MS2:" + feature for feature in feature_list]
    df = df.loc[(df["Name"].isin(feature_list)) | (df["Name"].isin(without_ms2_feature_list))]

    # Label annotations with and without MS2
    with_ms2 = df["MSMS spectrum"].notnull()
    without_ms2 = df["MSMS spectrum"].isnull()
    df.loc[with_ms2, "MSMS spectrum"] = "MS2"

    # Handle annotations made without MS2
    if len(df[with_ms2]) > 0:
        df.replace(["w/o MS2:"], "", regex=True, inplace=True)      # Remove "w/o MS2" from annotation name
        ms2_matching = True                                         # Boolean that says MS/MS was used for identification
    else:
        ms2_matching = False

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

            # First, remove duplicates without MS2 (if an MSP with MS2 spectra was used for processing)
            if ms2_matching:
                new_df = df_annotation.loc[df_annotation["MSMS spectrum"].notnull()]

                # If annotations with MS2 remain, use them moving forward
                if len(new_df) > 1:
                    df_annotation = new_df

                # If no annotations with MS2 remain, filter annotations without MS2 by height
                else:
                    # Choose the annotation with the highest intensity
                    if len(df_annotation) > 1:
                        df_annotation = df_annotation.loc[
                            df_annotation["Height"] == df_annotation["Height"].max()]

                    # If there's only one annotation without MS2 left, choose as the "correct" annotation
                    if len(df_annotation) == 1:
                        df = df.append(df_annotation, ignore_index=True)
                        continue

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
    Performs quality control on sample data based on user-defined criteria in Settings > QC Configurations.

    The following quality control parameters are used to determine QC pass, warning, or fail:
        1. Intensity dropouts cutoff:
            How many internal standards are missing in the sample?
        2. RT shift from library value cutoff:
            How many retention times are shifted from the expected value for the chromatography method?
        3. RT shift from in-run average cutoff:
            How many retention times are shifted from their average RT during the run?
        4. m/z shift from library value cutoff:
            How many precursor masses are shifted from the expected value for the internal standard?

    This function returns a DataFrame with a single record in the following format:

    | Sample     | Delta m/z | Delta RT | In-run delta RT | Warnings  | Fails |
    | ---------- | --------- | -------- | --------------- | --------- | ----- |
    | SAMPLE_001 | 0.000001  | 0.05     | 0.00001         | Delta RT  | None  |

    Confusingly, a sample has an overall QC result, as well as QC warnings and fails for each internal standard.
    This makes it easier to determine what caused the overall QC result.

    See a screenshot of the sample information card in the Overview page of the website for context.

    While the thresholds for QC pass and fail are explicit, allowing the user to determine thresholds for QC warnings
    was deemed too cumbersome. Instead, an overall QC result of "Warning" happens if any of the following are true:
        1. The number of intensity dropouts is 75% or more than the defined cutoff
        2. The QC result is not a "Fail" and 50% or more internal standards have a QC Warning

    For each internal standard, a note is added to the "Warning" or "Fail" column of qc_dataframe based on the user's
    defined criteria in Settings > QC Configurations. If the internal standard is not marked as a "Fail", then
    "Warnings" for individual internal standards could be marked if:
        1. The delta RT is greater than 66% of the "RT shift from library value" cutoff
        2. The In-run delta RT is greater than 80% of the "RT shift from in-run average value" cutoff
        3. The delta m/z is greater than 80% of the "RT shift from library value" cutoff

    TODO: Define and implement quality control for biological standards.

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (Job ID)
        polarity (str):
            Polarity, either "Pos or "Neg"
        df_peak_list (DataFrame):
            Filtered peak table, from peak_list_to_dataframe()
        df_features (DataFrame):
            An m/z - RT table derived from internal standard (or biological standard) MSP library in database
        is_bio_standard (bool):
            Whether sample is a biological standard or not

    Returns:
        (DataFrame, str): Tuple containing QC results table and QC result (either "Pass", "Fail", or "Warning").
    """

    # Handles sample QC checks
    if not is_bio_standard:

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
        df_peak_list_copy = df_peak_list.copy()

        # Get MS-DIAL RT threshold and filter out annotations without MS2 that are outside threshold
        with_ms2 = df_compare["MSMS spectrum"].notnull()
        without_ms2 = df_compare["MSMS spectrum"].isnull()
        annotations_without_ms2 = df_compare[without_ms2]["Name"].astype(str).tolist()

        if len(df_compare[with_ms2]) > 0:
            rt_threshold = db.get_msdial_configuration_parameters("Default", parameter="post_id_rt_tolerance")
            outside_rt_threshold = df_compare["Delta RT"].abs() > rt_threshold
            annotations_to_drop = df_compare.loc[(without_ms2) & (outside_rt_threshold)]

            df_compare.drop(annotations_to_drop.index, inplace=True)
            annotations_to_drop = annotations_to_drop["Name"].astype(str).tolist()
            annotations_to_drop = df_peak_list_copy[df_peak_list_copy["Name"].isin(annotations_to_drop)]
            df_peak_list_copy.drop(annotations_to_drop.index, inplace=True)

        # Get in-run RT average for each internal standard
        df_compare["In-run RT average"] = np.nan
        df_run_retention_times = db.parse_internal_standard_data(instrument_id, run_id, "retention_time", polarity, "processing", False)

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
            if feature not in df_peak_list_copy["Name"].astype(str).tolist():
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
            intensity_dropouts_cutoff = qc_config["intensity_dropouts_cutoff"].astype(int).tolist()[0]

            # Compare number of intensity dropouts to user-defined cutoff
            if intensity_dropouts >= intensity_dropouts_cutoff:
                qc_result = "Fail"

            if qc_result != "Fail":
                if intensity_dropouts > intensity_dropouts_cutoff / 1.33:
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

        # Mark annotations without MS2
        qc_dataframe.loc[qc_dataframe["Name"].isin(annotations_without_ms2), "Warnings"] = "No MS2"

    # Handles biological standard QC checks
    else:
        qc_dataframe = pd.DataFrame()
        qc_result = "Pass"

    return qc_dataframe, qc_result


def convert_to_dict(sample_id, df_peak_list, qc_dataframe):

    """
    Converts DataFrames to dictionary records, with features as columns and samples as rows,
    which are then cast to string format for database storage.

    See parse_internal_standard_data() in the DatabaseFunctions module for more information.

    Format:

    | Name       | iSTD 1 | iSTD 2 | iSTD 3 | iSTD 4 | ... |
    | ---------- | ------ | ------ | ------ | ------ | ... |
    | SAMPLE_001 | 1.207  | 1.934  | 3.953  | 8.132  | ... |

    Args:
        sample_id (str):
            Sample ID
        df_peak_list (DataFrame):
            Filtered MS-DIAL peak table result
        qc_dataframe (DataFrame):
            Table of various QC results

    Returns:
        (str, str, str, str): Tuple containing dictionary records of m/z, RT, intensity, and
        QC data, respectively, cast as strings for database storage.
    """

    # m/z, RT, intensity
    df_mz = df_peak_list[["Name", "Precursor m/z"]]
    df_rt = df_peak_list[["Name", "RT (min)"]]
    df_intensity = df_peak_list[["Name", "Height"]]

    df_mz = df_mz.rename(columns={"Precursor m/z": sample_id})
    df_rt = df_rt.rename(columns={"RT (min)": sample_id})
    df_intensity = df_intensity.rename(columns={"Height": sample_id})

    df_mz = df_mz.transpose().reset_index()
    df_rt = df_rt.transpose().reset_index()
    df_intensity = df_intensity.transpose().reset_index()

    df_mz.columns = df_mz.iloc[0].astype(str).tolist()
    df_rt.columns = df_rt.iloc[0].astype(str).tolist()
    df_intensity.columns = df_intensity.iloc[0].astype(str).tolist()

    df_mz = df_mz.drop(df_mz.index[0])
    df_rt = df_rt.drop(df_rt.index[0])
    df_intensity = df_intensity.drop(df_intensity.index[0])

    mz_record = df_mz.to_dict(orient="records")[0]
    rt_record = df_rt.to_dict(orient="records")[0]
    intensity_record = df_intensity.to_dict(orient="records")[0]

    # QC results
    if len(qc_dataframe) > 0:
        for column in qc_dataframe.columns:
            if column != "Name":
                qc_dataframe.rename(columns={column: sample_id + " " + column}, inplace=True)
        qc_dataframe = qc_dataframe.transpose().reset_index()
        qc_dataframe.columns = qc_dataframe.iloc[0].astype(str).tolist()
        qc_dataframe = qc_dataframe.drop(qc_dataframe.index[0])
        qc_dataframe = qc_dataframe.fillna(" ")
        qc_record = qc_dataframe.to_dict(orient="records")
    else:
        qc_record = {}

    return str(mz_record), str(rt_record), str(intensity_record), str(qc_record)


def process_data_file(path, filename, extension, instrument_id, run_id):

    """
    Processes data file upon sample acquisition completion.

    For more details, please visit the Documentation page on the website.

    Performs the following functions:
        1. Convert data file to mzML format using MSConvert
        2. Process data file using MS-DIAL and user-defined parameter configuration
        3. Load peak table into DataFrame and filter out poor annotations
        4. Perform quality control checks based on user-defined criteria
        5. Notify user of QC warnings or fails via Slack or email
        6. Write QC results to instrument database
        7. If Google Drive sync is enabled, upload results as CSV files

    Args:
        path (str):
            Data acquisition path
        filename (str):
            Name of sample data file
        extension (str):
            Data file extension, derived from instrument vendor
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)

    Returns:
        None
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

    # Retrieve chromatography, samples, and biological standards using run ID
    df_run = db.get_instrument_run(instrument_id, run_id)
    chromatography = df_run["chromatography"].astype(str).values[0]

    df_samples = db.get_samples_in_run(instrument_id, run_id, sample_type="Sample")
    df_biological_standards = db.get_samples_in_run(instrument_id, run_id, sample_type="Biological Standard")

    # Retrieve MS-DIAL parameters, internal standards, and targeted features from database
    if filename in df_biological_standards["sample_id"].astype(str).tolist():

        # Get biological standard type
        biological_standard = df_biological_standards.loc[
            df_biological_standards["sample_id"] == filename]

        # Get polarity
        try:
            polarity = biological_standard["polarity"].astype(str).values[0]
        except Exception as error:
            print("Could not read polarity from database:", error)
            print("Using default positive mode.")
            polarity = "Pos"

        biological_standard = biological_standard["biological_standard"].astype(str).values[0]

        # Get parameters and features for that biological standard type
        msdial_parameters = db.get_parameter_file_path(chromatography, polarity, biological_standard)
        df_features = db.get_targeted_features(biological_standard, chromatography, polarity)
        is_bio_standard = True

    elif filename in df_samples["sample_id"].astype(str).tolist():

        # Get polarity
        try:
            polarity = df_samples.loc[df_samples["sample_id"] == filename]["polarity"].astype(str).values[0]
        except Exception as error:
            print("Could not read polarity from database:", error)
            print("Using default positive mode.")
            polarity = "Positive"

        msdial_parameters = db.get_parameter_file_path(chromatography, polarity)
        df_features = db.get_internal_standards(chromatography, polarity)
        is_bio_standard = False

    else:
        print("Error! Could not retrieve MS-DIAL libraries and parameters.")
        return

    # Get MS-DIAL directory
    msdial_directory = db.get_msdial_directory()

    # Run MSConvert
    try:
        mzml_file = run_msconvert(path, filename, extension, mzml_file_directory)

        # For active instrument runs, give 3 more attempts if MSConvert fails
        if not db.is_completed_run(instrument_id, run_id):
            for attempt in range(3):
                if not os.path.exists(mzml_file):
                    print("MSConvert crashed, trying again in 3 minutes...")
                    time.sleep(180)
                    mzml_file = run_msconvert(path, filename, extension, mzml_file_directory)
                else:
                    break
    except:
        mzml_file = None
        print("Failed to run MSConvert.")
        traceback.print_exc()

    # Run MS-DIAL
    if mzml_file is not None:
        try:
            peak_list = run_msdial_processing(filename, msdial_directory, msdial_parameters,
                str(mzml_file_directory), str(qc_results_directory))
        except:
            peak_list = None
            print("Failed to run MS-DIAL.")
            traceback.print_exc()

    # Send peak list to MS-AutoQC algorithm if valid
    if mzml_file is not None and peak_list is not None:

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

        # Convert m/z, RT, and intensity data to dictionary records in string form
        try:
            mz_record, rt_record, intensity_record, qc_record = convert_to_dict(filename, df_peak_list, qc_dataframe)
        except:
            print("Failed to convert DataFrames to dictionary record format.")
            traceback.print_exc()
            return

        # Delete MS-DIAL result file
        try:
            os.remove(qc_results_directory + filename + ".msdial")
        except Exception as error:
            print("Failed to remove MS-DIAL result file.")
            traceback.print_exc()
            return

    else:
        print("Failed to process", filename)
        mz_record = None
        rt_record = None
        intensity_record = None
        qc_record = None
        qc_result = "Fail"
        peak_list = None

    # Send email and Slack notification (if they are enabled)
    try:
        if qc_result != "Pass":
            alert = "QC " + qc_result + ": " + filename
            if peak_list is None:
                alert = "Failed to process " + filename

            # Send Slack
            if db.slack_notifications_are_enabled():
                slack_bot.send_message(alert)

            # Send email
            if db.email_notifications_are_enabled():
                db.send_email(alert, "Please check on your instrument run accordingly.")

    except:
        print("Failed to send Slack notification.")
        traceback.print_exc()

    try:
        # Write QC results to database and upload to Google Drive
        db.write_qc_results(filename, instrument_id, run_id, mz_record, rt_record, intensity_record, qc_record, qc_result, is_bio_standard)

        # Update sample counters to trigger dashboard update
        db.update_sample_counters_for_run(instrument_id=instrument_id, run_id=run_id, latest_sample=filename)

        # If sync is enabled, upload the QC results to Google Drive
        if db.sync_is_enabled():
            db.upload_qc_results(instrument_id, run_id)

    except:
        print("Failed to write QC results to database.")
        traceback.print_exc()
        return


def subprocess_is_running(pid):

    """
    Returns True if subprocess is still running, and False if not.

    Args:
        pid (int): Subprocess ID

    Returns:
        bool: True if subprocess is still running, False if not
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


def kill_subprocess(pid):

    """
    Kills subprocess.

    Args:
        pid (int): Subprocess ID

    Returns:
        None
    """

    try:
        return psutil.Process(pid).kill()
    except Exception as error:
        print("Error killing acquisition listener.")
        traceback.print_exc()