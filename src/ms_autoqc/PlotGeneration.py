import os, json, ast, traceback, time
import plotly.express as px
import pandas as pd
import numpy as np
import ms_autoqc.DatabaseFunctions as db

# Bootstrap color dictionary
bootstrap_colors = {
    "blue": "rgb(0, 123, 255)",
    "red": "rgb(220, 53, 69)",
    "green": "rgb(40, 167, 69)",
    "yellow": "rgb(255, 193, 7)",
    "blue-low-opacity": "rgba(0, 123, 255, 0.4)",
    "red-low-opacity": "rgba(220, 53, 69, 0.4)",
    "green-low-opacity": "rgba(40, 167, 69, 0.4)",
    "yellow-low-opacity": "rgba(255, 193, 7, 0.4)"
}

def get_qc_results(instrument_id, run_id, status="Complete", biological_standard=None, biological_standards_only=False, for_benchmark_plot=False):

    """
    Loads and parses QC results (for samples and biological standards) into Plotly graphs.

    This function will return whatever tables it can in a tuple, and fill None for the tables that throw errors in parsing.
    This is so that an error in retrieving one table will not prevent retrieving other tables.

    Depending on whether Google Drive sync is enabled, this function will load data from either CSV files
    (for active instrument runs) or the local instrument database (for completed runs).

    Regardless of whether Google Drive sync is enabled, the instrument computer (on which the run was started) will
    always load data from its local SQLite database.

    Args:
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (Job ID)
        status (str):
            QC job status, either "Active" or "Complete"
        biological_standard (str, default None):
            If specified, returns QC results for given biological standard associated with job
        biological_standards_only (bool, default False):
            If specified, returns QC results for biological standards only
        for_benchmark_plot (bool, default False):
            If specified, returns QC results specifically for biological standard benchmark plot

    Returns:
        tuple: Tuple containing tables of various sample data in JSON "records" format. Order is as follows:
            1. df_rt_pos: Retention times for internal standards in positive mode
            2. df_rt_neg: Retention times for internal standards in negative mode
            3. df_intensity_pos: Intensities for internal standards in positive mode
            4. df_intensity_neg: Intensities for internal standards in negative mode
            5. df_mz_pos: Precursor masses for internal standards in positive mode
            6. df_mz_neg: Precursor masses for internal standards in negative mode
            7. df_sequence: Acquisition sequence table
            8. df_metadata: Sample metadata table
            9. df_bio_rt_pos: Retention times for targeted features in biological standard sample in positive mode
            10. df_bio_rt_neg: Retention times for targeted features in biological standard sample in negative mode
            11. df_bio_intensity_pos: Intensities for targeted features in biological standard sample in positive mode
            12. df_bio_intensity_neg: Intensities for targeted features in biological standard sample in negative mode
            13. df_bio_mz_pos: Precursor masses for targeted features in biological standard sample in positive mode
            14. df_bio_mz_neg: Precursor masses for targeted features in biological standard sample in negative mode
            15. resources: Metadata for instrument run
            16. df_samples: Table containing sample names, polarities, autosampler positions, and QC results
            17. pos_internal_standards: List of positive mode internal standards
            18. neg_internal_standards: List of negative mode internal standards
            19. df_delta_rt_pos: Delta RT's for internal standards in positive mode
            20. df_delta_rt_neg: Delta RT's for internal standards in negative mode
            21. df_in_run_delta_rt_pos: In-run delta RT's for internal standards in positive mode
            22. df_in_run_delta_rt_neg: In-run delta RT's for internal standards in negative mode
            23. df_delta_mz_pos: Delta m/z's for internal standards in positive mode
            24. df_delta_mz_neg: Delta m/z's for internal standards in negative mode
            25. df_warnings_pos: QC warnings for internal standards in positive mode
            26. df_warnings_neg: QC warnings for internal standards in negative mode
            27. df_fails_pos: QC fails for internal standards in positive mode
            28. df_fails_neg: QC fails for internal standards in negative mode
    """

    # Get run information / metadata
    if db.get_device_identity() != instrument_id and db.sync_is_enabled():
        if status == "Complete":
            load_from = "database"
        elif status == "Active":
            load_from = "csv"
    else:
        load_from = "database"

    if load_from == "database":
        df_run = db.get_instrument_run(instrument_id, run_id)
    elif load_from == "csv":
        db.download_qc_results(instrument_id, run_id)
        df_run = db.get_instrument_run_from_csv(instrument_id, run_id)

    chromatography = df_run["chromatography"].values[0]
    df_sequence = df_run["sequence"].values[0]
    df_metadata = df_run["metadata"].values[0]
    completed = df_run["completed"].astype(int).tolist()[0]

    biological_standards = df_run["biological_standards"].values[0]
    if biological_standards is not None:
        biological_standards = ast.literal_eval(biological_standards)

    # Get internal standards in chromatography method
    precursor_mz_dict = db.get_internal_standards_dict(chromatography, "precursor_mz")
    retention_times_dict = db.get_internal_standards_dict(chromatography, "retention_time")

    resources = {
        "instrument": instrument_id,
        "run_id": run_id,
        "status": status,
        "chromatography": chromatography,
        "precursor_mass_dict": precursor_mz_dict,
        "retention_times_dict": retention_times_dict,
        "samples_completed": completed,
        "biological_standards": json.dumps(biological_standards)
    }

    # Parse m/z, RT, and intensity data for biological standards into DataFrames
    if biological_standards is not None:

        if biological_standard is None:
            biological_standard = biological_standards[0]

        try:
            df_bio_mz_pos = db.parse_biological_standard_data(instrument_id=instrument_id, run_id=run_id,
                result_type="precursor_mz", polarity="Pos", biological_standard=biological_standard, load_from=load_from)
        except Exception as error:
            print("Error loading positive (–) mode biological standard precursor m/z data:", error)
            df_bio_mz_pos = None

        try:
            df_bio_rt_pos = db.parse_biological_standard_data(instrument_id=instrument_id, run_id=run_id,
                result_type="retention_time", polarity="Pos", biological_standard=biological_standard, load_from=load_from)
        except Exception as error:
            print("Error loading positive (–) mode biological standard precursor m/z data:", error)
            df_bio_rt_pos = None

        try:
            df_bio_intensity_pos = db.parse_biological_standard_data(instrument_id=instrument_id, run_id=run_id,
                result_type="intensity", polarity="Pos", biological_standard=biological_standard, load_from=load_from)
        except Exception as error:
            print("Error loading positive (–) mode biological standard retention time data:", error)
            df_bio_intensity_pos = None

        try:
            df_bio_mz_neg = db.parse_biological_standard_data(instrument_id=instrument_id, run_id=run_id,
                result_type="precursor_mz", polarity="Neg", biological_standard=biological_standard, load_from=load_from)
        except Exception as error:
            print("Error loading negative (–) mode biological standard precursor m/z data:", error)
            df_bio_mz_neg = None

        try:
            df_bio_rt_neg = db.parse_biological_standard_data(instrument_id=instrument_id, run_id=run_id,
                result_type="retention_time", polarity="Neg", biological_standard=biological_standard, load_from=load_from)
        except Exception as error:
            print("Error loading positive (–) mode biological standard retention time data:", error)
            df_bio_rt_neg = None

        try:
            df_bio_intensity_neg = db.parse_biological_standard_data(instrument_id=instrument_id, run_id=run_id,
                result_type="intensity", polarity="Neg", biological_standard=biological_standard, load_from=load_from)
        except Exception as error:
            print("Error loading negative (–) mode biological standard intensity data:", error)
            df_bio_intensity_neg = None

    else:
        df_bio_mz_pos = None
        df_bio_rt_pos = None
        df_bio_intensity_pos = None
        df_bio_mz_neg = None
        df_bio_rt_neg = None
        df_bio_intensity_neg = None

    if biological_standards_only:
        return df_bio_rt_pos, df_bio_rt_neg, df_bio_intensity_pos, df_bio_intensity_neg, df_bio_mz_pos, df_bio_mz_neg
    elif for_benchmark_plot:
        return df_bio_intensity_pos, df_bio_intensity_neg

    # Parse m/z, RT, and intensity data for internal standards into DataFrames
    try:
        df_mz_pos = db.parse_internal_standard_data(instrument_id=instrument_id,
            run_id=run_id, result_type="precursor_mz", polarity="Pos", load_from=load_from)
    except Exception as error:
        print("Error loading positive (+) mode precursor m/z data:", error)
        df_mz_pos = None

    try:
        df_rt_pos = db.parse_internal_standard_data(instrument_id=instrument_id,
            run_id=run_id, result_type="retention_time", polarity="Pos", load_from=load_from)
    except Exception as error:
        print("Error loading positive (+) mode retention time data:", error)
        df_rt_pos = None

    try:
        df_intensity_pos = db.parse_internal_standard_data(instrument_id=instrument_id,
            run_id=run_id, result_type="intensity", polarity="Pos", load_from=load_from)
    except Exception as error:
        print("Error loading positive (+) mode intensity data:", error)
        df_intensity_pos = None

    try:
        df_mz_neg = db.parse_internal_standard_data(instrument_id=instrument_id,
            run_id=run_id, result_type="precursor_mz", polarity="Neg", load_from=load_from)
    except Exception as error:
        print("Error loading negative (–) mode precursor m/z data:", error)
        df_mz_neg = None

    try:
        df_rt_neg = db.parse_internal_standard_data(instrument_id=instrument_id,
            run_id=run_id, result_type="retention_time", polarity="Neg", load_from=load_from)
    except Exception as error:
        print("Error loading negative (–) mode retention time data:", error)
        df_rt_neg = None

    try:
        df_intensity_neg = db.parse_internal_standard_data(instrument_id=instrument_id,
            run_id=run_id, result_type="intensity", polarity="Neg", load_from=load_from)
    except Exception as error:
        print("Error loading negative (–) mode intensity data:", error)
        df_intensity_neg = None

    try:
        df_delta_rt_pos = db.parse_internal_standard_qc_data(instrument_id=instrument_id,
            run_id=run_id, result_type="Delta RT", polarity="Pos", load_from=load_from)
    except Exception as error:
        print("Error loading positive (+) mode delta RT data:", error)
        df_delta_rt_pos = None

    try:
        df_delta_rt_neg = db.parse_internal_standard_qc_data(instrument_id=instrument_id,
            run_id=run_id, result_type="Delta RT", polarity="Neg", load_from=load_from)
    except Exception as error:
        print("Error loading negative (–) mode delta RT data:", error)
        df_delta_rt_neg = None

    try:
        df_in_run_delta_rt_pos = db.parse_internal_standard_qc_data(instrument_id=instrument_id,
            run_id=run_id, result_type="In-run delta RT", polarity="Pos", load_from=load_from)
    except Exception as error:
        print("Error loading positive (+) mode in-run delta RT data:", error)
        df_in_run_delta_rt_pos = None

    try:
        df_in_run_delta_rt_neg = db.parse_internal_standard_qc_data(instrument_id=instrument_id,
            run_id=run_id, result_type="In-run delta RT", polarity="Neg", load_from=load_from)
    except Exception as error:
        print("Error loading negative (–) mode in-run delta RT data:", error)
        df_in_run_delta_rt_neg = None

    try:
        df_delta_mz_pos = db.parse_internal_standard_qc_data(instrument_id=instrument_id,
            run_id=run_id, result_type="Delta m/z", polarity="Pos", load_from=load_from)
    except Exception as error:
        print("Error loading positive (+) mode delta m/z data:", error)
        df_delta_mz_pos = None

    try:
        df_delta_mz_neg = db.parse_internal_standard_qc_data(instrument_id=instrument_id,
            run_id=run_id, result_type="Delta m/z", polarity="Neg", load_from=load_from)
    except Exception as error:
        print("Error loading negative (–) mode delta m/z data:", error)
        df_delta_mz_neg = None

    try:
        df_warnings_pos = db.parse_internal_standard_qc_data(instrument_id=instrument_id,
            run_id=run_id, result_type="Warnings", polarity="Pos", load_from=load_from)
    except Exception as error:
        print("Error loading positive (+) mode QC warnings data:", error)
        df_warnings_pos = None

    try:
        df_warnings_neg = db.parse_internal_standard_qc_data(instrument_id=instrument_id,
            run_id=run_id, result_type="Warnings", polarity="Neg", load_from=load_from)
    except Exception as error:
        print("Error loading negative (–) mode QC warnings data:", error)
        df_warnings_neg = None

    try:
        df_fails_pos = db.parse_internal_standard_qc_data(instrument_id=instrument_id,
            run_id=run_id, result_type="Fails", polarity="Pos", load_from=load_from)
    except Exception as error:
        print("Error loading positive (+) mode QC fails data:", error)
        df_fails_pos = None

    try:
        df_fails_neg = db.parse_internal_standard_qc_data(instrument_id=instrument_id,
            run_id=run_id, result_type="Fails", polarity="Neg", load_from=load_from)
    except Exception as error:
        print("Error loading negative (+) mode QC fails data:", error)
        df_fails_neg = None

    # Generate DataFrame for sample table
    try:
        if load_from == "database":
            df_samples = db.get_samples_in_run(instrument_id, run_id, "Both")
        elif load_from == "csv":
            df_samples = db.get_samples_from_csv(instrument_id, run_id, "Both")

        df_samples = df_samples[["sample_id", "position", "qc_result", "polarity"]]
        df_samples = df_samples.rename(
            columns={
                "sample_id": "Sample",
                "position": "Position",
                "qc_result": "QC",
                "polarity": "Polarity"})
        df_samples = df_samples.to_json(orient="records")

    except Exception as error:
        print("Error loading samples from database:", error)
        traceback.print_exc()
        df_samples = ""

    # Get internal standards from data
    if df_rt_pos is not None:
        pos_internal_standards = pd.read_json(df_rt_pos, orient="records").columns.tolist()
        pos_internal_standards.remove("Sample")
    else:
        pos_internal_standards = []

    if df_rt_neg is not None:
        neg_internal_standards = pd.read_json(df_rt_neg, orient="records").columns.tolist()
        neg_internal_standards.remove("Sample")
    else:
        neg_internal_standards = []

    return (df_rt_pos, df_rt_neg, df_intensity_pos, df_intensity_neg, df_mz_pos, df_mz_neg, df_sequence, df_metadata,
        df_bio_rt_pos, df_bio_rt_neg, df_bio_intensity_pos, df_bio_intensity_neg, df_bio_mz_pos, df_bio_mz_neg,
        json.dumps(resources), df_samples, json.dumps(pos_internal_standards), json.dumps(neg_internal_standards),
        df_delta_rt_pos, df_delta_rt_neg, df_in_run_delta_rt_pos, df_in_run_delta_rt_neg, df_delta_mz_pos, df_delta_mz_neg,
        df_warnings_pos, df_warnings_neg, df_fails_pos, df_fails_neg)


def generate_sample_metadata_dataframe(sample, df_rt, df_mz, df_intensity, df_delta_rt, df_in_run_delta_rt,
    df_delta_mz, df_warnings, df_fails, df_sequence, df_metadata):

    """
    Aggregates tables of relevant data from the acquisition sequence, metadata file, and QC results for a selected sample.

    Returns two DataFrames by aggregating the following information:
        1. Acquisition sequence and sample metadata information
        2. Internal standard m/z, RT, intensity, delta m/z, delta RT, in-run delta RT, warnings, and fails

    Args:
        sample (str):
            Sample ID
        df_rt (DataFrame):
            Retention times for internal standards (columns) across samples (rows)
        df_mz (DataFrame):
            Precursor masses for internal standards (columns) across samples (rows)
        df_intensity (DataFrame):
            Intensities for internal standards (columns) across samples (rows)
        df_delta_rt (DataFrame):
            Delta RT's from library values for internal standards (columns) across samples (rows)
        df_in_run_delta_rt (DataFrame):
            Delta RT's from in-run values for internal standards (columns) across samples (rows)
        df_delta_mz (DataFrame):
            Delta m/z's from library values for internal standards (columns) across samples (rows)
        df_warnings (DataFrame):
            QC warnings for internal standards (columns) across samples (rows)
        df_fails (DataFrame):
            QC fails for internal standards (columns) across samples (rows)
        df_sequence (DataFrame):
            Acquisition sequence table
        df_metadata (DataFrame):
            Sample metadata table

    Returns:
        Tuple containing two DataFrames, the first storing internal standard data and the second storing sample metadata.
    """

    df_sample_istd = pd.DataFrame()
    df_sample_info = pd.DataFrame()

    # Index the selected sample, then make sure all columns in all dataframes are in the same order
    columns = df_rt.columns.tolist()
    internal_standards = df_rt.columns.tolist()
    internal_standards.remove("Sample")
    df_sample_istd["Internal Standard"] = internal_standards

    # Precursor m/z
    df_mz = df_mz.loc[df_mz["Sample"] == sample][columns]
    df_mz.drop(columns=["Sample"], inplace=True)
    df_sample_istd["m/z"] = df_mz.iloc[0].astype(float).values.tolist()

    # Retention times
    df_rt = df_rt.loc[df_rt["Sample"] == sample][columns]
    df_rt.drop(columns=["Sample"], inplace=True)
    df_sample_istd["RT"] = df_rt.iloc[0].astype(float).round(2).values.tolist()

    # Intensities
    df_intensity = df_intensity.loc[df_intensity["Sample"] == sample][columns]
    df_intensity.drop(columns=["Sample"], inplace=True)
    intensities = df_intensity.iloc[0].fillna(0).values.tolist()
    df_sample_istd["Intensity"] = ["{:.2e}".format(x) for x in intensities]

    # Delta m/z
    df_delta_mz.replace(" ", np.nan, inplace=True)
    df_delta_mz = df_delta_mz.loc[df_delta_mz["Sample"] == sample][columns]
    df_delta_mz.drop(columns=["Sample"], inplace=True)
    df_sample_istd["Delta m/z"] = df_delta_mz.iloc[0].astype(float).round(6).values.tolist()

    # Delta RT
    df_delta_rt.replace(" ", np.nan, inplace=True)
    df_delta_rt = df_delta_rt.loc[df_delta_rt["Sample"] == sample][columns]
    df_delta_rt.drop(columns=["Sample"], inplace=True)
    df_sample_istd["Delta RT"] = df_delta_rt.iloc[0].astype(float).round(3).values.tolist()

    # In-run delta RT
    df_in_run_delta_rt.replace(" ", np.nan, inplace=True)
    df_in_run_delta_rt = df_in_run_delta_rt.loc[df_in_run_delta_rt["Sample"] == sample][columns]
    df_in_run_delta_rt.drop(columns=["Sample"], inplace=True)
    df_sample_istd["In-Run Delta RT"] = df_in_run_delta_rt.iloc[0].astype(float).round(3).values.tolist()

    # Warnings
    df_warnings.replace(" ", np.nan, inplace=True)
    df_warnings = df_warnings.loc[df_warnings["Sample"] == sample][columns]
    df_warnings.drop(columns=["Sample"], inplace=True)
    df_sample_istd["Warnings"] = df_warnings.iloc[0].astype(str).values.tolist()

    # Fails
    df_fails.replace(" ", np.nan, inplace=True)
    df_fails = df_fails.loc[df_fails["Sample"] == sample][columns]
    df_fails.drop(columns=["Sample"], inplace=True)
    df_sample_istd["Fails"] = df_fails.iloc[0].astype(str).values.tolist()

    if len(df_sequence) > 0:
        df_sequence = df_sequence.loc[df_sequence["File Name"].astype(str) == sample]
        df_sample_info["Sample ID"] = df_sequence["L1 Study"].astype(str).values
        df_sample_info["Position"] = df_sequence["Position"].astype(str).values
        df_sample_info["Injection Volume"] = df_sequence["Inj Vol"].astype(str).values + " uL"
        df_sample_info["Instrument Method"] = df_sequence["Instrument Method"].astype(str).values

    if len(df_metadata) > 0:
        df_metadata = df_metadata.loc[df_metadata["Filename"].astype(str) == sample]
        if len(df_metadata) > 0:
            df_sample_info["Species"] = df_metadata["Species"].astype(str).values
            df_sample_info["Matrix"] = df_metadata["Matrix"].astype(str).values
            df_sample_info["Growth-Harvest Conditions"] = df_metadata["Growth-Harvest Conditions"].astype(str).values
            df_sample_info["Treatment"] = df_metadata["Treatment"].astype(str).values

    df_sample_info = df_sample_info.append(df_sample_info.iloc[0])
    df_sample_info.iloc[0] = df_sample_info.columns.tolist()
    df_sample_info = df_sample_info.rename(index={0: "Sample Information"})
    df_sample_info = df_sample_info.transpose()

    return df_sample_istd, df_sample_info


def generate_bio_standard_dataframe(clicked_sample, instrument_id, run_id, df_rt, df_mz, df_intensity):

    """
    Aggregates data for a selected biological standard.

    TODO: More metrics could be added to sample information cards for biological standards here.

    Aggregates and returns 2 DataFrames for a selected sample:
        1. QC result and causes
        2. Targeted metabolite m/z, RT, intensity, delta RT, and percent change

    Args:
        clicked_sample (str):
            Sample ID
        instrument_id (str):
            Instrument ID
        run_id (str):
            Instrument run ID (job ID)
        df_rt (DataFrame):
            Retention times of targeted metabolites in the biological standard
        df_mz (DataFrame):
            Precursor masses of targeted metabolites in the biological standard
        df_intensity:
            Intensities of targeted metabolites in the biological standard

    Returns:
        Tuple containing two DataFrames, the first storing targeted metabolites data and the second storing sample metadata.
    """

    metabolites = df_mz.columns.tolist()
    metabolites.remove("Name")

    df_sample_features = pd.DataFrame()
    df_sample_features["Metabolite name"] = metabolites
    df_sample_features["Precursor m/z"] = df_mz[metabolites].iloc[0].astype(float).values
    df_sample_features["Retention time (min)"] = df_rt[metabolites].iloc[0].astype(float).round(3).values
    intensities = df_intensity[metabolites].iloc[0].fillna(0).astype(float).values.tolist()
    df_sample_features["Intensity"] = ["{:.2e}".format(x) for x in intensities]

    df_sample_info = pd.DataFrame()
    df_sample_info["Sample ID"] = [clicked_sample]
    qc_result = db.get_qc_results(
        instrument_id=instrument_id, sample_list=[clicked_sample], is_bio_standard=True)["qc_result"].values[0]
    df_sample_info["QC Result"] = [qc_result]

    df_sample_info = df_sample_info.append(df_sample_info.iloc[0])
    df_sample_info.iloc[0] = df_sample_info.columns.tolist()
    df_sample_info = df_sample_info.rename(index={0: "Sample Information"})
    df_sample_info = df_sample_info.transpose()

    return df_sample_features, df_sample_info


def load_istd_rt_plot(dataframe, samples, internal_standard, retention_times):

    """
    Returns line plot figure of retention times (for a selected internal standard) across samples.

    Documentation on Plotly line plots: https://plotly.com/python-api-reference/generated/plotly.express.line.html

    Args:
        dataframe (DataFrame):
            Table of retention times for internal standards (columns) across samples (rows)
        samples (list):
            Samples to query from the DataFrame
        internal_standard (str):
            The selected internal standard
        retention_times (dict):
            Dictionary with key-value pairs of type { internal_standard: retention_time }

    Returns:
        plotly.express.line object: Plotly line plot of retention times (for the selected internal standard) across samples.
    """

    df_filtered_by_samples = dataframe.loc[dataframe["Sample"].isin(samples)]
    df_filtered_by_samples[internal_standard] = df_filtered_by_samples[internal_standard].astype(float).round(3)

    y_min = retention_times[internal_standard] - 0.1
    y_max = retention_times[internal_standard] + 0.1

    fig = px.line(df_filtered_by_samples,
        title="Retention Time vs. Samples – " + internal_standard,
        x=samples,
        y=internal_standard,
        height=600,
        markers=True,
        hover_name=samples,
        labels={"variable": "Internal Standard",
              "index": "Sample",
              "value": "Retention Time"},
        log_x=False)
    fig.update_layout(
        transition_duration=500,
        clickmode="event",
        showlegend=False,
        legend_title_text="Internal Standards",
        margin=dict(t=75, b=75, l=0, r=0))
    fig.update_xaxes(showticklabels=False, title="Sample")
    fig.update_yaxes(title="Retention Time (min)", range=[y_min, y_max])
    fig.add_hline(y=retention_times[internal_standard], line_width=2, line_dash="dash")
    fig.update_traces(hovertemplate="Sample: %{x} <br>Retention Time: %{y} min<br>")

    return fig


def load_istd_intensity_plot(dataframe, samples, internal_standard, treatments):

    """
    Returns bar plot figure of peak intensities (for a selected internal standard) across samples.

    Documentation on Plotly bar plots: https://plotly.com/python-api-reference/generated/plotly.express.bar.html

    Args:
        dataframe (DataFrame):
            Table of intensities for internal standards (columns) across samples (rows)
        samples (list):
            Samples to query from the DataFrame
        internal_standard (str):
            The selected internal standard
        treatments (DataFrame):
            DataFrame with sample treatments (from the metadata file) mapped to sample ID's

    Returns:
        plotly.express.bar object: Plotly bar plot of intensities (for the selected internal standard) across samples.
    """

    df_filtered_by_samples = dataframe.loc[dataframe["Sample"].isin(samples)]

    if len(treatments) > 0:
        # Map treatments to sample names
        df_mapped = pd.DataFrame()
        df_mapped["Sample"] = df_filtered_by_samples["Sample"]
        df_mapped["Treatment"] = df_mapped.replace(
            treatments.set_index("Filename")["Treatment"])
        df_filtered_by_samples["Treatment"] = df_mapped["Treatment"].astype(str)
    else:
        df_filtered_by_samples["Treatment"] = " "

    fig = px.bar(df_filtered_by_samples,
        title="Intensity vs. Samples – " + internal_standard,
        x="Sample",
        y=internal_standard,
        text="Sample",
        color="Treatment",
        height=600)
    fig.update_layout(
        showlegend=False,
        transition_duration=500,
        clickmode="event",
        xaxis=dict(rangeslider=dict(visible=True), autorange=True),
        legend=dict(font=dict(size=10)),
        margin=dict(t=75, b=75, l=0, r=0))
    fig.update_xaxes(showticklabels=False, title="Sample")
    fig.update_yaxes(title="Intensity")
    fig.update_traces(textposition="outside", hovertemplate="Sample: %{x}<br>Intensity: %{y:.2e}<br>")

    return fig


def load_istd_delta_mz_plot(dataframe, samples, internal_standard):

    """
    Returns line plot figure of delta m/z (for a selected internal standard) across samples.

    Documentation on Plotly line plots: https://plotly.com/python-api-reference/generated/plotly.express.line.html

    Args:
        dataframe (DataFrame):
            Table of delta m/z's for internal standards (columns) across samples (rows)
        samples (list):
            Samples to query from the DataFrame
        internal_standard (str):
            The selected internal standard

    Returns:
        plotly.express.line object: Plotly line plot of delta m/z (for the selected internal standard) across samples.
    """

    # Get delta m/z results for selected samples
    df_filtered_by_samples = dataframe.loc[dataframe["Sample"].isin(samples)]

    fig = px.line(df_filtered_by_samples,
        title="Delta m/z vs. Samples – " + internal_standard,
        x=samples,
        y=internal_standard,
        height=600,
        markers=True,
        hover_name=samples,
        labels={"variable": "Internal Standard",
              "index": "Sample",
              "value": "Delta m/z"},
        log_x=False)
    fig.update_layout(
        transition_duration=500,
        clickmode="event",
        showlegend=False,
        legend_title_text="Internal Standards",
        margin=dict(t=75, b=75, l=0, r=0))
    fig.update_xaxes(showticklabels=False, title="Sample")
    fig.update_yaxes(title="delta m/z", range=[-0.01, 0.01])
    fig.update_traces(hovertemplate="Sample: %{x} <br>Delta m/z: %{y}<br>")

    return fig


def load_bio_feature_plot(run_id, df_rt, df_mz, df_intensity):

    """
    Returns scatter plot figure of precursor m/z vs. retention time for targeted features in the biological standard.

    To further clarify:
        x-axis: retention times
        y-axis: precursor masses
        colorscale: percent change in intensity for each feature compared to the average intensity across all runs

    Documentation on Plotly scatter plots: https://plotly.com/python-api-reference/generated/plotly.express.scatter.html

    Args:
        run_id (str):
            Run ID to query the biological standard from
        df_rt (DataFrame):
            Table of retention times for targeted features (columns) across instrument runs (rows)
        df_mz (DataFrame):
            Table of precursor masses for targeted features (columns) across instrument runs (rows)
        df_intensity (DataFrame):
            Table of intensities for targeted features (columns) across instrument runs (rows)

    Returns:
        plotly.express.scatter object: m/z - RT scatter plot for targeted metabolites in the biological standard
    """

    # Get metabolites
    metabolites = df_mz.columns.tolist()
    del metabolites[0]

    # Construct new DataFrame
    bio_df = pd.DataFrame()
    bio_df["Metabolite name"] = metabolites
    bio_df["Precursor m/z"] = df_mz.loc[df_mz["Name"] == run_id][metabolites].iloc[0].astype(float).values
    bio_df["Retention time (min)"] =  df_rt.loc[df_rt["Name"] == run_id][metabolites].iloc[0].astype(float).values
    bio_df["Intensity"] =  df_intensity.loc[df_intensity["Name"] == run_id][metabolites].iloc[0].astype(float).values

    # Get percent change of feature intensities (only for runs previous to this one)
    df_intensity = df_intensity.fillna(0)

    try:
        index_of_run = df_intensity.loc[df_intensity["Name"] == run_id].index.tolist()[0]
        df_intensity = df_intensity[0:index_of_run + 1]
    finally:
        feature_intensity_from_study = df_intensity.loc[df_intensity["Name"] == run_id][metabolites].iloc[0].astype(float).values

    if len(df_intensity) > 1:
        average_intensity_in_studies = df_intensity.loc[df_intensity["Name"] != run_id][metabolites].astype(float).mean().values
        bio_df["% Change"] = ((feature_intensity_from_study - average_intensity_in_studies) / average_intensity_in_studies) * 100
        bio_df.replace(np.inf, 100, inplace=True)
        bio_df.replace(-np.inf, -100, inplace=True)
    else:
        bio_df["% Change"] = 0

    # Plot readiness
    bio_df["Retention time (min)"] = bio_df["Retention time (min)"].round(2)
    bio_df["% Change"] = bio_df["% Change"].round(1).fillna(0)

    labels = {"Retention time (min)": "Retention time (min)",
              "Precursor m/z": "Precursor m/z",
              "Intensity": "Intensity",
              "Metabolite name": "Metabolite name"}

    # Colorscale
    diverging_colorscale = ["#1a88ff", "#3395ff", "#4da3ff", "#a186ca", "#e7727d", "#e35d6a", "#e04958"]
    diverging_colorscale.reverse()

    fig = px.scatter(bio_df,
        title="Biological Standard – Targeted Metabolites",
        x="Retention time (min)",
        y="Precursor m/z",
        height=600,
        hover_name="Metabolite name",
        color="% Change",
        color_continuous_scale=diverging_colorscale,
        labels=labels,
        log_x=False,
        range_color=[-100, 100])
    fig.update_layout(
        showlegend=False,
        transition_duration=500,
        clickmode="event",
        margin=dict(t=75, b=75, l=0, r=0))
    fig.update_xaxes(title="Retention time (min)")
    fig.update_yaxes(title="Precursor m/z")
    fig.update_traces(marker={"size": 30})

    return fig


def load_bio_benchmark_plot(dataframe, metabolite_name):

    """
    Returns bar plot figure of intensities for a targeted metabolite in a biological standard across instrument runs.

    Documentation on Plotly bar plots: https://plotly.com/python-api-reference/generated/plotly.express.bar.html

    Args:
        dataframe (DataFrame):
            Table of intensities for targeted metabolites (columns) across instrument runs (rows)
        metabolite_name (str):
            The targeted metabolite to query from the DataFrame

    Returns:
        plotly.express.bar object: Plotly bar plot of intensities (for the selected targeted metabolite) across instrument runs.
    """

    # Get list of runs
    instrument_runs = dataframe["Name"].astype(str).tolist()

    # Get targeted metabolite intensities for each run
    intensities = dataframe[metabolite_name].values.tolist()
    if len(intensities) == 0:
        intensities = [0 for x in instrument_runs]

    # Get intensities in scientific notation for labeling bar plot
    if intensities is not None:
        intensities_text = ["{:.2e}".format(x) for x in intensities]
    else:
        intensities_text = []

    fig = px.bar(
        x=instrument_runs,
        y=intensities,
        text=intensities_text,
        height=600)
    fig.update_layout(
        title="Biological Standard Benchmark",
        showlegend=False,
        transition_duration=500,
        clickmode="event",
        xaxis=dict(rangeslider=dict(visible=True), autorange=True),
        legend=dict(font=dict(size=10)),
        margin=dict(t=75, b=75, l=0, r=0))
    fig.update_xaxes(title="Study")
    fig.update_yaxes(title="Intensity")
    fig.update_traces(textposition="outside",
                      hovertemplate=f"{metabolite_name}" + "<br>Study: %{x} <br>Intensity: %{text}<br>")

    return fig


def get_internal_standard_index(previous, next, max):

    """
    Button functionality for seeking through internal standards.

    Uses n_clicks from the previous and next buttons to generate an index, which is used to index a list of internal
    standards in the populate_istd_rt_plot(), populate_istd_intensity_plot(), and populate_istd_mz_plot() callback
    functions of the DashWebApp module.

    This function relies on the previous button's n_clicks to be reset to None on every click.

    Args:
        previous (int):
            n_clicks for the "previous" button (None, unless previous button is clicked)
        next (int):
            n_clicks for the "next" button
        max (int):
            Number of internal standards (maximum index for list of internal standards)

    Returns:
        Integer index for a list of internal standards.
    """

    if previous is not None:
        if next is None or next == 0:
            return max - 1

    if previous is None:
        if next is None:
            index = 0
        else:
            index = next
    elif previous is not None:
        index = next - previous

    if index < 0 or index >= max:
        index = 0

    return index