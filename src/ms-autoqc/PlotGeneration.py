import os, json, ast, traceback, time
import plotly.express as px
import pandas as pd
import numpy as np
import DatabaseFunctions as db

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

def get_qc_results(instrument_id, run_id, status="Complete", drive=None, biological_standard=None,
                   biological_standards_only=False, for_benchmark_plot=False):

    """
    Loads and parses QC results for samples and biological standards from either CSV
    files (for active instrument runs) or the SQLite database (for completed runs)

    Input: instrument run ID and status
    Output: tuple of multiple tables encoded as JSON strings
    """

    start = time.time()

    # Download CSV files if instrument run is active
    if status == "Active" and drive is not None:
        if not db.is_instrument_computer():
            db.download_qc_results(drive, instrument_id, run_id)

    # Get run information / metadata
    if status == "Complete":
        df_run = db.get_instrument_run(instrument_id, run_id)
    elif status == "Active":
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
                result_type="precursor_mz", polarity="Pos", biological_standard=biological_standard, status=status)
        except Exception as error:
            print("Error loading positive (–) mode biological standard precursor m/z data:", error)
            df_bio_mz_pos = None

        try:
            df_bio_rt_pos = db.parse_biological_standard_data(instrument_id=instrument_id, run_id=run_id,
                result_type="retention_time", polarity="Pos", biological_standard=biological_standard, status=status)
        except Exception as error:
            print("Error loading positive (–) mode biological standard precursor m/z data:", error)
            df_bio_rt_pos = None

        try:
            df_bio_intensity_pos = db.parse_biological_standard_data(instrument_id=instrument_id, run_id=run_id,
                result_type="intensity", polarity="Pos", biological_standard=biological_standard, status=status)
        except Exception as error:
            print("Error loading positive (–) mode biological standard retention time data:", error)
            df_bio_intensity_pos = None

        try:
            df_bio_mz_neg = db.parse_biological_standard_data(instrument_id=instrument_id, run_id=run_id,
                result_type="precursor_mz", polarity="Neg", biological_standard=biological_standard, status=status)
        except Exception as error:
            print("Error loading negative (–) mode biological standard precursor m/z data:", error)
            df_bio_mz_neg = None

        try:
            df_bio_rt_neg = db.parse_biological_standard_data(instrument_id=instrument_id, run_id=run_id,
                result_type="retention_time", polarity="Neg", biological_standard=biological_standard, status=status)
        except Exception as error:
            print("Error loading positive (–) mode biological standard retention time data:", error)
            df_bio_rt_neg = None

        try:
            df_bio_intensity_neg = db.parse_biological_standard_data(instrument_id=instrument_id, run_id=run_id,
                result_type="intensity", polarity="Neg", biological_standard=biological_standard, status=status)
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
            run_id=run_id, result_type="precursor_mz", polarity="Pos", status=status)
    except Exception as error:
        print("Error loading positive (+) mode precursor m/z data:", error)
        df_mz_pos = None

    try:
        df_rt_pos = db.parse_internal_standard_data(instrument_id=instrument_id,
            run_id=run_id, result_type="retention_time", polarity="Pos", status=status)
    except Exception as error:
        print("Error loading positive (+) mode retention time data:", error)
        df_rt_pos = None

    try:
        df_intensity_pos = db.parse_internal_standard_data(instrument_id=instrument_id,
            run_id=run_id, result_type="intensity", polarity="Pos", status=status)
    except Exception as error:
        print("Error loading positive (+) mode intensity data:", error)
        df_intensity_pos = None

    try:
        df_mz_neg = db.parse_internal_standard_data(instrument_id=instrument_id,
            run_id=run_id, result_type="precursor_mz", polarity="Neg", status=status)
    except Exception as error:
        print("Error loading negative (–) mode precursor m/z data:", error)
        df_mz_neg = None

    try:
        df_rt_neg = db.parse_internal_standard_data(instrument_id=instrument_id,
            run_id=run_id, result_type="retention_time", polarity="Neg", status=status)
    except Exception as error:
        print("Error loading negative (–) mode retention time data:", error)
        df_rt_neg = None

    try:
        df_intensity_neg = db.parse_internal_standard_data(instrument_id=instrument_id,
            run_id=run_id, result_type="intensity", polarity="Neg", status=status)
    except Exception as error:
        print("Error loading negative (–) mode intensity data:", error)
        df_intensity_neg = None

    try:
        df_delta_rt_pos = db.parse_internal_standard_qc_data(instrument_id=instrument_id,
            run_id=run_id, result_type="Delta RT", polarity="Pos", status=status)
    except Exception as error:
        print("Error loading positive (+) mode delta RT data:", error)
        df_delta_rt_pos = None

    try:
        df_delta_rt_neg = db.parse_internal_standard_qc_data(instrument_id=instrument_id,
            run_id=run_id, result_type="Delta RT", polarity="Neg", status=status)
    except Exception as error:
        print("Error loading negative (–) mode delta RT data:", error)
        df_delta_rt_neg = None

    try:
        df_in_run_delta_rt_pos = db.parse_internal_standard_qc_data(instrument_id=instrument_id,
            run_id=run_id, result_type="In-run delta RT", polarity="Pos", status=status)
    except Exception as error:
        print("Error loading positive (+) mode in-run delta RT data:", error)
        df_in_run_delta_rt_pos = None

    try:
        df_in_run_delta_rt_neg = db.parse_internal_standard_qc_data(instrument_id=instrument_id,
            run_id=run_id, result_type="In-run delta RT", polarity="Neg", status=status)
    except Exception as error:
        print("Error loading negative (–) mode in-run delta RT data:", error)
        df_in_run_delta_rt_neg = None

    try:
        df_delta_mz_pos = db.parse_internal_standard_qc_data(instrument_id=instrument_id,
            run_id=run_id, result_type="Delta m/z", polarity="Pos", status=status)
    except Exception as error:
        print("Error loading positive (+) mode delta m/z data:", error)
        df_delta_mz_pos = None

    try:
        df_delta_mz_neg = db.parse_internal_standard_qc_data(instrument_id=instrument_id,
            run_id=run_id, result_type="Delta m/z", polarity="Neg", status=status)
    except Exception as error:
        print("Error loading negative (–) mode delta m/z data:", error)
        df_delta_mz_neg = None

    try:
        df_warnings_pos = db.parse_internal_standard_qc_data(instrument_id=instrument_id,
            run_id=run_id, result_type="Warnings", polarity="Pos", status=status)
    except Exception as error:
        print("Error loading positive (+) mode QC warnings data:", error)
        df_warnings_pos = None

    try:
        df_warnings_neg = db.parse_internal_standard_qc_data(instrument_id=instrument_id,
            run_id=run_id, result_type="Warnings", polarity="Neg", status=status)
    except Exception as error:
        print("Error loading negative (–) mode QC warnings data:", error)
        df_warnings_neg = None

    try:
        df_fails_pos = db.parse_internal_standard_qc_data(instrument_id=instrument_id,
            run_id=run_id, result_type="Fails", polarity="Pos", status=status)
    except Exception as error:
        print("Error loading positive (+) mode QC fails data:", error)
        df_fails_pos = None

    try:
        df_fails_neg = db.parse_internal_standard_qc_data(instrument_id=instrument_id,
            run_id=run_id, result_type="Fails", polarity="Neg", status=status)
    except Exception as error:
        print("Error loading negative (+) mode QC fails data:", error)
        df_fails_neg = None

    # Generate DataFrame for sample table
    try:
        if status == "Complete":
            df_samples = db.get_samples_in_run(instrument_id, run_id, "Both")
        elif status == "Active":
            df_samples = db.get_samples_from_csv(instrument_id, run_id, "Both")

        df_samples = df_samples[["sample_id", "position", "qc_result"]]
        df_samples = df_samples.rename(
            columns={
                "sample_id": "Sample",
                "position": "Position",
                "qc_result": "QC"})
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

    end = time.time()
    print("Loading time:", end - start)

    return (df_rt_pos, df_rt_neg, df_intensity_pos, df_intensity_neg, df_mz_pos, df_mz_neg, df_sequence, df_metadata,
        df_bio_rt_pos, df_bio_rt_neg, df_bio_intensity_pos, df_bio_intensity_neg, df_bio_mz_pos, df_bio_mz_neg,
        json.dumps(resources), df_samples, json.dumps(pos_internal_standards), json.dumps(neg_internal_standards),
        df_delta_rt_pos, df_delta_rt_neg, df_in_run_delta_rt_pos, df_in_run_delta_rt_neg, df_delta_mz_pos, df_delta_mz_neg,
        df_warnings_pos, df_warnings_neg, df_fails_pos, df_fails_neg)


def generate_sample_metadata_dataframe(sample, df_rt, df_mz, df_intensity, df_delta_rt, df_in_run_delta_rt,
    df_delta_mz, df_warnings, df_fails, df_sequence, df_metadata):

    """
    Aggregates and returns 3 DataFrames for a selected sample:
    1. QC result and causes
    2. Sequence and metadata information
    3. Internal standard m/z, RT, intensity, delta RT, and in-run delta RT
    """

    df_sample_istd = pd.DataFrame()
    df_sample_info = pd.DataFrame()


    # Get sequence and metadata
    df_sequence = df_sequence.loc[df_sequence["File Name"].astype(str) == sample]
    df_metadata = df_metadata.loc[df_metadata["Filename"].astype(str) == sample]

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
        df_sample_info["Sample ID"] = df_sequence["L1 Study"].astype(str).values
        df_sample_info["Position"] = df_sequence["Position"].astype(str).values
        df_sample_info["Injection Volume"] = df_sequence["Inj Vol"].astype(str).values + " uL"
        df_sample_info["Instrument Method"] = df_sequence["Instrument Method"].astype(str).values

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
    Aggregates and returns 2 DataFrames for a selected sample:
    1. QC result and causes
    2. Targeted metabolite m/z, RT, intensity, delta RT, and percent change
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
    Returns scatter plot figure of retention time vs. sample for internal standards
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
    fig.update_layout(transition_duration=500,
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
    Returns bar plot figure of intensity vs. sample for internal standards
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
    fig.update_layout(showlegend=False,
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
    Returns scatter plot figure of delta m/z vs. sample for internal standards
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
    fig.update_layout(transition_duration=500,
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
    Returns scatter plot figure of m/z vs. retention time for urine features
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

    # Get standard deviation of feature intensities
    df_intensity = df_intensity.fillna(0)
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
    fig.update_layout(showlegend=False,
                      transition_duration=500,
                      clickmode="event",
                      margin=dict(t=75, b=75, l=0, r=0))
    fig.update_xaxes(title="Retention time (min)")
    fig.update_yaxes(title="Precursor m/z")
    fig.update_traces(marker={"size": 30})

    return fig


def load_bio_benchmark_plot(dataframe, metabolite_name):

    """
    Returns bar plot figure of intensity vs. study for biological standard features
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

    fig = px.bar(x=instrument_runs,
                 y=intensities,
                 text=intensities_text,
                 height=600)
    fig.update_layout(title="Biological Standard Benchmark",
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
    Button functionality for seeking through internal standards
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