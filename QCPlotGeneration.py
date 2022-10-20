import os, json
import plotly.express as px
import pandas as pd
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import DatabaseFunctions as db

# Authenticate with Google Drive
gauth = GoogleAuth()
drive = GoogleDrive(gauth)

# Get directory for files
current_directory = os.getcwd()

# Define client secrets file
GoogleAuth.DEFAULT_SETTINGS["client_config_file"] = current_directory + "/assets/client_secrets.json"

def get_qc_results(run_id):

    """
    Loads QC results for samples and biological standards from either the SQLite database or Google Drive
    """

    no_data = (None, None, None, None, None, None, None, None,
               None, None, None, None, None, None, None, None)

    positive = "Positive Mode"
    negative = "Negative Mode"

    # Get run metadata from database
    df_run = db.get_instrument_run(run_id)
    instrument = df_run["instrument_id"].values[0]
    chromatography = df_run["chromatography"].values[0]
    df_sequence = df_run["sequence"].values[0]
    df_metadata = df_run["metadata"].values[0]

    # Get internal standards in chromatography method
    pos_internal_standards = db.get_internal_standards_list(chromatography, positive)
    neg_internal_standards = db.get_internal_standards_list(chromatography, negative)
    precursor_mz_dict = db.get_internal_standards_dict(chromatography, "precursor_mz")
    retention_times_dict = db.get_internal_standards_dict(chromatography, "retention_time")

    resources = {
        "instrument": instrument,
        "run_id": run_id,
        "chromatography": chromatography,
        "pos_internal_standards": pos_internal_standards,
        "neg_internal_standards": neg_internal_standards,
        "precursor_mass_dict": precursor_mz_dict,
        "retention_times_dict": retention_times_dict,
        "ui_callback": False,
        "clicked_feature": False
    }

    # Overarching try/catch
    try:
        try:
            # Parse m/z, RT, and intensity data for internal standards into DataFrames
            df_mz_pos = db.parse_internal_standard_data(run_id=run_id, result_type="precursor_mz", polarity="Pos")
            df_rt_pos = db.parse_internal_standard_data(run_id=run_id, result_type="retention_time", polarity="Pos")
            df_intensity_pos = db.parse_internal_standard_data(run_id=run_id, result_type="intensity", polarity="Pos")
            df_mz_neg = db.parse_internal_standard_data(run_id=run_id, result_type="precursor_mz", polarity="Neg")
            df_rt_neg = db.parse_internal_standard_data(run_id=run_id, result_type="retention_time", polarity="Neg")
            df_intensity_neg = db.parse_internal_standard_data(run_id=run_id, result_type="intensity", polarity="Neg")

            # Parse m/z, RT, and intensity data for biological standards into DataFrames
            df_bio_mz_pos = db.parse_biological_standard_data(
                result_type="precursor_mz", polarity="Pos", biological_standard="Urine")
            df_bio_rt_pos = db.parse_biological_standard_data(
                result_type="retention_time", polarity="Pos", biological_standard="Urine")
            df_bio_intensity_pos = db.parse_biological_standard_data(
                result_type="intensity", polarity="Pos", biological_standard="Urine")
            df_bio_mz_neg = db.parse_biological_standard_data(
                result_type="precursor_mz", polarity="Neg", biological_standard="Urine")
            df_bio_rt_neg = db.parse_biological_standard_data(
                result_type="retention_time", polarity="Neg", biological_standard="Urine")
            df_bio_intensity_neg = db.parse_biological_standard_data(
                result_type="intensity", polarity="Neg", biological_standard="Urine")

            # Generate DataFrame for sample table
            df_samples = db.get_samples_in_run(run_id, "Both")
            df_samples = df_samples[["sample_id", "position", "qc_result"]]
            df_samples = df_samples.rename(
                columns={"sample_id": "Sample",
                "position": "Position",
                "qc_result": "QC"})
            df_samples = df_samples.to_json(orient="split")

        except Exception as error:
            print("Data retrieval error: " + str(error))
            return no_data

        return (df_rt_pos, df_rt_neg, df_intensity_pos, df_intensity_neg, df_mz_pos, df_mz_neg, df_sequence, df_metadata, \
        df_bio_rt_pos, df_bio_rt_neg, df_bio_intensity_pos, df_bio_intensity_neg, df_bio_mz_pos, df_bio_mz_neg, \
        json.dumps(resources), df_samples)

    except Exception as error:
        print("Data parsing error: " + str(error))
        return no_data


def generate_sample_metadata_dataframe(sample, df_istd_rt, df_istd_delta_mz, df_istd_intensity, df_sequence, df_metadata):

    """
    Creates a DataFrame for a single sample with m/z, RT, intensity and metadata info
    """

    df_sample_istd = pd.DataFrame()
    df_sample_info = pd.DataFrame()

    df_sequence = df_sequence.loc[df_sequence["File Name"].astype(str) == sample]
    df_metadata = df_metadata.loc[df_metadata["Filename"].astype(str) == sample]

    internal_standards = df_istd_rt.columns.tolist()
    retention_times = df_istd_rt[df_istd_rt["Sample"] == sample].values.tolist()[0]
    intensities = df_istd_intensity[df_istd_intensity["Sample"] == sample].fillna("0").values.tolist()[0]
    precursor_masses = df_istd_delta_mz[df_istd_delta_mz["Sample"] == sample].values.tolist()[0]

    for list in [internal_standards, retention_times, intensities, precursor_masses]:
        del list[0]

    df_sample_istd["Internal Standard"] = internal_standards
    df_sample_istd["m/z"] = precursor_masses
    df_sample_istd["RT"] = retention_times
    df_sample_istd["Intensity"] = ["{:.2e}".format(x) for x in intensities]
    # df_sample_istd["Delta RT"] = [x.split(": ")[-1] for x in retention_times]
    # df_sample_istd["Delta m/z"] = [x.split(": ")[-1] for x in mz_values]

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


def load_istd_rt_plot(dataframe, samples, internal_standard, retention_times_dict):

    """
    Returns scatter plot figure of retention time vs. sample for internal standards
    """

    df_filtered_by_samples = dataframe.loc[dataframe["Sample"].isin(samples)]

    y_min = retention_times_dict[internal_standard] - 0.1
    y_max = retention_times_dict[internal_standard] + 0.1
    # median_rt = dataframe[y].median()

    fig = px.line(df_filtered_by_samples,
                  title="Internal Standards RT – " + internal_standard,
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
                     margin=dict(t=75, b=75))
    fig.update_xaxes(showticklabels=False, title="Sample")
    fig.update_yaxes(title="Retention time (min)", range=[y_min, y_max])
    fig.add_hline(y=retention_times_dict[internal_standard], line_width=2, line_dash="dash")
    fig.update_traces(hovertemplate="Sample: %{x} <br>Retention Time: %{y} min<br>")

    return fig


def load_istd_intensity_plot(dataframe, samples, internal_standard, text, treatments):

    """
    Returns bar plot figure of intensity vs. sample for internal standards
    """

    df_filtered_by_samples = dataframe.loc[dataframe["Sample"].isin(samples)]

    if treatments:
        if len(treatments) == len(df_filtered_by_samples):
            df_filtered_by_samples["Treatment"] = treatments
    else:
        df_filtered_by_samples["Treatment"] = " "

    fig = px.bar(df_filtered_by_samples,
                 title="Internal Standards Intensity – " + internal_standard,
                 x=samples,
                 y=internal_standard,
                 text=text,
                 color=df_filtered_by_samples["Treatment"],
                 height=600)
    fig.update_layout(showlegend=False,
                      transition_duration=500,
                      clickmode="event",
                      xaxis=dict(rangeslider=dict(visible=True), autorange=True),
                      legend=dict(font=dict(size=10)),
                      margin=dict(t=75, b=75))
    fig.update_xaxes(showticklabels=False, title="Sample")
    fig.update_yaxes(title="Intensity")
    fig.update_traces(textposition="outside", hovertemplate="Sample: %{x}<br>Intensity: %{y:.2e}<br>")

    return fig


def load_istd_delta_mz_plot(dataframe, samples, internal_standard):

    """
    Returns scatter plot figure of delta m/z vs. sample for internal standards
    """

    # Get precursor m/z results for selected samples
    df_filtered_by_samples = dataframe.loc[dataframe["Sample"].isin(samples)]

    # Get delta m/z (experimental m/z minus reference m/z)
    df_istd = db.get_table("internal_standards")
    reference_mz = df_istd.loc[df_istd["name"] == internal_standard]["precursor_mz"].astype(float).values[0]
    df_filtered_by_samples[internal_standard] = df_filtered_by_samples[internal_standard].astype(float) - reference_mz

    fig = px.line(df_filtered_by_samples,
                  title="Delta m/z – " + internal_standard,
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
                      margin=dict(t=75, b=75))
    fig.update_xaxes(showticklabels=False, title="Sample")
    fig.update_yaxes(title="delta m/z", range=[-0.01, 0.01])
    fig.update_traces(hovertemplate="Sample: %{x} <br>Delta m/z: %{y}<br>")

    return fig


def load_bio_feature_plot(run_id, df_rt, df_mz, df_intensity):

    """
    Returns scatter plot figure of m/z vs. retention time for urine features
    """

    bio_df = pd.DataFrame()

    # Rename columns
    bio_df["Metabolite name"] = df_mz["Name"]
    bio_df["Precursor m/z"] = df_mz[run_id]
    bio_df["Retention time (min)"] = df_rt[run_id]
    bio_df["Intensity"] = df_intensity[run_id]

    # Get standard deviation of feature intensities
    df_intensity = df_intensity.fillna(0)
    feature_intensity_from_study = df_intensity.loc[:, run_id].astype(float)
    average_intensity_in_studies = df_intensity.iloc[:, 1:].astype(float).mean(axis=1)
    bio_df["% Change"] = ((feature_intensity_from_study - average_intensity_in_studies) / average_intensity_in_studies) * 100

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
                     title="QC Urine Metabolites",
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
                      margin=dict(t=75, b=75))
    fig.update_xaxes(title="Retention time (min)")
    fig.update_yaxes(title="Precursor m/z")
    fig.update_traces(marker={"size": 30})

    return fig


def load_bio_benchmark_plot(dataframe, feature_name):

    """
    Returns bar plot figure of intensity vs. study for biological standard features
    """

    # Get list of runs
    instrument_runs = dataframe.columns.tolist()
    del instrument_runs[0]

    # Get targeted metabolite intensities for each run
    intensities = dataframe.loc[dataframe["Name"] == feature_name].values.tolist()
    if len(intensities) != 0:
        intensities = intensities[0]
        del intensities[0]
    else:
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
    fig.update_layout(title="QC Urine Benchmark",
                      showlegend=False,
                      transition_duration=500,
                      clickmode="event",
                      xaxis=dict(rangeslider=dict(visible=True), autorange=True),
                      legend=dict(font=dict(size=10)),
                      margin=dict(t=75, b=75))
    fig.update_xaxes(title="Study")
    fig.update_yaxes(title="Intensity")
    fig.update_traces(textposition="outside",
                      hovertemplate=f"{feature_name}" + "<br>Study: %{x} <br>Intensity: %{text}<br>")

    return fig