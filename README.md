# MS-AutoQC
**MS-AutoQC** is an all-in-one solution for automated quality control of liquid chromatography-mass spectrometry (LC-MS) data, both during and after data acquisition.

![](https://user-images.githubusercontent.com/7220175/200887970-d8bcc29e-3a9a-4a51-912a-56a223d50f8a.png)
<br>

It offers a fast, straightforward approach to ensure collection of high-quality data, allowing for less time investigating raw data and more time conducting experiments.

Developed at the [Mass Spectrometry Platform of CZ Biohub San Francisco](https://www.czbiohub.org/mass-spec/), MS-AutoQC provides a host of key features to streamline untargeted metabolomics research, such as:

- **Automated and user-defined quality control checks** during instrument runs
- **Realtime updates on QC fails** in the form of Slack or email notifications
- **Interactive data visualization** of internal standard retention time, _m/z_, and intensity across samples
- **Google Drive cloud sync** and secure, Google-authenticated access to QC results from any device

# Requirements
**MS-AutoQC was designed to run on Windows platforms** because of its dependency on [MSConvert](https://proteowizard.sourceforge.io/tools/msconvert.html) for vendor format data conversion and [MS-DIAL](http://prime.psc.riken.jp/compms/msdial/main.html) for data processing and identification. However, MacOS users can still use MS-AutoQC to monitor / view their instrument run data.

In addition, MS-AutoQC requires Python 3.8+ and various Python packages, including Pandas, SQLAlchemy, Plotly, Dash, Bootstrap, Watchdog, Google API, and Slack API. These are installed automatically during setup.

**Note:** Installation of Python and various Python packages on MS instrument computers comes at no risk. For extra security and peace of mind, you can opt to install MS-AutoQC in a virtual environment. To learn more, please read the [installation guide](https://czbiohub.github.io/MS-AutoQC/installation.html).

# Installation
Installing MS-AutoQC is easy. Simply open your Terminal or Command Prompt and type:
```python
pip install ms-autoqc
```

Python dependencies are installed automatically, but dependencies such as MSConvert and MS-DIAL will need to be installed manually. Check out the [installation guide](https://czbiohub.github.io/MS-AutoQC/installation.html) for more details.

If necessary, you can download and install MS-AutoQC v1.0.0 [manually from GitHub](https://github.com/czbiohub/MS-AutoQC/releases) – although we strongly recommend using pip!

Please keep in mind that MS-AutoQC is still in beta development. If you encounter a bug or issue, please report it by [opening an issue on GitHub](https://github.com/czbiohub/MS-AutoQC/issues).

# Supported Vendors
MS-AutoQC was designed to be a universal, open-source solution for data quality control. Because MSConvert converts raw acquired data into open mzML format before routing it to the data processing pipeline, the package will work seamlessly with data of all vendor formats.

**However, MS-AutoQC has only been tested extensively on Thermo Fisher mass spectrometers and Thermo RAW files.** As such, it is expected that there may be bugs / issues with processing data of other vendor formats.

We are welcome to collaboration! If you would like to help us comprehensively test support on Agilent / Bruker / Sciex / Waters instruments, please send an email to [brian.defelice@czbiohub.org](mailto:brian.defelice@czbiohub.org).
