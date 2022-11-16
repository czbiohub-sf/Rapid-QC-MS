# MS-AutoQC
**MS-AutoQC** is an all-in-one solution for automated quality control of liquid chromatography-mass spectrometry (LC-MS) data, either during or after data acquisition.

It offers a fast, straightforward approach to ensure collection of high-quality data, allowing for less time extracting ion chromatograms and more time conducting experiments.

<img width="1624" alt="Screenshot 2022-11-09 at 8 37 05 AM" src="https://user-images.githubusercontent.com/7220175/200887970-d8bcc29e-3a9a-4a51-912a-56a223d50f8a.png"><br>

# Features
Developed at the [Mass Spectrometry Platform of CZ Biohub San Francisco](https://www.czbiohub.org/mass-spec/), MS-AutoQC provides a host of key features to streamline untargeted metabolomics research, such as:

- Automated and user-defined quality control checks during instrument runs
- Realtime updates on QC fails in the form of Slack or email notifications
- Interactive data visualization of internal standard retention time, precursor _m/z_, and intensity across samples
- Google Drive cloud sync and secure, Google-authenticated access to QC results from any device
<br>

![MANA 2022 Poster](https://user-images.githubusercontent.com/7220175/190062493-129b2640-2e60-4787-8460-16f653655365.jpg)

# Navigation
1. [Installation](https://github.com/czbiohub/MS-AutoQC/wiki/Installation)
2. [Getting Started](https://github.com/czbiohub/MS-AutoQC/wiki/Getting-Started)
3. [Features](https://github.com/czbiohub/MS-AutoQC/wiki/Features)
4. [Troubleshooting](https://github.com/czbiohub/MS-AutoQC/wiki/Troubleshooting)
5. [Frequently Asked Questions](https://github.com/czbiohub/MS-AutoQC/wiki/FAQ)
6. [Documentation](https://github.com/czbiohub/MS-AutoQC/wiki/Documentation)
7. [Source](https://github.com/czbiohub/MS-AutoQC)

# Download
Download the latest version of MS-AutoQC here: https://github.com/czbiohub/MS-AutoQC/releases

# Requirements
**MS-AutoQC was designed to run on Windows platforms** because of its dependency on [MSConvert](https://proteowizard.sourceforge.io/tools/msconvert.html) for vendor format data conversion and [MS-DIAL](http://prime.psc.riken.jp/compms/msdial/main.html) for data processing and identification.

**MacOS users cannot setup MS-AutoQC jobs.** However, MS-AutoQC is a local web app, so MacOS users can still log in to their workspace to view QC data visualizations, as well as to configure instrument settings.

In addition, MS-AutoQC requires Python 3.9+ and various Python packages, including Pandas, SQLAlchemy, Plotly, Dash, Bootstrap, Watchdog, and the Google Drive and Slack API. These are installed automatically during setup.

Installation of Python and various Python packages on MS instrument computers comes at no risk, but MS-AutoQC can be run and installed on an separate network computer if preferred. To learn more, please read the [installation guide](https://github.com/czbiohub/MS-AutoQC/wiki/Installation).

# Supported Vendors
MS-AutoQC was designed to be a universal tool. Because MSConvert converts a copy of the raw data into mzML format before performing the rest of its functions, the package should work seamlessly with data of all vendor formats.

**However, MS-AutoQC has only been tested extensively on Thermo Fisher mass spectrometers and Thermo RAW files.** As such, it is expected that there may be bugs / issues with processing data of other vendor formats.

If you would like to help us test support on Agilent / Bruker / Sciex / Waters instruments, please send an email to wasim.sandhu@czbiohub.org.
