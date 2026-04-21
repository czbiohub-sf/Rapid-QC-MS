"""Tests for src/rapidqcms/service/processor.py"""

import subprocess
from pathlib import Path
from unittest import mock

import pytest

from rapidqcms.service.processor import (
    run_msdial_processing,
    run_msconvert,
)


# ---------------------------------------------------------------------------
# run_msconvert
# ---------------------------------------------------------------------------


def test_run_msconvert_success(tmp_path):
    """Returns the expected .mzML path when msconvert exits 0."""
    raw_file = tmp_path / "sample.raw"
    raw_file.write_bytes(b"fake raw data")

    # Simulate msconvert producing the output file
    def fake_run(cmd, **kwargs):
        mzml = tmp_path / "out" / "sample.mzML"
        mzml.parent.mkdir(parents=True, exist_ok=True)
        mzml.write_text("<?xml version='1.0'?>")

    with mock.patch("subprocess.run", side_effect=fake_run):
        result = run_msconvert(
            path=raw_file,
            filename="sample",
            extension=".raw",
            output_folder=tmp_path / "out",
            msconvert_exe="/usr/bin/msconvert",
        )

    assert result is not None
    assert result.suffix == ".mzML"
    assert result.name == "sample.mzML"


def test_run_msconvert_timeout_returns_none(tmp_path):
    """Returns None when subprocess.run raises TimeoutExpired."""
    raw_file = tmp_path / "sample.raw"
    raw_file.write_bytes(b"fake")

    with mock.patch(
        "subprocess.run",
        side_effect=subprocess.TimeoutExpired(cmd="msconvert", timeout=30),
    ):
        result = run_msconvert(
            path=raw_file,
            filename="sample",
            extension=".raw",
            output_folder=tmp_path / "out",
            msconvert_exe="/usr/bin/msconvert",
        )

    assert result is None


def test_run_msconvert_nonzero_exit_returns_none(tmp_path):
    """Returns None when subprocess exits with a non-zero code."""
    raw_file = tmp_path / "sample.raw"
    raw_file.write_bytes(b"fake")

    with mock.patch(
        "subprocess.run",
        side_effect=subprocess.CalledProcessError(returncode=1, cmd="msconvert", stderr=b"err"),
    ):
        result = run_msconvert(
            path=raw_file,
            filename="sample",
            extension=".raw",
            output_folder=tmp_path / "out",
            msconvert_exe="/usr/bin/msconvert",
        )

    assert result is None


# ---------------------------------------------------------------------------
# run_msdial_processing
# ---------------------------------------------------------------------------


def test_run_msdial_success(tmp_path):
    """Returns the expected .msdial path when MsdialConsoleApp exits 0."""
    mzml = tmp_path / "sample.mzML"
    mzml.write_text("<?xml?>")

    def fake_run(cmd, **kwargs):
        out = tmp_path / "msdial_out" / "sample.msdial"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text("Peak list output")

    with mock.patch("subprocess.run", side_effect=fake_run):
        result = run_msdial_processing(
            mzml_path=mzml,
            msdial_exe="/usr/bin/MsdialConsoleApp",
            parameter_file=tmp_path / "params.txt",
            output_folder=tmp_path / "msdial_out",
        )

    assert result is not None
    assert result.suffix == ".msdial"
    assert result.stem == "sample"


def test_run_msdial_timeout_returns_none(tmp_path):
    """Returns None when subprocess.run raises TimeoutExpired."""
    mzml = tmp_path / "sample.mzML"
    mzml.write_text("<?xml?>")

    with mock.patch(
        "subprocess.run",
        side_effect=subprocess.TimeoutExpired(cmd="MsdialConsoleApp", timeout=300),
    ):
        result = run_msdial_processing(
            mzml_path=mzml,
            msdial_exe="/usr/bin/MsdialConsoleApp",
            parameter_file=tmp_path / "params.txt",
            output_folder=tmp_path / "msdial_out",
        )

    assert result is None


# ---------------------------------------------------------------------------
# process_sample (integration-style with mocked QC modules)
# ---------------------------------------------------------------------------


def test_process_sample_skips_conversion_when_no_exe(tmp_path):
    """When no msconvert/msdial exe is given and input is already .mzML,
    process_sample passes the file directly to run_qc and returns results."""
    mzml = tmp_path / "sample_001.mzML"
    mzml.write_text("fake mzml")

    fake_result = mock.MagicMock()
    fake_result.status.value = "Pass"
    fake_result.module = "metabolomics"

    with mock.patch(
        "rapidqcms.service.processor.run_qc",
        return_value=[fake_result],
    ) as mock_run_qc:
        from rapidqcms.service.processor import process_sample

        results = process_sample(
            raw_path=mzml,
            work_dir=tmp_path / "work",
            instrument_id="INST01",
            run_id="RUN001",
            context={"df_features": None},
            stage="pre_search",
            # no msconvert_exe, no msdial_exe
        )

    assert len(results) == 1
    mock_run_qc.assert_called_once()
    call_kwargs = mock_run_qc.call_args
    # First positional arg must be the mzml path (no conversion)
    assert call_kwargs.args[0] == mzml
