"""MSConvert + MS-DIAL orchestration, extracted from AutoQCProcessing.py.

No database calls — all I/O is file-based.  The caller is responsible for
persisting results and uploading files.

Subprocess time limits match the original implementation:
  - MSConvert: 30 s
  - MS-DIAL:   300 s (5 min)
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path

from ..qc.base import QCResult
from .pipeline import run_qc

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# MSConvert
# ---------------------------------------------------------------------------


def run_msconvert(
    path: Path,
    filename: str,
    extension: str,
    output_folder: Path,
    msconvert_exe: Path | str,
    timeout: int = 30,
) -> Path | None:
    """Convert a vendor raw file to mzML using MSConvert.

    Copies the file to *output_folder* first (preserving the original), then
    runs ``msconvert.exe``.  Returns the path to the produced ``.mzML`` file,
    or ``None`` on timeout or non-zero exit.

    Args:
        path:           Full path to the source raw file.
        filename:       File name without extension (stem).
        extension:      Vendor file extension, e.g. ``".raw"`` or ``".d"``.
        output_folder:  Directory that will receive the ``.mzML`` output.
        msconvert_exe:  Path to the MSConvert executable.
        timeout:        Maximum seconds to wait (default 30).
    """
    output_folder.mkdir(parents=True, exist_ok=True)
    dest = output_folder / (filename + extension)
    if not dest.exists():
        shutil.copy2(path, dest)

    cmd = [
        str(msconvert_exe),
        str(dest),
        "--mzML",
        "--outdir", str(output_folder),
    ]
    log.info("Running MSConvert: %s", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True, timeout=timeout, capture_output=True)
    except subprocess.TimeoutExpired:
        log.warning("MSConvert timed out after %d s for %s", timeout, filename)
        return None
    except subprocess.CalledProcessError as exc:
        log.warning("MSConvert failed (rc=%d) for %s: %s", exc.returncode, filename, exc.stderr)
        return None

    mzml_path = output_folder / (filename + ".mzML")
    if not mzml_path.exists():
        log.warning("MSConvert exited OK but %s not found", mzml_path)
        return None
    return mzml_path


# ---------------------------------------------------------------------------
# MS-DIAL
# ---------------------------------------------------------------------------


def run_msdial_processing(
    mzml_path: Path,
    msdial_exe: Path | str,
    parameter_file: Path | str,
    output_folder: Path,
    timeout: int = 300,
) -> Path | None:
    """Process an mzML file through MsdialConsoleApp (LCMS DDA mode).

    Returns the path to the ``.msdial`` peak-list file produced, or ``None``
    on timeout or non-zero exit.

    Args:
        mzml_path:      Path to the input ``.mzML`` file.
        msdial_exe:     Path to the MsdialConsoleApp executable.
        parameter_file: Path to the MS-DIAL parameter file.
        output_folder:  Directory that will receive the ``.msdial`` output.
        timeout:        Maximum seconds to wait (default 300).
    """
    output_folder.mkdir(parents=True, exist_ok=True)
    cmd = [
        str(msdial_exe),
        "lcmsdda",
        "-i", str(mzml_path),
        "-o", str(output_folder),
        "-p", str(parameter_file),
    ]
    log.info("Running MS-DIAL: %s", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True, timeout=timeout, capture_output=True)
    except subprocess.TimeoutExpired:
        log.warning("MS-DIAL timed out after %d s for %s", timeout, mzml_path.stem)
        return None
    except subprocess.CalledProcessError as exc:
        log.warning("MS-DIAL failed (rc=%d) for %s: %s", exc.returncode, mzml_path.stem, exc.stderr)
        return None

    stem = mzml_path.stem
    msdial_path = output_folder / (stem + ".msdial")
    if not msdial_path.exists():
        log.warning("MS-DIAL exited OK but %s not found", msdial_path)
        return None
    return msdial_path


# ---------------------------------------------------------------------------
# High-level sample processor
# ---------------------------------------------------------------------------


def process_sample(
    raw_path: Path,
    work_dir: Path,
    instrument_id: str,
    run_id: str,
    context: dict,
    stage: str = "pre_search",
    msconvert_exe: Path | str | None = None,
    msdial_exe: Path | str | None = None,
    msdial_params: Path | str | None = None,
    config_path: Path | None = None,
    storage=None,
) -> list[QCResult]:
    """Run the full per-sample processing pipeline.

    Steps:
        1. MSConvert raw → mzML  (skipped when *msconvert_exe* is None)
        2. MS-DIAL mzML → .msdial  (skipped when *msdial_exe* is None)
        3. run_qc() on the resulting file
        4. Upload raw + result files to storage  (skipped when *storage* is None)

    The input file for run_qc() is chosen as follows:
        - If MS-DIAL ran: the ``.msdial`` file
        - Else if MSConvert ran: the ``.mzML`` file
        - Else: *raw_path* itself (callers can pass an mzML directly)

    Args:
        raw_path:       Path to the source file (vendor raw, mzML, or .msdial).
        work_dir:       Scratch directory for intermediate outputs.
        instrument_id:  Instrument identifier (used for storage key prefix).
        run_id:         Acquisition run identifier (used for storage key prefix).
        context:        Context dict forwarded to QCModule.analyze().
        stage:          QC stage identifier (e.g. "pre_search").
        msconvert_exe:  MSConvert executable path, or None to skip.
        msdial_exe:     MsdialConsoleApp executable path, or None to skip.
        msdial_params:  MS-DIAL parameter file path, or None to skip.
        config_path:    Path to qc_modules.toml (uses package default if None).
        storage:        StorageBackend instance for uploading results, or None.

    Returns:
        List of QCResult objects (one per QC module that ran).
    """
    stem = raw_path.stem
    sample_work = work_dir / stem
    sample_work.mkdir(parents=True, exist_ok=True)

    qc_input = raw_path

    # Step 1 – MSConvert
    if msconvert_exe is not None:
        mzml = run_msconvert(
            path=raw_path,
            filename=stem,
            extension=raw_path.suffix,
            output_folder=sample_work,
            msconvert_exe=msconvert_exe,
        )
        if mzml is not None:
            qc_input = mzml
        else:
            log.error("MSConvert failed for %s — skipping sample", raw_path.name)
            return []

    # Step 2 – MS-DIAL
    if msdial_exe is not None and msdial_params is not None:
        msdial_out = run_msdial_processing(
            mzml_path=qc_input,
            msdial_exe=msdial_exe,
            parameter_file=msdial_params,
            output_folder=sample_work,
        )
        if msdial_out is not None:
            qc_input = msdial_out
        else:
            log.error("MS-DIAL failed for %s — skipping sample", raw_path.name)
            return []

    # Step 3 – QC analysis
    results = run_qc(qc_input, context=context, stage=stage, config_path=config_path)

    # Step 4 – Upload to storage
    if storage is not None:
        prefix = f"{instrument_id}/{run_id}/{stem}/"
        for upload_path in (raw_path, qc_input):
            if upload_path.exists():
                storage.upload(upload_path, prefix + upload_path.name)

    return results
