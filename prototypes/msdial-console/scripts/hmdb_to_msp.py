#!/usr/bin/env python3.9
"""Convert HMDB individual spectrum text files to a single MSP library.

HMDB distributes predicted and experimental MS/MS spectra as individual
text files with tab-separated m/z and intensity columns. This script
consolidates them into a single MSP file suitable for spectral searching.

Filename convention:
  HMDB0000001_msms_1_2_experimental.txt
  HMDB0041076_msms_6506_43378_predicted.txt

The HMDB ID is extracted from the filename for the NAME field.
"""

import argparse
import os
import re
import sys
from pathlib import Path


def parse_hmdb_spectrum(filepath):
    """Parse an HMDB peak list text file. Returns list of (mz, intensity) tuples."""
    peaks = []
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) >= 2:
                try:
                    mz = float(parts[0])
                    intensity = float(parts[1])
                    peaks.append((mz, intensity))
                except ValueError:
                    continue
    return peaks


def extract_hmdb_id(filename):
    """Extract HMDB ID from filename like HMDB0000001_msms_1_2_experimental.txt"""
    match = re.match(r"(HMDB\d+)", filename)
    return match.group(1) if match else filename


def extract_spectrum_type(filename):
    """Extract 'predicted' or 'experimental' from filename."""
    if "predicted" in filename.lower():
        return "predicted"
    elif "experimental" in filename.lower():
        return "experimental"
    return "unknown"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", required=True, type=Path,
                        help="Directory of extracted HMDB spectrum .txt files")
    parser.add_argument("--output", required=True, type=Path,
                        help="Output MSP file")
    parser.add_argument("--min-peaks", type=int, default=2,
                        help="Minimum peaks per spectrum to include [default: 2]")
    parser.add_argument("--ion-mode", default=None, choices=["Positive", "Negative"],
                        help="Filter to specific ion mode (if detectable from filename)")
    args = parser.parse_args()

    input_dir = args.input_dir
    files = sorted(f for f in input_dir.iterdir()
                   if f.suffix.lower() in (".txt", ".tsv"))
    print(f"Found {len(files)} spectrum files in {input_dir}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    n_written = 0
    n_skipped = 0

    with args.output.open("w") as out:
        for filepath in files:
            peaks = parse_hmdb_spectrum(filepath)
            if len(peaks) < args.min_peaks:
                n_skipped += 1
                continue

            hmdb_id = extract_hmdb_id(filepath.name)
            spec_type = extract_spectrum_type(filepath.name)

            # Estimate precursor m/z as the highest m/z peak (rough heuristic)
            max_mz = max(p[0] for p in peaks)

            out.write(f"NAME: {hmdb_id}\n")
            out.write(f"PRECURSORMZ: {max_mz:.5f}\n")
            out.write(f"COMMENT: HMDB {spec_type} spectrum from {filepath.name}\n")
            out.write(f"INCHIKEY: {hmdb_id}\n")
            out.write(f"Num Peaks: {len(peaks)}\n")

            for mz, intensity in peaks:
                out.write(f"{mz:.5f}\t{intensity:.4f}\n")

            out.write("\n")
            n_written += 1

    print(f"Wrote {n_written} spectra to {args.output} (skipped {n_skipped} with <{args.min_peaks} peaks)")


if __name__ == "__main__":
    main()
