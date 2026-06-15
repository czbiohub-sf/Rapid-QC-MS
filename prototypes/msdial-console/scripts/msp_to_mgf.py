#!/usr/bin/env python3
"""Convert MS-DIAL MSP output to MGF format for MIST-CF input."""

import argparse
import re


def convert_msp_to_mgf(input_path, output_path):
    """Convert MSP file to MGF format."""
    with open(input_path, "r") as fin, open(output_path, "w") as fout:
        metadata = {}
        peaks = []
        in_peaks = False

        for raw_line in fin:
            line = raw_line.strip()

            if not line:
                if metadata or peaks:
                    write_mgf_entry(fout, metadata, peaks)
                    metadata = {}
                    peaks = []
                    in_peaks = False
                continue

            if in_peaks or (line[0].isdigit() or line[0] == "-"):
                parts = re.split(r"[\t ]+", line)
                if len(parts) >= 2:
                    try:
                        mz = float(parts[0])
                        intensity = float(parts[1])
                        peaks.append((mz, intensity))
                        in_peaks = True
                    except ValueError:
                        pass
            elif ":" in line:
                key, _, val = line.partition(":")
                metadata[key.strip().upper()] = val.strip()

        if metadata or peaks:
            write_mgf_entry(fout, metadata, peaks)


def write_mgf_entry(fout, metadata, peaks):
    """Write a single MGF entry."""
    if not peaks:
        return

    precursor_mz = metadata.get(
        "PRECURSORMZ", metadata.get("PRECURSOR M/Z", "0")
    )
    align_id = metadata.get("ALIGNMENTID", metadata.get("SCANS", "unknown"))
    name = metadata.get("NAME", f"feature_{align_id}")
    ion_mode = metadata.get("IONMODE", "Positive")
    precursor_type = metadata.get("PRECURSORTYPE", "[M+H]+")
    rt = metadata.get("RETENTIONTIME", "")

    fout.write("BEGIN IONS\n")
    fout.write(f"PEPMASS={precursor_mz}\n")
    fout.write(f"PARENTMASS={precursor_mz}\n")
    fout.write(f"FEATURE_ID={align_id}\n")
    fout.write(f"NAME={name}\n")
    fout.write(f"IONMODE={ion_mode}\n")
    fout.write(f"PRECURSOR_TYPE={precursor_type}\n")
    fout.write(f"INSTRUMENT=Orbitrap (LCMS)\n")
    if rt:
        fout.write(f"RTINSECONDS={rt}\n")

    for mz, intensity in peaks:
        fout.write(f"{mz:.5f} {intensity:.1f}\n")

    fout.write("END IONS\n\n")


def main():
    parser = argparse.ArgumentParser(description="Convert MSP to MGF")
    parser.add_argument("--input", required=True, help="Input MSP file")
    parser.add_argument("--output", required=True, help="Output MGF file")
    args = parser.parse_args()
    convert_msp_to_mgf(args.input, args.output)
    print(f"Converted {args.input} → {args.output}")


if __name__ == "__main__":
    main()
