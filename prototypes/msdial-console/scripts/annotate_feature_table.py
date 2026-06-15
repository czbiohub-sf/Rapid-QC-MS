#!/usr/bin/env python3.9
"""Join annotation columns onto the MS-DIAL alignment result feature table.

MS-DIAL .msdial files have 4 header rows (Class, File type, Injection order,
Batch ID) followed by the data header and feature rows.  We parse the data
portion, left-join the merged annotations by Alignment ID, and write a
combined TSV that preserves all original columns plus annotation columns.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


def parse_msdial_header_rows(lines: list[str]) -> int:
    """Return the number of pre-header rows in an MS-DIAL file.

    MS-DIAL alignment files have metadata rows before the column header.
    The column header is the first row containing 'Alignment ID'.
    """
    for i, line in enumerate(lines):
        if "Alignment ID" in line:
            return i
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feature-table", type=Path, required=True,
                        help="MS-DIAL .msdial alignment result")
    parser.add_argument("--annotations", type=Path, required=True,
                        help="Merged annotations CSV from merge_annotations.py")
    parser.add_argument("--output", type=Path, required=True,
                        help="Output annotated feature table (TSV)")
    args = parser.parse_args()

    # Read annotations into dict keyed by alignment_id
    annotations: dict[str, dict] = {}
    ann_fields: list[str] = []
    with args.annotations.open("r") as f:
        reader = csv.DictReader(f)
        ann_fields = [c for c in reader.fieldnames if c != "alignment_id"]
        for row in reader:
            aid = row.get("alignment_id", "")
            if aid:
                annotations[aid] = {k: row.get(k, "") for k in ann_fields}

    print(f"Loaded {len(annotations)} annotations with fields: {ann_fields}")

    # Read the MS-DIAL file
    with args.feature_table.open("r") as f:
        all_lines = f.readlines()

    skip = parse_msdial_header_rows(all_lines)
    pre_header = all_lines[:skip]

    # Parse header + data
    header_line = all_lines[skip].rstrip("\n\r")
    header_cols = header_line.split("\t")
    data_lines = all_lines[skip + 1:]

    # Write output: pre-header rows, then header + annotation columns, then data
    with args.output.open("w") as out:
        # Preserve pre-header metadata rows, pad with empty tabs for new columns
        for line in pre_header:
            out.write(line.rstrip("\n\r") + "\t" * len(ann_fields) + "\n")

        # Write header with annotation columns
        out.write(header_line + "\t" + "\t".join(ann_fields) + "\n")

        # Write data rows with joined annotations
        matched = 0
        total = 0
        for line in data_lines:
            line = line.rstrip("\n\r")
            if not line:
                continue
            cols = line.split("\t")
            aid = cols[0] if cols else ""
            total += 1

            ann_values = []
            if aid in annotations:
                matched += 1
                for field in ann_fields:
                    ann_values.append(annotations[aid].get(field, ""))
            else:
                ann_values = [""] * len(ann_fields)

            out.write(line + "\t" + "\t".join(ann_values) + "\n")

    print(f"Joined {matched}/{total} features with annotations")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
