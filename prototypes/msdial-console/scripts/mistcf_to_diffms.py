#!/usr/bin/env python3
"""Convert MIST-CF output + MSP spectra into DiffMS input format.

Produces:
  spec_files/   - .ms files (SIRIUS text format) per feature
  labels.tsv    - feature → formula mapping
  split.tsv     - all features assigned to 'test' split
  subformulae/  - per-feature JSON with subformula assignments from MIST-CF
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
from pathlib import Path


def parse_msp_entries(msp_path: Path) -> list[dict]:
    """Parse an MSP file into a list of spectrum entries."""
    entries: list[dict] = []
    current: dict = {"metadata": {}, "peaks": []}

    with msp_path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                if current["peaks"] or current["metadata"]:
                    entries.append(current)
                    current = {"metadata": {}, "peaks": []}
                continue

            if ":" in line and not line[0].isdigit():
                key, _, val = line.partition(":")
                current["metadata"][key.strip().upper()] = val.strip()
            elif line[0].isdigit() or line[0] == "-":
                parts = re.split(r"[\t ]+", line)
                if len(parts) >= 2:
                    try:
                        mz = float(parts[0])
                        intensity = float(parts[1])
                        current["peaks"].append((mz, intensity))
                    except ValueError:
                        pass

    if current["peaks"] or current["metadata"]:
        entries.append(current)

    return entries


def safe_name(text: str) -> str:
    """Create a filesystem-safe name from text."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", text)


def parse_mistcf_formulas(tsv_path: Path) -> dict[str, dict]:
    """Parse MIST-CF formatted_output.tsv to get top formula per feature.

    MIST-CF output columns: spec, cand_ion, cand_form, parentmass
    Rows are ranked; we take the first (top-scoring) entry per spectrum.
    """
    results: dict[str, dict] = {}

    with tsv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            spec_id = row.get("spec", "")
            if not spec_id or spec_id in results:
                continue  # keep only top-ranked formula

            results[spec_id] = {
                "formula": row.get("cand_form", ""),
                "adduct": row.get("cand_ion", "[M+H]+"),
                "precursor_mz": row.get("parentmass", "0"),
            }

    return results


def write_ms_file(out_path: Path, entry: dict, precursor_mz: str):
    """Write a spectrum in SIRIUS .ms text format for DiffMS."""
    with out_path.open("w", encoding="utf-8") as f:
        f.write(f"#PARENTMASS {precursor_mz}\n")

        meta = entry["metadata"]
        if "IONMODE" in meta:
            f.write(f"#IONMODE {meta['IONMODE']}\n")
        if "PRECURSORTYPE" in meta:
            f.write(f"#ION {meta['PRECURSORTYPE']}\n")
        if "RETENTIONTIME" in meta:
            f.write(f"#RT {meta['RETENTIONTIME']}\n")
        if "NAME" in meta:
            f.write(f"#NAME {meta['NAME']}\n")

        f.write("\n>ms2\n")
        for mz, intensity in entry["peaks"]:
            f.write(f"{mz:.5f} {intensity:.1f}\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--msp", type=Path, required=True,
                        help="Input MSP file from MS-DIAL")
    parser.add_argument("--mistcf-output", type=Path, required=True,
                        help="MIST-CF formatted_output.tsv")
    parser.add_argument("--mistcf-subforms", type=Path, default=None,
                        help="MIST-CF subform_assigns/ directory (optional)")
    parser.add_argument("--output", type=Path, required=True,
                        help="Output directory for DiffMS input")
    parser.add_argument("--polarity", choices=["pos", "neg"], default="pos")
    args = parser.parse_args()

    out = args.output
    spec_dir = out / "spec_files"
    subform_dir = out / "subformulae"
    spec_dir.mkdir(parents=True, exist_ok=True)
    subform_dir.mkdir(parents=True, exist_ok=True)

    # Parse MSP
    entries = parse_msp_entries(args.msp)
    print(f"Parsed {len(entries)} spectra from {args.msp}")

    # Parse MIST-CF formula results
    formulas = parse_mistcf_formulas(args.mistcf_output)
    print(f"Found {len(formulas)} formula predictions from MIST-CF")

    # Build index of MSP entries by alignment ID
    entry_by_id = {}
    for i, entry in enumerate(entries):
        align_id = entry["metadata"].get("ALIGNMENTID", str(i))
        entry_by_id[align_id] = entry

    # Match MIST-CF results to MSP entries
    labels_rows = []
    split_rows = []
    matched = 0

    for spec_id, formula_info in formulas.items():
        formula = formula_info["formula"]
        adduct = formula_info["adduct"]
        precursor_mz = formula_info["precursor_mz"]

        # MIST-CF spec IDs are the alignment IDs we passed as FEATURE_ID
        entry = entry_by_id.get(spec_id)
        if entry is None:
            continue

        spec_name = safe_name(f"{args.polarity}_{spec_id}")
        matched += 1

        # Write .ms file
        write_ms_file(spec_dir / f"{spec_name}.ms", entry, precursor_mz)

        # Write subformulae JSON in DiffMS expected format
        # (single top-ranked formula, not MIST-CF multi-candidate format)
        data = {
            "cand_form": formula,
            "cand_ion": adduct,
            "output_tbl": None,
        }
        (subform_dir / f"{spec_name}.json").write_text(
            json.dumps(data, indent=2), encoding="utf-8"
        )

        labels_rows.append({
            "spec": spec_name,
            "formula": formula,
            "smiles": "CC",  # dummy SMILES; DiffMS requires valid mol with edges
            "instrument": "Orbitrap",
        })
        split_rows.append({"name": spec_name, "split": "test"})

    # DiffMS requires non-empty train/val splits even in test-only mode.
    # Assign the first two entries as dummy train/val with a valid SMILES.
    if len(split_rows) >= 2:
        split_rows[0]["split"] = "train"
        labels_rows[0]["smiles"] = "C"
        split_rows[1]["split"] = "val"
        labels_rows[1]["smiles"] = "C"
    elif len(split_rows) == 1:
        split_rows[0]["split"] = "train"
        labels_rows[0]["smiles"] = "C"

    # Write labels.tsv
    labels_path = out / "labels.tsv"
    with labels_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["spec", "formula", "smiles", "instrument"],
                                delimiter="\t")
        writer.writeheader()
        writer.writerows(labels_rows)

    # Write split.tsv
    split_path = out / "split.tsv"
    with split_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "split"], delimiter="\t")
        writer.writeheader()
        writer.writerows(split_rows)

    print(f"Matched {matched}/{len(entries)} spectra to MIST-CF formulas")
    print(f"DiffMS input written to {out}")


if __name__ == "__main__":
    main()
