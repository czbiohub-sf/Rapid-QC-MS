#!/usr/bin/env python3
"""Convert SIRIUS project output + MSP spectra into DiffMS input format.

Produces:
  spec_files/   - .ms files (SIRIUS text format) per feature
  labels.tsv    - feature → formula mapping
  split.tsv     - all features assigned to 'test' split
  subformulae/  - per-feature JSON with fragment annotations from SIRIUS trees
"""

from __future__ import annotations

import argparse
import csv
import json
import re
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


def find_sirius_trees(project_dir: Path) -> dict[str, dict]:
    """Extract fragmentation tree data from SIRIUS project.

    Returns a dict mapping feature display name → tree info with formula
    and fragment list.
    """
    trees: dict[str, dict] = {}

    # SIRIUS project-space stores results under compound directories
    # Look for formula results in the project
    for tree_json in project_dir.rglob("*.json"):
        # Skip non-tree files
        if "tree" not in tree_json.name.lower() and "ftree" not in tree_json.name.lower():
            continue
        try:
            data = json.loads(tree_json.read_text(encoding="utf-8"))
            # Extract fragment formulas if available
            if "fragments" in data:
                parent_dir = tree_json.parent
                trees[str(parent_dir.name)] = data
        except (json.JSONDecodeError, KeyError):
            continue

    return trees


def parse_formulas_summary(summary_path: Path) -> dict[str, dict]:
    """Parse SIRIUS formulas_summary.tsv to get top formula per feature."""
    results: dict[str, dict] = {}

    if not summary_path.exists():
        return results

    with summary_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            # Use the feature/compound ID
            feature_id = row.get("id", row.get("featureId", row.get("compoundId", "")))
            if not feature_id:
                continue

            # Keep only the top-ranked formula per feature
            if feature_id in results:
                continue

            formula = row.get("molecularFormula", row.get("formula", ""))
            adduct = row.get("adduct", row.get("ionization", "[M+H]+"))
            precursor_mz = row.get("precursorMz", row.get("ionMass", ""))

            if formula:
                results[feature_id] = {
                    "formula": formula,
                    "adduct": adduct,
                    "precursor_mz": precursor_mz,
                }

    return results


def write_ms_file(out_path: Path, entry: dict, precursor_mz: str):
    """Write a spectrum in SIRIUS .ms text format."""
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


def write_subform_json(out_path: Path, formula: str, adduct: str,
                       peaks: list[tuple[float, float]]):
    """Write a minimal subformulae JSON for DiffMS.

    When we don't have fragment annotations from SIRIUS trees,
    write output_tbl as null so DiffMS uses only the parent peak.
    """
    data = {
        "cand_form": formula,
        "cand_ion": adduct,
        "output_tbl": None,
    }
    out_path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--msp", type=Path, required=True,
                        help="Input MSP file from MS-DIAL")
    parser.add_argument("--sirius-summary", type=Path, required=True,
                        help="SIRIUS summary directory (contains formulas_summary.tsv)")
    parser.add_argument("--sirius-project", type=Path, default=None,
                        help="SIRIUS project directory (for fragment trees, optional)")
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

    # Parse SIRIUS formula results
    formulas_tsv = args.sirius_summary / "formulas_summary.tsv"
    if not formulas_tsv.exists():
        # Try alternate path patterns
        candidates = list(args.sirius_summary.rglob("*formula*summary*"))
        if candidates:
            formulas_tsv = candidates[0]
    formulas = parse_formulas_summary(formulas_tsv)
    print(f"Found {len(formulas)} formula predictions from SIRIUS")

    # Build index: match MSP entries to SIRIUS results by alignment ID
    labels_rows = []
    split_rows = []
    matched = 0

    for i, entry in enumerate(entries):
        meta = entry["metadata"]
        align_id = meta.get("ALIGNMENTID", str(i))
        name = meta.get("NAME", f"feature_{i}")
        precursor_mz = meta.get("PRECURSORMZ", meta.get("PRECURSOR M/Z", "0"))
        adduct = meta.get("PRECURSORTYPE", "[M+H]+")

        spec_name = safe_name(f"{args.polarity}_{align_id}")

        # Try to find formula from SIRIUS - match by various ID patterns
        formula_info = None
        for sid, info in formulas.items():
            # SIRIUS IDs can be complex; try substring matching on alignment ID
            if align_id in sid or sid in align_id:
                formula_info = info
                break

        if formula_info is None:
            # Try matching by precursor m/z (within 0.01 Da)
            try:
                target_mz = float(precursor_mz)
                for sid, info in formulas.items():
                    try:
                        sirius_mz = float(info.get("precursor_mz", "0"))
                        if abs(target_mz - sirius_mz) < 0.01:
                            formula_info = info
                            break
                    except ValueError:
                        continue
            except ValueError:
                pass

        if formula_info is None:
            continue

        formula = formula_info["formula"]
        matched += 1

        # Write .ms file
        write_ms_file(spec_dir / f"{spec_name}.ms", entry, precursor_mz)

        # Write subformulae JSON (minimal - DiffMS requires this file to exist)
        write_subform_json(subform_dir / f"{spec_name}.json", formula, adduct,
                           entry["peaks"])

        # Collect for labels and splits
        labels_rows.append({
            "spec": spec_name,
            "formula": formula,
            "smiles": "",
            "instrument": "Orbitrap",
        })
        split_rows.append({"name": spec_name, "split": "test"})

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

    print(f"Matched {matched}/{len(entries)} spectra to SIRIUS formulas")
    print(f"DiffMS input written to {out}")


if __name__ == "__main__":
    main()
