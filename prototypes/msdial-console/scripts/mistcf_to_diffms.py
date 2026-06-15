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

import numpy as np


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


def formula_to_smiles(formula: str) -> str:
    """Convert a molecular formula to a disconnected-atom SMILES.

    DiffMS generates bond structure (edges) conditioned on a fixed set of
    atoms (nodes).  The atom identities and count come from the molecular
    graph built from the SMILES.  By encoding the predicted formula as
    disconnected atoms, DiffMS gets the correct atom inventory to wire up.
    """
    atoms: list[str] = []
    for match in re.finditer(r"([A-Z][a-z]?)(\d*)", formula):
        elem, num = match.groups()
        if elem:
            count = int(num) if num else 1
            for _ in range(count):
                atoms.append(f"[{elem}]")
    return ".".join(atoms) if atoms else "[C]"


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


def parse_formula(formula: str) -> dict[str, int]:
    """Parse a molecular formula string into element counts."""
    counts: dict[str, int] = {}
    for match in re.finditer(r"([A-Z][a-z]?)(\d*)", formula):
        elem, num = match.groups()
        if elem:
            counts[elem] = counts.get(elem, 0) + (int(num) if num else 1)
    return counts


def write_diffms_stats(out: Path, formulas: list[str], max_nodes: int = 128):
    """Write dataset statistics for DiffMS marginal transition model.

    DiffMS computes marginal distributions from the training split to use as
    the prior (limit distribution) in the diffusion process.  With dummy
    training molecules this produces degenerate/NaN marginals.

    We derive atom-type and node-count statistics from the predicted molecular
    formulas, which gives a data-driven prior matching our actual spectra.

    Atom types (8): C, O, P, N, S, Cl, F, H
    Edge types (5): no-edge, single, double, triple, aromatic
    """
    # DiffMS atom ordering
    atom_order = ["C", "O", "P", "N", "S", "Cl", "F", "H"]

    # Count atoms across all predicted formulas
    atom_counts = np.zeros(len(atom_order))
    node_sizes = []
    for f in formulas:
        parsed = parse_formula(f)
        total_atoms = 0
        for i, elem in enumerate(atom_order):
            n = parsed.get(elem, 0)
            atom_counts[i] += n
            total_atoms += n
        node_sizes.append(total_atoms)

    # Atom-type distribution from real predicted formulas
    if atom_counts.sum() > 0:
        atom_types = atom_counts / atom_counts.sum()
    else:
        atom_types = np.ones(len(atom_order)) / len(atom_order)
    print(f"Atom-type distribution from {len(formulas)} formulas: "
          + ", ".join(f"{a}={v:.3f}" for a, v in zip(atom_order, atom_types)))

    # Edge-type distribution: can't derive from formulas alone, use
    # reasonable estimates for drug-like molecules
    # [no-edge, single, double, triple, aromatic]
    edge_types = np.array([0.90, 0.055, 0.015, 0.002, 0.028])
    edge_types /= edge_types.sum()

    # Node-count distribution from actual formula sizes
    n_counts = np.zeros(max_nodes + 1)
    for s in node_sizes:
        if s <= max_nodes:
            n_counts[s] += 1
    if n_counts.sum() > 0:
        n_counts /= n_counts.sum()
    else:
        # Fallback: Gaussian around 30
        for i in range(max_nodes + 1):
            n_counts[i] = np.exp(-0.5 * ((i - 30) / 10) ** 2)
        n_counts /= n_counts.sum()

    # Valency distribution: reasonable for organic molecules
    valencies = np.array([0.05, 0.15, 0.30, 0.30, 0.20])

    np.savetxt(out / "atom_types.txt", atom_types)
    np.savetxt(out / "edge_types.txt", edge_types)
    np.savetxt(out / "n_counts.txt", n_counts)
    np.savetxt(out / "valencies.txt", valencies)
    print(f"Wrote DiffMS stats files to {out}")


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

    # Write DiffMS stats derived from predicted formulas
    all_formulas = [info["formula"] for info in formulas.values() if info["formula"]]
    write_diffms_stats(out, all_formulas)

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
            "smiles": formula_to_smiles(formula),
            "instrument": "Orbitrap",
        })
        split_rows.append({"name": spec_name, "split": "test"})

    # DiffMS requires non-empty train/val splits even in test-only mode.
    if len(split_rows) >= 2:
        split_rows[0]["split"] = "train"
        split_rows[1]["split"] = "val"
    elif len(split_rows) == 1:
        split_rows[0]["split"] = "train"

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
