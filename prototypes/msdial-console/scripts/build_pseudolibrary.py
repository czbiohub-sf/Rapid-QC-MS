#!/usr/bin/env python3.9
"""Build an MSP pseudolibrary by merging original MS2 spectra with DiffMS-predicted structures.

Takes:
  - Original MSP file from MS-DIAL (real spectra)
  - DiffMS predictions directory (pickled RDKit Mol objects)
  - DiffMS labels.tsv (maps spectrum names to features)

Produces:
  - Annotated MSP library with predicted SMILES for each feature
"""

import argparse
import csv
import glob
import os
import pickle
import re
import sys
from pathlib import Path


def parse_msp_entries(msp_path):
    """Parse MSP file into list of dicts with metadata and raw peak text."""
    entries = []
    current_meta = {}
    current_peaks = []
    in_peaks = False

    with open(msp_path, "r") as f:
        for line in f:
            stripped = line.strip()

            if not stripped:
                if current_meta or current_peaks:
                    entries.append({"meta": current_meta, "peaks": current_peaks})
                    current_meta = {}
                    current_peaks = []
                    in_peaks = False
                continue

            if in_peaks and stripped[0].isdigit():
                current_peaks.append(stripped)
                continue

            if ":" in stripped and not stripped[0].isdigit():
                key, _, val = stripped.partition(":")
                key_upper = key.strip().upper()
                val = val.strip()
                current_meta[key_upper] = val

                if key_upper in ("NUM PEAKS", "NUMPEAKS"):
                    in_peaks = True

    if current_meta or current_peaks:
        entries.append({"meta": current_meta, "peaks": current_peaks})

    return entries


def load_diffms_predictions(preds_dir):
    """Load DiffMS prediction pickle files and extract best SMILES per spectrum."""
    pred_files = sorted(glob.glob(os.path.join(preds_dir, "*_pred_*.pkl")))

    all_predictions = []
    for pf in pred_files:
        with open(pf, "rb") as f:
            batch = pickle.load(f)
            all_predictions.extend(batch)

    # Each entry is a list of RDKit Mol objects (multiple samples per spectrum)
    # Pick the best (first valid) SMILES for each
    smiles_list = []
    for mol_list in all_predictions:
        best_smiles = ""
        for mol in mol_list:
            if mol is not None:
                try:
                    from rdkit import Chem
                    smi = Chem.MolToSmiles(mol)
                    if smi:
                        best_smiles = smi
                        break
                except Exception:
                    continue
        smiles_list.append(best_smiles)

    return smiles_list


def load_labels(labels_path):
    """Load labels.tsv to get spectrum name → formula mapping."""
    entries = []
    with open(labels_path, "r") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            entries.append(row)
    return entries


def write_pseudolibrary(msp_entries, labels, smiles_list, output_path):
    """Write annotated MSP library."""
    # Build lookup from alignment ID to DiffMS prediction
    # Labels have spec names like "pos_42" which encode polarity + alignment ID
    pred_lookup = {}
    for i, label in enumerate(labels):
        spec_name = label.get("spec", "")
        formula = label.get("formula", "")
        smiles = smiles_list[i] if i < len(smiles_list) else ""
        # Extract alignment ID from spec name (e.g., "pos_42" → "42")
        parts = spec_name.rsplit("_", 1)
        align_id = parts[-1] if len(parts) > 1 else spec_name
        pred_lookup[align_id] = {"formula": formula, "smiles": smiles, "spec_name": spec_name}

    n_annotated = 0
    with open(output_path, "w") as f:
        for entry in msp_entries:
            meta = entry["meta"]
            peaks = entry["peaks"]

            align_id = meta.get("ALIGNMENTID", "")
            pred = pred_lookup.get(align_id, {})
            predicted_smiles = pred.get("smiles", "")
            predicted_formula = pred.get("formula", "")

            # Write metadata
            f.write(f"NAME: {meta.get('NAME', 'Unknown')}\n")
            f.write(f"PRECURSORMZ: {meta.get('PRECURSORMZ', '0')}\n")
            f.write(f"PRECURSORTYPE: {meta.get('PRECURSORTYPE', '')}\n")
            f.write(f"RETENTIONTIME: {meta.get('RETENTIONTIME', '0')}\n")
            f.write(f"IONMODE: {meta.get('IONMODE', '')}\n")
            f.write(f"ALIGNMENTID: {align_id}\n")

            if predicted_formula:
                f.write(f"FORMULA: {predicted_formula}\n")
            if predicted_smiles:
                f.write(f"PREDICTED_SMILES: {predicted_smiles}\n")
                n_annotated += 1
            if meta.get("FORMULA"):
                f.write(f"LIBRARY_FORMULA: {meta['FORMULA']}\n")
            if meta.get("SMILES"):
                f.write(f"LIBRARY_SMILES: {meta['SMILES']}\n")
            if meta.get("INCHIKEY"):
                f.write(f"INCHIKEY: {meta['INCHIKEY']}\n")
            if meta.get("SPECTRUMREFERENCEFILE"):
                f.write(f"SPECTRUMREFERENCEFILE: {meta['SPECTRUMREFERENCEFILE']}\n")

            f.write(f"COMMENT: DiffMS predicted structure\n")
            f.write(f"Num Peaks: {len(peaks)}\n")

            for peak_line in peaks:
                f.write(f"{peak_line}\n")

            f.write("\n")

    total = len(msp_entries)
    print(f"Wrote {total} spectra to {output_path} ({n_annotated} with predicted structures)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--msp", required=True, help="Original MSP file from MS-DIAL")
    parser.add_argument("--preds", required=True, help="DiffMS predictions directory")
    parser.add_argument("--labels", required=True, help="DiffMS labels.tsv")
    parser.add_argument("--output", required=True, help="Output pseudolibrary MSP file")
    args = parser.parse_args()

    msp_entries = parse_msp_entries(args.msp)
    print(f"Loaded {len(msp_entries)} spectra from {args.msp}")

    labels = load_labels(args.labels)
    print(f"Loaded {len(labels)} labels from {args.labels}")

    smiles_list = load_diffms_predictions(args.preds)
    print(f"Loaded {len(smiles_list)} predictions ({sum(1 for s in smiles_list if s)} with valid SMILES)")

    write_pseudolibrary(msp_entries, labels, smiles_list, args.output)


if __name__ == "__main__":
    main()
