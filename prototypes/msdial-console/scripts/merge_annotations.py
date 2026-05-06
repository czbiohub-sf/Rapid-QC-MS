#!/usr/bin/env python3.9
"""Merge tiered annotations into a single feature table.

All tiers run on all features.  The tier label indicates the best available
evidence level for each annotation:
  tier1 = reference spectral library match
  tier2 = predicted spectral library match
  tier3 = MIST-CF molecular formula prediction

Each feature keeps its best annotation from each tier.  The "best_tier"
column indicates the highest-confidence tier that produced a result.
"""

import argparse
import csv
from pathlib import Path


def load_spectral_hits(path: Path, tier_name: str) -> dict:
    """Load a spectral search hits.csv.  Keep best hit per query."""
    hits = {}
    if not path or not path.exists():
        return hits
    with path.open("r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            qid = row.get("query_alignment_id", "")
            if not qid:
                continue
            score = float(row.get("cosine_score", "0"))
            if qid not in hits or score > hits[qid]["cosine_score"]:
                hits[qid] = {
                    "name": row.get("library_name", ""),
                    "formula": row.get("library_formula", ""),
                    "smiles": row.get("library_smiles", ""),
                    "inchikey": row.get("library_inchikey", ""),
                    "cosine_score": score,
                    "matched_peaks": int(row.get("matched_peaks", "0")),
                }
    return hits


def load_mistcf_formulas(path: Path) -> dict:
    """Load MIST-CF formatted_output.tsv.  Keep top-ranked formula per spectrum."""
    hits = {}
    if not path or not path.exists():
        return hits
    with path.open("r") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            spec_id = row.get("spec", "")
            if not spec_id or spec_id in hits:
                continue  # top-ranked is first
            hits[spec_id] = {
                "formula": row.get("cand_form", ""),
                "adduct": row.get("cand_ion", ""),
                "score": row.get("scores", ""),
            }
    return hits


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tier1", type=Path, default=None,
                        help="Tier 1 hits.csv (reference library match)")
    parser.add_argument("--tier2", type=Path, default=None,
                        help="Tier 2 hits.csv (predicted library match)")
    parser.add_argument("--tier3", type=Path, default=None,
                        help="Tier 3 MIST-CF formatted_output.tsv")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    t1_hits = load_spectral_hits(args.tier1, "tier1_reference")
    t2_hits = load_spectral_hits(args.tier2, "tier2_predicted")
    t3_hits = load_mistcf_formulas(args.tier3)

    # Collect all alignment IDs across tiers
    all_ids = set(t1_hits.keys()) | set(t2_hits.keys()) | set(t3_hits.keys())

    print(f"Tier 1 (reference library): {len(t1_hits)} hits")
    print(f"Tier 2 (predicted library): {len(t2_hits)} hits")
    print(f"Tier 3 (MIST-CF formula):   {len(t3_hits)} hits")
    print(f"Unique features across all tiers: {len(all_ids)}")

    fieldnames = [
        "alignment_id", "best_tier",
        "t1_name", "t1_formula", "t1_smiles", "t1_inchikey",
        "t1_cosine", "t1_matched_peaks",
        "t2_name", "t2_formula", "t2_smiles", "t2_inchikey",
        "t2_cosine", "t2_matched_peaks",
        "t3_formula", "t3_adduct", "t3_score",
    ]

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for aid in sorted(all_ids, key=lambda x: int(x) if x.isdigit() else x):
            row = {"alignment_id": aid}

            # Determine best tier
            if aid in t1_hits:
                row["best_tier"] = "tier1_reference"
            elif aid in t2_hits:
                row["best_tier"] = "tier2_predicted"
            elif aid in t3_hits:
                row["best_tier"] = "tier3_formula"
            else:
                row["best_tier"] = ""

            # Tier 1 columns
            if aid in t1_hits:
                h = t1_hits[aid]
                row["t1_name"] = h["name"]
                row["t1_formula"] = h["formula"]
                row["t1_smiles"] = h["smiles"]
                row["t1_inchikey"] = h["inchikey"]
                row["t1_cosine"] = h["cosine_score"]
                row["t1_matched_peaks"] = h["matched_peaks"]

            # Tier 2 columns
            if aid in t2_hits:
                h = t2_hits[aid]
                row["t2_name"] = h["name"]
                row["t2_formula"] = h["formula"]
                row["t2_smiles"] = h["smiles"]
                row["t2_inchikey"] = h["inchikey"]
                row["t2_cosine"] = h["cosine_score"]
                row["t2_matched_peaks"] = h["matched_peaks"]

            # Tier 3 columns
            if aid in t3_hits:
                h = t3_hits[aid]
                row["t3_formula"] = h["formula"]
                row["t3_adduct"] = h["adduct"]
                row["t3_score"] = h["score"]

            writer.writerow(row)

    # Summary
    tier_counts = {}
    for aid in all_ids:
        if aid in t1_hits:
            t = "tier1_reference"
        elif aid in t2_hits:
            t = "tier2_predicted"
        elif aid in t3_hits:
            t = "tier3_formula"
        else:
            t = "none"
        tier_counts[t] = tier_counts.get(t, 0) + 1

    print(f"\nMerged annotations for {len(all_ids)} features:")
    for t, n in sorted(tier_counts.items()):
        print(f"  {t}: {n} (best available)")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
