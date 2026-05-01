#!/usr/bin/env python3.9
"""Merge tiered spectral search annotations into a single feature table.

Tier priority: tier1 (real library) > tier2 (predicted library) > tier3 (DiffMS de novo)
For each feature, the highest-confidence annotation wins.
"""

import argparse
import csv
from pathlib import Path


def load_hits(path, tier_name):
    """Load a hits.csv from spectral search and tag with tier."""
    hits = {}
    if not path.exists():
        return hits
    with path.open("r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            qid = row.get("query_alignment_id", "")
            if not qid:
                continue
            score = float(row.get("cosine_score", "0"))
            # Keep best hit per query
            if qid not in hits or score > hits[qid]["cosine_score"]:
                hits[qid] = {
                    "alignment_id": qid,
                    "tier": tier_name,
                    "library_name": row.get("library_name", ""),
                    "library_formula": row.get("library_formula", ""),
                    "library_smiles": row.get("library_smiles", ""),
                    "library_inchikey": row.get("library_inchikey", ""),
                    "cosine_score": score,
                    "matched_peaks": int(row.get("matched_peaks", "0")),
                    "precursor_mz_diff": float(row.get("precursor_mz_diff", "0")),
                }
    return hits


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tier1", type=Path, default=None,
                        help="Tier 1 hits.csv (real library match)")
    parser.add_argument("--tier2", type=Path, default=None,
                        help="Tier 2 hits.csv (predicted library match)")
    parser.add_argument("--tier3", type=Path, default=None,
                        help="Tier 3 DiffMS pseudolibrary hits.csv")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    # Load tiers in priority order
    annotations = {}

    # Tier 3 first (lowest priority, gets overwritten)
    if args.tier3 and args.tier3.exists():
        annotations.update(load_hits(args.tier3, "tier3_diffms"))

    # Tier 2 overwrites tier 3
    if args.tier2 and args.tier2.exists():
        annotations.update(load_hits(args.tier2, "tier2_predicted"))

    # Tier 1 overwrites everything
    if args.tier1 and args.tier1.exists():
        annotations.update(load_hits(args.tier1, "tier1_reference"))

    # Write merged annotations
    fieldnames = [
        "alignment_id", "tier", "library_name", "library_formula",
        "library_smiles", "library_inchikey", "cosine_score",
        "matched_peaks", "precursor_mz_diff",
    ]

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for aid in sorted(annotations.keys(), key=lambda x: int(x) if x.isdigit() else x):
            writer.writerow(annotations[aid])

    tiers = {}
    for a in annotations.values():
        tiers[a["tier"]] = tiers.get(a["tier"], 0) + 1

    print(f"Merged annotations for {len(annotations)} features:")
    for t, n in sorted(tiers.items()):
        print(f"  {t}: {n}")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
