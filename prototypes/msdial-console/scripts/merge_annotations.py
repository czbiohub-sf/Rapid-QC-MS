#!/usr/bin/env python3.9
"""Merge tiered annotations into a single feature table.

All tiers run on all features.  The tier label indicates the best available
evidence level for each annotation:
  tier1 = curated experimental spectral library match (MS1+MS2)
  tier2 = larger experimental spectral library match (MS1+MS2)
  tier3 = predicted spectral library match (MS1+MS2)
  tier4 = molecular formula prediction (MS1+MS2, MIST-CF)

Each feature keeps its best annotation from each tier.  The "best_tier"
column indicates the highest-confidence tier that produced a result.
"""

import argparse
import csv
from pathlib import Path


def load_spectral_hits(path: Path) -> dict:
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


def load_mistcf_formulas(path: Path, top_k: int = 3) -> dict:
    """Load MIST-CF formatted_output.tsv. Keep top-k candidates per spectrum, ordered by rank."""
    hits: dict[str, list[dict]] = {}
    if not path or not path.exists():
        return hits
    with path.open("r") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            spec_id = row.get("spec", "")
            if not spec_id:
                continue
            lst = hits.setdefault(spec_id, [])
            if len(lst) >= top_k:
                continue
            lst.append({
                "rank":    int(row.get("rank", "0") or 0),
                "formula": row.get("cand_form", ""),
                "adduct":  row.get("cand_ion", ""),
                "score":   row.get("scores", ""),
            })
    # MIST-CF orders rows by rank within each spec already, but sort defensively.
    for spec in hits:
        hits[spec].sort(key=lambda r: r["rank"])
    return hits


SPECTRAL_FIELDS = ["name", "formula", "smiles", "inchikey", "cosine", "matched_peaks"]
FORMULA_FIELDS = ["formula", "adduct", "score"]
MISTCF_TOP_K = 3


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tier1", type=Path, default=None,
                        help="Tier 1 hits.csv (curated experimental library)")
    parser.add_argument("--tier2", type=Path, default=None,
                        help="Tier 2 hits.csv (larger experimental library)")
    parser.add_argument("--tier3", type=Path, default=None,
                        help="Tier 3 hits.csv (predicted library)")
    parser.add_argument("--tier4", type=Path, default=None,
                        help="Tier 4 MIST-CF formatted_output.tsv")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    t1_hits = load_spectral_hits(args.tier1)
    t2_hits = load_spectral_hits(args.tier2)
    t3_hits = load_spectral_hits(args.tier3)
    t4_hits = load_mistcf_formulas(args.tier4, top_k=MISTCF_TOP_K)

    all_ids = set(t1_hits) | set(t2_hits) | set(t3_hits) | set(t4_hits)

    print(f"Tier 1 (curated experimental): {len(t1_hits)} hits")
    print(f"Tier 2 (experimental):         {len(t2_hits)} hits")
    print(f"Tier 3 (predicted):            {len(t3_hits)} hits")
    print(f"Tier 4 (MIST-CF formula):      {len(t4_hits)} hits (up to top-{MISTCF_TOP_K} per feature)")
    print(f"Unique features across tiers:  {len(all_ids)}")

    fieldnames = ["alignment_id", "best_tier"]
    for prefix in ["t1", "t2", "t3"]:
        fieldnames += [f"{prefix}_{f}" for f in SPECTRAL_FIELDS]
    # Tier 4: keep rank-1 in the bare t4_* columns for backwards compat,
    # plus rank-2/3 alternates in t4_alt2_*/t4_alt3_*.
    fieldnames += [f"t4_{f}" for f in FORMULA_FIELDS]
    for k in range(2, MISTCF_TOP_K + 1):
        fieldnames += [f"t4_alt{k}_{f}" for f in FORMULA_FIELDS]

    tier_order = [
        ("tier1_curated", t1_hits),
        ("tier2_experimental", t2_hits),
        ("tier3_predicted", t3_hits),
        ("tier4_formula", t4_hits),
    ]

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for aid in sorted(all_ids, key=lambda x: int(x) if x.isdigit() else x):
            row = {"alignment_id": aid}

            # Best tier = highest-confidence with a hit
            for tier_name, tier_data in tier_order:
                if aid in tier_data:
                    row["best_tier"] = tier_name
                    break

            # Spectral tiers (1-3)
            for prefix, tier_data in [("t1", t1_hits), ("t2", t2_hits), ("t3", t3_hits)]:
                if aid in tier_data:
                    h = tier_data[aid]
                    row[f"{prefix}_name"] = h["name"]
                    row[f"{prefix}_formula"] = h["formula"]
                    row[f"{prefix}_smiles"] = h["smiles"]
                    row[f"{prefix}_inchikey"] = h["inchikey"]
                    row[f"{prefix}_cosine"] = h["cosine_score"]
                    row[f"{prefix}_matched_peaks"] = h["matched_peaks"]

            # Tier 4: rank-1 in t4_*, rank-2/3 in t4_alt2_*/t4_alt3_*
            if aid in t4_hits:
                candidates = t4_hits[aid]
                if candidates:
                    row["t4_formula"] = candidates[0]["formula"]
                    row["t4_adduct"]  = candidates[0]["adduct"]
                    row["t4_score"]   = candidates[0]["score"]
                for k, c in enumerate(candidates[1:MISTCF_TOP_K], start=2):
                    row[f"t4_alt{k}_formula"] = c["formula"]
                    row[f"t4_alt{k}_adduct"]  = c["adduct"]
                    row[f"t4_alt{k}_score"]   = c["score"]

            writer.writerow(row)

    tier_counts = {}
    for aid in all_ids:
        for tier_name, tier_data in tier_order:
            if aid in tier_data:
                tier_counts[tier_name] = tier_counts.get(tier_name, 0) + 1
                break

    print(f"\nMerged annotations for {len(all_ids)} features:")
    for t, n in sorted(tier_counts.items()):
        print(f"  {t}: {n} (best available)")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
