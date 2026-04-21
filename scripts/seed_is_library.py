#!/usr/bin/env python3
"""Seed the internal standards library from MSP spectral database files.

Parses all entries whose Name starts with '1_' (the IS naming convention used
in MS-DIAL annotation databases at CZ Biohub SF) and loads them into the
internal_standards table keyed by chromatography method.

Usage:
    python scripts/seed_is_library.py \\
        --pos /Users/ngloria/Downloads/Jan2026/posMSP_HILIC_Jan2026.msp \\
        --neg /Users/ngloria/Downloads/Jan2026/negMSP_HILIC_Jan2026.msp \\
        --chromatography HILIC

    # Dry run — print IS found without writing to DB:
    python scripts/seed_is_library.py \\
        --pos posMSP.msp --neg negMSP.msp --chromatography HILIC --dry-run
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


def parse_is_from_msp(path: Path, polarity: str) -> list[dict]:
    """Return IS entries (Name starts with '1_') from an MSP file.

    Deduplicates by name — first occurrence wins.
    """
    entries = []
    seen_names: set[str] = set()

    current: dict = {}
    in_peaks = False

    with open(path, encoding="utf-8", errors="replace") as fh:
        for raw_line in fh:
            line = raw_line.rstrip("\n")

            # Blank line → end of entry
            if line.strip() == "":
                if current.get("name") and current["name"].startswith("1_"):
                    name = current["name"]
                    if name not in seen_names and "mz" in current and "rt" in current:
                        seen_names.add(name)
                        entries.append({
                            "name":           name,
                            "precursor_mz":   current["mz"],
                            "retention_time": current["rt"],
                            "polarity":       polarity,
                            "inchikey":       current.get("inchikey"),
                        })
                current = {}
                in_peaks = False
                continue

            # Skip peak lines
            if in_peaks:
                continue
            if line.startswith("Num Peaks:"):
                in_peaks = True
                continue

            key, _, value = line.partition(": ")
            key = key.strip().upper()
            value = value.strip()

            if key == "NAME":
                current["name"] = value
            elif key == "PRECURSORMZ":
                try:
                    current["mz"] = float(value)
                except ValueError:
                    pass
            elif key == "RETENTIONTIME":
                try:
                    current["rt"] = float(value)
                except ValueError:
                    pass
            elif key == "INCHIKEY":
                current["inchikey"] = value if value.lower() not in ("internal standard", "istd", "") else None

    # Handle file that doesn't end with blank line
    if current.get("name") and current["name"].startswith("1_"):
        name = current["name"]
        if name not in seen_names and "mz" in current and "rt" in current:
            entries.append({
                "name":           name,
                "precursor_mz":   current["mz"],
                "retention_time": current["rt"],
                "polarity":       polarity,
                "inchikey":       current.get("inchikey"),
            })

    return entries


def main():
    parser = argparse.ArgumentParser(description="Seed IS library from MSP files")
    parser.add_argument("--pos", required=True, help="Positive-mode MSP file")
    parser.add_argument("--neg", required=True, help="Negative-mode MSP file")
    parser.add_argument("--chromatography", default="HILIC",
                        help="Chromatography method label (default: HILIC)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print IS found without writing to DB")
    args = parser.parse_args()

    pos_path = Path(args.pos)
    neg_path = Path(args.neg)

    for p in (pos_path, neg_path):
        if not p.is_file():
            print(f"ERROR: {p} not found")
            sys.exit(1)

    print(f"Parsing positive IS from {pos_path.name} ...")
    pos_is = parse_is_from_msp(pos_path, polarity="Pos")
    print(f"  Found {len(pos_is)} unique IS")

    print(f"Parsing negative IS from {neg_path.name} ...")
    neg_is = parse_is_from_msp(neg_path, polarity="Neg")
    print(f"  Found {len(neg_is)} unique IS")

    all_is = pos_is + neg_is
    print()
    print(f"{'Name':<40} {'Pol':>4}  {'m/z':>10}  {'RT (min)':>9}")
    print("-" * 68)
    for entry in all_is:
        print(f"{entry['name']:<40} {entry['polarity']:>4}  {entry['precursor_mz']:>10.4f}  {entry['retention_time']:>9.4f}")

    if args.dry_run:
        print(f"\nDry run — {len(all_is)} IS NOT written to DB.")
        return

    from rapidqcms.db.connection import get_session, init_db
    from rapidqcms.db.models import InternalStandard

    init_db()
    n_added = 0
    n_skipped = 0

    with get_session() as session:
        for entry in all_is:
            existing = (
                session.query(InternalStandard)
                .filter_by(
                    name=entry["name"],
                    chromatography=args.chromatography,
                    polarity=entry["polarity"],
                )
                .first()
            )
            if existing:
                n_skipped += 1
                continue
            session.add(InternalStandard(
                name=entry["name"],
                chromatography=args.chromatography,
                polarity=entry["polarity"],
                precursor_mz=entry["precursor_mz"],
                retention_time=entry["retention_time"],
                inchikey=entry.get("inchikey"),
            ))
            n_added += 1
        session.commit()

    print(f"\nLoaded {n_added} IS into DB ({n_skipped} already existed).")
    print(f"Chromatography: {args.chromatography}")


if __name__ == "__main__":
    main()
