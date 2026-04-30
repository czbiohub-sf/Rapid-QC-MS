#!/usr/bin/env python3
"""Convert MS-DIAL Console AlignResult exports to a simple feature matrix."""

from __future__ import annotations

import argparse
import csv
import math
import re
from collections import OrderedDict
from pathlib import Path
from typing import Iterable


OUTPUT_COLS = [
    "feature_id",
    "mz",
    "rt",
    "polarity",
    "adduct",
    "isotope_parent_id",
    "occurrence_overall",
]


def to_float(value: object, default: float = 0.0) -> float:
    text = str(value).strip()
    if text == "":
        return default
    try:
        num = float(text)
    except ValueError:
        return default
    if math.isnan(num) or math.isinf(num):
        return default
    return num


def normalize_id(value: object) -> str:
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null", "na"}:
        return ""
    return text


def safe_token(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", text.strip())


def intensity_name_from_raw(sample_name: str) -> str:
    return f"intensity_{sample_name.replace('.mzML', '')}"


def read_msdial_table(path: Path) -> list[list[str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.reader(handle, delimiter="\t"))
    if not rows:
        raise ValueError(f"File is empty: {path}")
    width = max(len(row) for row in rows)
    return [row + [""] * (width - len(row)) for row in rows]


def col_idx(headers: list[str], name: str) -> int:
    try:
        return headers.index(name)
    except ValueError:
        return -1


def source_intensity_cols(path: Path) -> list[str]:
    rows = read_msdial_table(path)
    row_type = rows[1]
    headers = [str(x).strip() for x in rows[4]]
    sample_indices = [
        i
        for i, sample_type in enumerate(row_type)
        if str(sample_type).strip() in {"Sample", "QC", "Blank"}
    ]
    return [intensity_name_from_raw(headers[i]) for i in sample_indices]


def iter_msdial_rows(path: Path, tool_key: str, polarity: str):
    rows = read_msdial_table(path)
    row_type = rows[1]
    headers = [str(x).strip() for x in rows[4]]
    data_rows = rows[5:]

    sample_indices = [
        i
        for i, sample_type in enumerate(row_type)
        if str(sample_type).strip() in {"Sample", "QC", "Blank"}
    ]
    intensity_col_names = [intensity_name_from_raw(headers[i]) for i in sample_indices]

    idx_align = col_idx(headers, "Alignment ID")
    idx_mz = col_idx(headers, "Average Mz")
    idx_rt = col_idx(headers, "Average Rt(min)")
    idx_adduct = col_idx(headers, "Adduct type")
    idx_iso_parent = col_idx(headers, "Isotope tracking parent ID")

    feature_ids = [f"{tool_key}_{polarity}_{i + 1:06d}" for i in range(len(data_rows))]
    align_to_feature: dict[str, str] = {}
    if idx_align >= 0:
        for i, row in enumerate(data_rows):
            align_id = normalize_id(row[idx_align])
            if align_id and align_id not in align_to_feature:
                align_to_feature[align_id] = feature_ids[i]

    for i, row in enumerate(data_rows):
        adduct = normalize_id(row[idx_adduct]) if idx_adduct >= 0 else ""
        if not adduct:
            adduct = "unknown"

        isotope_parent_id = ""
        if idx_iso_parent >= 0:
            parent = normalize_id(row[idx_iso_parent])
            isotope_parent_id = align_to_feature.get(parent, "")

        intensities: dict[str, float] = {}
        non_zero = 0
        for name, idx in zip(intensity_col_names, sample_indices):
            value = to_float(row[idx], 0.0)
            intensities[name] = value
            if value > 0.0:
                non_zero += 1

        fixed = {
            "feature_id": feature_ids[i],
            "mz": round(to_float(row[idx_mz], 0.0), 4) if idx_mz >= 0 else 0.0,
            "rt": round(to_float(row[idx_rt], 0.0), 2) if idx_rt >= 0 else 0.0,
            "polarity": polarity,
            "adduct": adduct,
            "isotope_parent_id": isotope_parent_id,
            "occurrence_overall": round(non_zero / len(sample_indices), 4)
            if sample_indices
            else 0.0,
        }
        yield fixed, intensities


def write_combined_matrix(
    sources: list[tuple[Path, str]], tool_name: str, output: Path
) -> None:
    tool_key = safe_token(tool_name)
    intensity_union: OrderedDict[str, None] = OrderedDict()

    for path, _polarity in sources:
        for col in source_intensity_cols(path):
            intensity_union.setdefault(col, None)

    intensity_cols = list(intensity_union.keys())
    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(OUTPUT_COLS + intensity_cols)

        feature_id_counts: dict[str, int] = {}
        for path, polarity in sources:
            for fixed, intensity in iter_msdial_rows(path, tool_key, polarity):
                fid = str(fixed["feature_id"])
                if fid in feature_id_counts:
                    feature_id_counts[fid] += 1
                    fixed["feature_id"] = f"{fid}_dup{feature_id_counts[fid]}"
                else:
                    feature_id_counts[fid] = 0

                writer.writerow(
                    [fixed[col] for col in OUTPUT_COLS]
                    + [intensity.get(col, 0.0) for col in intensity_cols]
                )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pos", type=Path, help="Positive-mode AlignResult-*.msdial")
    parser.add_argument("--neg", type=Path, help="Negative-mode AlignResult-*.msdial")
    parser.add_argument("--tool-name", default="msdial-console")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    if args.pos is None and args.neg is None:
        parser.error("At least one of --pos or --neg is required.")
    return args


def main() -> None:
    args = parse_args()
    sources: list[tuple[Path, str]] = []
    if args.pos is not None:
        sources.append((args.pos, "pos"))
    if args.neg is not None:
        sources.append((args.neg, "neg"))

    missing = [str(path) for path, _ in sources if not path.exists()]
    if missing:
        raise FileNotFoundError("Missing AlignResult input(s): " + ", ".join(missing))

    write_combined_matrix(sources, args.tool_name, args.output)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
