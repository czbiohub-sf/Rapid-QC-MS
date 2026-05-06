#!/usr/bin/env python3.9
"""Add tiered annotation attributes to msknit molecular network GraphML.

Reads the merged annotations CSV and injects annotation columns as node
attributes in the GraphML file.  The join key is alignment_id (present
as a node data attribute in msknit output).

The annotated GraphML can be opened in Cytoscape or other viewers with
annotation columns immediately visible for filtering, coloring, and labeling.
"""

from __future__ import annotations

import argparse
import csv
import xml.etree.ElementTree as ET
from pathlib import Path


GRAPHML_NS = "http://graphml.graphstruct.org/graphml"

# Annotation columns to add as node attributes
ANNOTATION_KEYS = [
    ("best_tier",       "string"),
    ("t1_name",         "string"),
    ("t1_formula",      "string"),
    ("t1_smiles",       "string"),
    ("t1_inchikey",     "string"),
    ("t1_cosine",       "double"),
    ("t1_matched_peaks","int"),
    ("t2_name",         "string"),
    ("t2_formula",      "string"),
    ("t2_smiles",       "string"),
    ("t2_inchikey",     "string"),
    ("t2_cosine",       "double"),
    ("t2_matched_peaks","int"),
    ("t3_name",         "string"),
    ("t3_formula",      "string"),
    ("t3_smiles",       "string"),
    ("t3_inchikey",     "string"),
    ("t3_cosine",       "double"),
    ("t3_matched_peaks","int"),
    ("t4_formula",      "string"),
    ("t4_adduct",       "string"),
    ("t4_score",        "double"),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--network", type=Path, required=True,
                        help="Input GraphML from msknit")
    parser.add_argument("--annotations", type=Path, required=True,
                        help="Merged annotations CSV")
    parser.add_argument("--output", type=Path, required=True,
                        help="Output annotated GraphML")
    args = parser.parse_args()

    # Load annotations keyed by alignment_id
    annotations: dict[str, dict] = {}
    with args.annotations.open("r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            aid = row.get("alignment_id", "")
            if aid:
                annotations[aid] = row

    print(f"Loaded {len(annotations)} annotations")

    # Parse GraphML
    ET.register_namespace("", GRAPHML_NS)
    tree = ET.parse(args.network)
    root = tree.getroot()

    # Add key declarations for annotation attributes
    # Insert before the <graph> element
    graph_elem = root.find(f"{{{GRAPHML_NS}}}graph")
    graph_idx = list(root).index(graph_elem)

    for attr_name, attr_type in ANNOTATION_KEYS:
        key_id = f"ann_{attr_name}"
        key_elem = ET.Element(f"{{{GRAPHML_NS}}}key")
        key_elem.set("id", key_id)
        key_elem.set("for", "node")
        key_elem.set("attr.name", attr_name)
        key_elem.set("attr.type", attr_type)
        root.insert(graph_idx, key_elem)
        graph_idx += 1  # keep inserting before graph

    # Find the alignment_id key id
    aid_key_id = None
    for key_elem in root.findall(f"{{{GRAPHML_NS}}}key"):
        if key_elem.get("attr.name") == "alignment_id":
            aid_key_id = key_elem.get("id")
            break

    if not aid_key_id:
        print("WARNING: No alignment_id key found in GraphML")
        aid_key_id = "aid"

    # Add annotation data to each node
    matched = 0
    for node in graph_elem.findall(f"{{{GRAPHML_NS}}}node"):
        # Find this node's alignment_id
        aid = None
        for data_elem in node.findall(f"{{{GRAPHML_NS}}}data"):
            if data_elem.get("key") == aid_key_id:
                aid = data_elem.text
                break

        if aid and aid in annotations:
            matched += 1
            ann = annotations[aid]
            for attr_name, attr_type in ANNOTATION_KEYS:
                val = ann.get(attr_name, "")
                if val:
                    data_elem = ET.SubElement(node, f"{{{GRAPHML_NS}}}data")
                    data_elem.set("key", f"ann_{attr_name}")
                    data_elem.text = val

    # Write output
    tree.write(args.output, xml_declaration=True, encoding="UTF-8")
    print(f"Annotated {matched}/{len(list(graph_elem.findall(f'{{{GRAPHML_NS}}}node')))} nodes")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
