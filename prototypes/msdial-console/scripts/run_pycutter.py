#!/usr/bin/env python3.9
"""CLI wrapper for PyCutter Step 1 + Step 2.

Invokes PyCutter's processing functions with CLI-provided paths,
avoiding the need to edit hardcoded variables in the original scripts.
"""

import argparse
import os
import sys


def run_step1(pycutter_dir, input_file, output_file, polarity):
    """Run PyCutter Step 1 on a single polarity file."""
    sys.path.insert(0, pycutter_dir)

    # Copy the annotation library to cwd if it exists in the pycutter dir
    lib_src = os.path.join(pycutter_dir, "MetaboliteAnnotationLibrary_v1.1.csv")
    if os.path.exists(lib_src) and not os.path.exists("MetaboliteAnnotationLibrary_v1.1.csv"):
        import shutil
        shutil.copy2(lib_src, "MetaboliteAnnotationLibrary_v1.1.csv")

    from MetabolitePyCutter_Step1_v1_1 import process_single_file
    result, summary = process_single_file(input_file, output_file, polarity=polarity)

    if result is not None:
        print(f"Step 1 {polarity} complete: {result.shape[0]} rows x {result.shape[1]} columns")
        print(f"Output: {output_file}")
    else:
        print(f"Step 1 {polarity} FAILED", file=sys.stderr)
        sys.exit(1)


def run_step2(pycutter_dir, pos_xlsx, neg_xlsx, experiment_name, output_dir):
    """Run PyCutter Step 2 to combine pos/neg."""
    sys.path.insert(0, pycutter_dir)

    # Patch the module-level config before importing main
    import PyCutterStep2 as step2
    step2.EXPERIMENT_NAME = experiment_name
    step2.BASE_PATH = output_dir
    step2.pos_file_path = pos_xlsx
    step2.neg_file_path = neg_xlsx
    step2.output_file_path = os.path.join(output_dir, f"{experiment_name}_Metabolites_Combined.xlsx")

    step2.main()
    print(f"Step 2 complete: {step2.output_file_path}")


def main():
    parser = argparse.ArgumentParser(description="CLI wrapper for PyCutter")
    parser.add_argument("--pycutter-dir", required=True,
                        help="Path to PyCutterForMetabolites repo")
    sub = parser.add_subparsers(dest="step", required=True)

    # Step 1
    s1 = sub.add_parser("step1", help="Run PyCutter Step 1 on one polarity")
    s1.add_argument("--input", required=True, help="MS-DIAL AlignResult .msdial file")
    s1.add_argument("--output", required=True, help="Output .xlsx path")
    s1.add_argument("--polarity", required=True, choices=["positive", "negative"])

    # Step 2
    s2 = sub.add_parser("step2", help="Run PyCutter Step 2 to combine polarities")
    s2.add_argument("--pos-xlsx", required=True, help="Step 1 positive output .xlsx")
    s2.add_argument("--neg-xlsx", required=True, help="Step 1 negative output .xlsx")
    s2.add_argument("--experiment", default="experiment", help="Experiment name")
    s2.add_argument("--outdir", default=".", help="Output directory")

    args = parser.parse_args()

    if args.step == "step1":
        run_step1(args.pycutter_dir, args.input, args.output, args.polarity)
    elif args.step == "step2":
        run_step2(args.pycutter_dir, args.pos_xlsx, args.neg_xlsx,
                  args.experiment, args.outdir)


if __name__ == "__main__":
    main()
