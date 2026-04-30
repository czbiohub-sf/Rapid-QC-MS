#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  run_msdial_console.sh <polarity> <manifest.csv> <params.txt> <output_dir>

Environment:
  MSDIAL_CONSOLE  Path to patched MsdialConsoleApp. If unset, MsdialConsoleApp is resolved from PATH.

Example:
  MSDIAL_CONSOLE=/path/to/MsdialConsoleApp \
    run_msdial_console.sh pos input_manifest_pos.csv lcms_pos.txt out/pos
EOF
}

if [[ $# -ne 4 ]]; then
  usage
  exit 2
fi

polarity="$1"
manifest="$2"
params="$3"
out_dir="$4"
msdial_console="${MSDIAL_CONSOLE:-MsdialConsoleApp}"

case "$polarity" in
  pos|neg) ;;
  *)
    echo "ERROR: polarity must be 'pos' or 'neg'." >&2
    exit 2
    ;;
esac

if [[ ! -f "$manifest" ]]; then
  echo "ERROR: manifest not found: $manifest" >&2
  exit 1
fi

if [[ ! -f "$params" ]]; then
  echo "ERROR: params file not found: $params" >&2
  exit 1
fi

mkdir -p "$out_dir"

echo "[$polarity] MS-DIAL Console: $msdial_console"
echo "[$polarity] manifest: $manifest"
echo "[$polarity] params: $params"
echo "[$polarity] output: $out_dir"

"$msdial_console" lcmsdda \
  -i "$manifest" \
  -o "$out_dir" \
  -m "$params"

echo "[$polarity] done"
