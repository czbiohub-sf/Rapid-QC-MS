process BUILD_PSEUDOLIBRARY {
    tag "pseudolibrary"
    label 'process_low'

    publishDir "${params.outdir}/pseudolibrary", mode: 'copy'

    input:
    path msp_files
    path diffms_preds
    path diffms_input

    output:
    path "pseudolibrary.msp", emit: library

    script:
    """
    set -euo pipefail

    echo "Building pseudolibrary from DiffMS predictions"

    # Concatenate all MSP files if multiple
    cat ${msp_files} > combined_spectra.msp

    python3.9 ${projectDir}/scripts/build_pseudolibrary.py \
        --msp combined_spectra.msp \
        --preds ${diffms_preds} \
        --labels ${diffms_input}/labels.tsv \
        --output pseudolibrary.msp

    echo "Pseudolibrary build complete"
    """
}
