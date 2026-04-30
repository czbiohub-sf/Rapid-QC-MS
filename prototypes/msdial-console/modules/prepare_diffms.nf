process PREPARE_DIFFMS {
    tag "prepare_diffms"
    label 'process_low'

    input:
    tuple val(polarity), path(msp_file)
    tuple val(polarity2), path(sirius_summary)

    output:
    path "diffms_input", emit: diffms_dir

    script:
    """
    set -euo pipefail

    python3.9 ${projectDir}/scripts/sirius_to_diffms.py \
        --msp ${msp_file} \
        --sirius-summary ${sirius_summary} \
        --polarity ${polarity} \
        --output diffms_input
    """
}
