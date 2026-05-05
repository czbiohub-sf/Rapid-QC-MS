process PREPARE_DIFFMS {
    tag "prepare_diffms"
    label 'process_low'

    input:
    tuple val(polarity), path(msp_file)
    tuple val(polarity2), path(mistcf_output)
    tuple val(polarity3), path(mistcf_subforms)

    output:
    path "diffms_input", emit: diffms_dir

    script:
    """
    set -euo pipefail

    python3.9 ${projectDir}/scripts/mistcf_to_diffms.py \
        --msp ${msp_file} \
        --mistcf-output ${mistcf_output} \
        --mistcf-subforms ${mistcf_subforms} \
        --polarity ${polarity} \
        --output diffms_input
    """
}
