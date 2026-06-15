process STANDARDIZE {
    tag "standardize"
    label 'process_low'

    publishDir "${params.outdir}", mode: 'copy'

    input:
    path pos_align, stageAs: 'pos_*'
    path neg_align, stageAs: 'neg_*'

    output:
    path "feature_matrix.csv", emit: matrix

    script:
    def pos_arg = pos_align.name != 'NO_FILE' ? "--pos ${pos_align}" : ''
    def neg_arg = neg_align.name != 'NO_FILE' ? "--neg ${neg_align}" : ''
    """
    set -euo pipefail

    python3.9 ${projectDir}/scripts/standardize_msdial_console.py \
        ${pos_arg} \
        ${neg_arg} \
        --tool-name ${params.tool_name} \
        --output feature_matrix.csv
    """
}
