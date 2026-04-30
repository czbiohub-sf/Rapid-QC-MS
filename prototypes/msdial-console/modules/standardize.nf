process STANDARDIZE {
    tag "standardize"
    label 'process_low'

    publishDir "${params.outdir}", mode: 'copy'

    input:
    val align_results

    output:
    path "feature_matrix.csv", emit: matrix

    script:
    def pos_entry = align_results.find { it[0] == 'pos' }
    def neg_entry = align_results.find { it[0] == 'neg' }
    def pos_arg   = pos_entry ? "--pos ${pos_entry[1]}" : ''
    def neg_arg   = neg_entry ? "--neg ${neg_entry[1]}" : ''
    """
    set -euo pipefail

    python3 ${projectDir}/scripts/standardize_msdial_console.py \
        ${pos_arg} \
        ${neg_arg} \
        --tool-name ${params.tool_name} \
        --output feature_matrix.csv
    """
}
