process MOLNET {
    tag "${polarity}"
    label 'process_medium'

    publishDir "${params.outdir}/${polarity}/msknit", mode: 'copy'

    input:
    tuple val(polarity), path(msp_file)

    output:
    tuple val(polarity), path("msknit_out/edges.csv"),  emit: edges
    tuple val(polarity), path("msknit_out/nodes.csv"),  emit: nodes
    path "msknit_out/*",                                 emit: all_output

    script:
    def graphml_flag = params.msknit_graphml ? '--graphml' : ''
    def cosine_thresh = params.msknit_min_cosine ?: 0.7
    def matched_thresh = params.msknit_min_matched ?: 4
    def top_k = params.msknit_top_k ?: 10
    """
    set -euo pipefail

    echo "[${polarity}] Running molecular networking"

    ${params.msknit_binary} \
        --input ${msp_file} \
        --outdir msknit_out \
        --min-cosine ${cosine_thresh} \
        --min-matched ${matched_thresh} \
        --top-k ${top_k} \
        --threads ${task.cpus} \
        --sqrt-transform \
        ${graphml_flag}

    echo "[${polarity}] Molecular networking finished"
    """
}
