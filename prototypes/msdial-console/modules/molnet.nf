process MOLNET {
    tag "${polarity}"
    label 'process_medium'

    publishDir "${params.outdir}/${polarity}/molnet", mode: 'copy'

    input:
    tuple val(polarity), path(msp_file)

    output:
    tuple val(polarity), path("molnet_out/edges.csv"),  emit: edges
    tuple val(polarity), path("molnet_out/nodes.csv"),  emit: nodes
    path "molnet_out/*",                                 emit: all_output

    script:
    def graphml_flag = params.molnet_graphml ? '--graphml' : ''
    def cosine_thresh = params.molnet_min_cosine ?: 0.7
    def matched_thresh = params.molnet_min_matched ?: 4
    def top_k = params.molnet_top_k ?: 10
    """
    set -euo pipefail

    echo "[${polarity}] Running molecular networking"

    ${params.molnet_binary} \
        --input ${msp_file} \
        --outdir molnet_out \
        --min-cosine ${cosine_thresh} \
        --min-matched ${matched_thresh} \
        --top-k ${top_k} \
        --threads ${task.cpus} \
        --sqrt-transform \
        ${graphml_flag}

    echo "[${polarity}] Molecular networking finished"
    """
}
