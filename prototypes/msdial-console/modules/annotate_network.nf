process ANNOTATE_NETWORK {
    tag "${polarity}"
    label 'process_low'

    publishDir "${params.outdir}/${polarity}/msknit", mode: 'copy'

    input:
    tuple val(polarity), path(network)
    tuple val(polarity2), path(annotations)

    output:
    tuple val(polarity), path("annotated_network.graphml"), emit: annotated

    script:
    """
    set -euo pipefail

    python3.9 ${projectDir}/scripts/annotate_network.py \
        --network ${network} \
        --annotations ${annotations} \
        --output annotated_network.graphml
    """
}
