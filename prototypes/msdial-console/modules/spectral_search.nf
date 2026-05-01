process SPECTRAL_SEARCH {
    tag "${tier_name}:${polarity}"
    label 'process_medium'

    publishDir "${params.outdir}/${polarity}/annotations/${tier_name}", mode: 'copy'

    input:
    tuple val(polarity), path(query_msp)
    tuple val(tier_name), path(library_msp)

    output:
    tuple val(polarity), val(tier_name), path("hits.csv"), emit: hits

    script:
    def min_cosine = params.search_min_cosine ?: 0.7
    def min_matched = params.search_min_matched ?: 4
    def precursor_tol = params.search_precursor_tol ?: 0.5
    """
    set -euo pipefail

    echo "[${polarity}:${tier_name}] Searching ${query_msp} against ${library_msp}"

    ${params.msknit_binary} search \
        --query ${query_msp} \
        --library ${library_msp} \
        --outdir . \
        --min-cosine ${min_cosine} \
        --min-matched ${min_matched} \
        --precursor-tol ${precursor_tol} \
        --top-k 5 \
        --threads ${task.cpus} \
        --sqrt-transform

    echo "[${polarity}:${tier_name}] Search complete"
    """
}
