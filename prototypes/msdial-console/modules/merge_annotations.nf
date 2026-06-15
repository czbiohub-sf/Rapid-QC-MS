process MERGE_ANNOTATIONS {
    tag "${polarity}"
    label 'process_low'

    publishDir "${params.outdir}/${polarity}/annotations", mode: 'copy'

    input:
    tuple val(polarity), path(tier1_hits, stageAs: 'tier1_*')
    tuple val(polarity2), path(tier2_hits, stageAs: 'tier2_*')
    tuple val(polarity3), path(tier3_hits, stageAs: 'tier3_*')
    tuple val(polarity4), path(tier4_hits, stageAs: 'tier4_*')

    output:
    tuple val(polarity), path("merged_annotations.csv"), emit: merged

    script:
    def t1 = tier1_hits.name != 'NO_FILE' ? "--tier1 ${tier1_hits}" : ''
    def t2 = tier2_hits.name != 'NO_FILE' ? "--tier2 ${tier2_hits}" : ''
    def t3 = tier3_hits.name != 'NO_FILE' ? "--tier3 ${tier3_hits}" : ''
    def t4 = tier4_hits.name != 'NO_FILE' ? "--tier4 ${tier4_hits}" : ''
    def t4_top_k = params.merge_mistcf_top_k ?: 3
    """
    set -euo pipefail

    python3.9 ${projectDir}/scripts/merge_annotations.py \
        ${t1} ${t2} ${t3} ${t4} \
        --tier4-top-k ${t4_top_k} \
        --output merged_annotations.csv
    """
}
