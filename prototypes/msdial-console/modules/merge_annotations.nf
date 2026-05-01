process MERGE_ANNOTATIONS {
    tag "${polarity}"
    label 'process_low'

    publishDir "${params.outdir}/${polarity}/annotations", mode: 'copy'

    input:
    tuple val(polarity), path(tier1_hits)
    tuple val(polarity2), path(tier2_hits)
    tuple val(polarity3), path(tier3_hits)

    output:
    tuple val(polarity), path("merged_annotations.csv"), emit: merged

    script:
    def t1 = tier1_hits.name != 'NO_FILE' ? "--tier1 ${tier1_hits}" : ''
    def t2 = tier2_hits.name != 'NO_FILE' ? "--tier2 ${tier2_hits}" : ''
    def t3 = tier3_hits.name != 'NO_FILE' ? "--tier3 ${tier3_hits}" : ''
    """
    set -euo pipefail

    python3.9 ${projectDir}/scripts/merge_annotations.py \
        ${t1} ${t2} ${t3} \
        --output merged_annotations.csv
    """
}
