process ANNOTATE_FEATURES {
    tag "${polarity}"
    label 'process_low'

    publishDir "${params.outdir}/${polarity}", mode: 'copy'

    input:
    tuple val(polarity), path(feature_table)
    tuple val(polarity2), path(annotations)

    output:
    tuple val(polarity), path("annotated_feature_table.tsv"), emit: annotated

    script:
    """
    set -euo pipefail

    python3.9 ${projectDir}/scripts/annotate_feature_table.py \
        --feature-table ${feature_table} \
        --annotations ${annotations} \
        --output annotated_feature_table.tsv
    """
}
