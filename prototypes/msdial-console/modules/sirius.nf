process SIRIUS_FORMULAS {
    tag "${polarity}"
    label 'process_high'

    publishDir "${params.outdir}/${polarity}", mode: 'copy', pattern: 'sirius_summary/*'

    input:
    tuple val(polarity), path(msp_file)

    output:
    tuple val(polarity), path("sirius_project"),       emit: project
    tuple val(polarity), path("sirius_summary"),        emit: summary
    tuple val(polarity), path("sirius_summary/formulas_summary.tsv"), emit: formulas, optional: true

    script:
    def profile_flag = params.sirius_profile ?: 'orbitrap'
    def candidates   = params.sirius_candidates ?: 5
    def ppm          = params.sirius_ppm ?: 10
    """
    set -euo pipefail

    echo "[${polarity}] Running SIRIUS formula prediction"
    echo "[${polarity}] Input MSP: ${msp_file}"
    echo "[${polarity}] Profile: ${profile_flag}, Candidates: ${candidates}, PPM: ${ppm}"

    # Run formula prediction + fragmentation trees
    ${params.sirius_binary} \
        --cores ${task.cpus} \
        --input ${msp_file} \
        --output sirius_project \
        formulas \
            --profile ${profile_flag} \
            --candidates ${candidates} \
            --ppm-max ${ppm}

    echo "[${polarity}] Exporting summaries"

    # Export TSV summaries
    mkdir -p sirius_summary
    ${params.sirius_binary} \
        --cores ${task.cpus} \
        sirius_project \
        summaries \
            --output sirius_summary

    echo "[${polarity}] SIRIUS finished"
    """
}
