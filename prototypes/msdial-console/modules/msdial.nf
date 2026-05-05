process MSDIAL_CONSOLE {
    tag "${polarity}"
    label 'process_high'

    publishDir "${params.outdir}/${polarity}", mode: 'copy', pattern: 'output/AlignResult-*.msdial'
    publishDir "${params.outdir}/${polarity}", mode: 'copy', pattern: 'output/AlignResult-*.msp'

    input:
    tuple val(polarity), path(manifest), path(params_file)

    output:
    tuple val(polarity), path("output/AlignResult-*.msdial"), emit: align_result
    tuple val(polarity), path("output/AlignResult-*.msp"),    emit: msp_library
    path "output/**",                                          emit: all_output

    script:
    """
    set -euo pipefail

    # Rewrite dynamic params into a new file (staged copy may be a symlink)
    PARAMS_RUNTIME="params_runtime_${polarity}.txt"
    sed \
        -e 's|^[[:space:]]*Number of threads:.*|Number of threads: ${task.cpus}|' \
        -e 's|^[[:space:]]*Intermediate file output folder:.*|Intermediate file output folder: ${PWD}/intermediate|' \
        "${params_file}" > "\${PARAMS_RUNTIME}"

    # Ensure intermediate folder is set even if not present in the original params
    if ! grep -q 'Intermediate file output folder' "\${PARAMS_RUNTIME}"; then
        echo "Intermediate file output folder: ${PWD}/intermediate" >> "\${PARAMS_RUNTIME}"
    fi

    mkdir -p output intermediate

    echo "[$polarity] MS-DIAL Console starting"
    echo "[$polarity] threads: ${task.cpus}"
    echo "[$polarity] manifest: ${manifest}"
    echo "[$polarity] params: \${PARAMS_RUNTIME}"
    echo "[$polarity] output: \${PWD}/output"

    ${params.msdial_binary} lcmsdda \
        -i ${manifest} \
        -o output \
        -m "\${PARAMS_RUNTIME}"

    echo "[$polarity] MS-DIAL Console finished"

    # Verify AlignResult was produced
    if ! ls output/AlignResult-*.msdial 1>/dev/null 2>&1; then
        echo "ERROR: No AlignResult file found in output/" >&2
        exit 1
    fi
    """
}
