process PYCUTTER_STEP1 {
    tag "${polarity}"
    label 'process_low'

    publishDir "${params.outdir}/${polarity}/pycutter", mode: 'copy'

    input:
    tuple val(polarity), path(align_result)

    output:
    tuple val(polarity), path("*_PyCutterStep1_Export.xlsx"), emit: step1_xlsx
    path "*_QC_Report.html",                                   emit: qc_report, optional: true
    path "*.png",                                              emit: plots, optional: true

    script:
    def pol_long = polarity == 'pos' ? 'positive' : 'negative'
    def out_name = "${params.pycutter_experiment}_${polarity == 'pos' ? 'Pos' : 'Neg'}Align_PyCutterStep1_Export.xlsx"
    def conda_setup = params.conda_init ?: ''
    """
    set -euo pipefail

    echo "[${polarity}] Running PyCutter Step 1"

    ${conda_setup ? conda_setup + ' && conda activate omni' : '# Running in container'}

    python ${projectDir}/scripts/run_pycutter.py \
        --pycutter-dir ${params.pycutter_dir} \
        step1 \
        --input ${align_result} \
        --output ${out_name} \
        --polarity ${pol_long}

    echo "[${polarity}] PyCutter Step 1 finished"
    """
}

process PYCUTTER_STEP2 {
    tag "combine"
    label 'process_low'

    publishDir "${params.outdir}/pycutter", mode: 'copy'

    input:
    path pos_xlsx, stageAs: 'pos_*'
    path neg_xlsx, stageAs: 'neg_*'

    output:
    path "*_Metabolites_Combined.xlsx", emit: combined
    path "*_ProcessingLog.txt",          emit: log, optional: true
    path "*_Perseus_*.txt",              emit: perseus, optional: true

    script:
    def conda_setup = params.conda_init ?: ''
    """
    set -euo pipefail

    echo "Running PyCutter Step 2 (combine polarities)"

    ${conda_setup ? conda_setup + ' && conda activate omni' : '# Running in container'}

    python ${projectDir}/scripts/run_pycutter.py \
        --pycutter-dir ${params.pycutter_dir} \
        step2 \
        --pos-xlsx ${pos_xlsx} \
        --neg-xlsx ${neg_xlsx} \
        --experiment ${params.pycutter_experiment} \
        --outdir .

    echo "PyCutter Step 2 finished"
    """
}
