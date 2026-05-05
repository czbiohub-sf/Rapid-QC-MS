process MIST_CF {
    tag "${polarity}"
    label 'process_medium'

    publishDir "${params.outdir}/${polarity}/mist_cf", mode: 'copy'

    input:
    tuple val(polarity), path(msp_file)

    output:
    tuple val(polarity), path("mist_cf_out/formatted_output.tsv"), emit: formulas
    tuple val(polarity), path("mist_cf_out/subform_assigns"),      emit: subforms

    script:
    def instrument = params.mist_cf_instrument ?: 'Orbitrap (LCMS)'
    def ppm        = params.mist_cf_ppm ?: 5
    """
    set -euo pipefail

    echo "[${polarity}] Running MIST-CF formula prediction"
    echo "[${polarity}] Input MSP: ${msp_file}"

    source /hpc/apps/anaconda/25.3.1/etc/profile.d/conda.sh
    conda activate ms-gen
    export LD_LIBRARY_PATH=\$CONDA_PREFIX/lib:\$LD_LIBRARY_PATH

    # Convert MSP → MGF for MIST-CF
    python3 ${projectDir}/scripts/msp_to_mgf.py \
        --input ${msp_file} \
        --output query_spectra.mgf

    # Run MIST-CF
    export SIRIUS_PATH="${params.sirius_decomp_binary}"

    python ${params.mist_cf_dir}/src/mist_cf/mist_cf_score/predict_mgf.py \
        --id-key FEATURE_ID \
        --num-workers ${task.cpus} \
        --batch-size 16 \
        --save-dir mist_cf_out \
        --mgf-file query_spectra.mgf \
        --checkpoint-pth ${params.mist_cf_model} \
        --fast-model ${params.mist_cf_fast_model} \
        --fast-num 256 \
        --instrument-override "${instrument}" \
        --decomp-ppm ${ppm} \
        --decomp-filter RDBE

    echo "[${polarity}] MIST-CF finished"
    """
}
