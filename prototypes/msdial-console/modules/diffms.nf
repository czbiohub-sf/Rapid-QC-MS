process DIFFMS_PREDICT {
    tag "diffms"
    label 'process_gpu'

    publishDir "${params.outdir}/diffms", mode: 'copy'

    input:
    path diffms_input

    output:
    path "preds",       emit: predictions
    path "diffms_input", emit: input_data

    script:
    def checkpoint = params.diffms_checkpoint
    def n_samples  = params.diffms_samples ?: 10
    """
    set -euo pipefail

    echo "Running DiffMS structure prediction"
    echo "Input: ${diffms_input}"
    echo "Checkpoint: ${checkpoint}"
    echo "Samples per spectrum: ${n_samples}"

    cd ${params.diffms_repo}

    python src/spec2mol_main.py \
        general.test_only=${checkpoint} \
        general.test_samples_to_generate=${n_samples} \
        general.wandb=disabled \
        general.gpus=1 \
        dataset.datadir=\${OLDPWD}/${diffms_input} \
        dataset.spec_folder=\${OLDPWD}/${diffms_input}/spec_files \
        dataset.labels_file=\${OLDPWD}/${diffms_input}/labels.tsv \
        dataset.split_file=\${OLDPWD}/${diffms_input}/split.tsv \
        dataset.subform_folder=\${OLDPWD}/${diffms_input}/subformulae

    cp -r preds \${OLDPWD}/preds

    echo "DiffMS finished"
    """
}
