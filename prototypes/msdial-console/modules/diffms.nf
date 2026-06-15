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
    def conda_setup = params.conda_init ?: ''
    """
    set -euo pipefail

    ${conda_setup ? conda_setup + ' && conda activate diffms && export LD_LIBRARY_PATH=\$CONDA_PREFIX/lib:\$LD_LIBRARY_PATH' : '# Running in container'}

    echo "Running DiffMS structure prediction"
    echo "Input: ${diffms_input}"
    echo "Checkpoint: ${checkpoint}"
    echo "Samples per spectrum: ${n_samples}"

    cd ${params.diffms_repo}
    export PYTHONPATH=\${PWD}:\${PYTHONPATH:-}
    export WANDB_MODE=disabled

    python src/spec2mol_main.py \
        general.test_only=${checkpoint} \
        general.test_samples_to_generate=${n_samples} \
        general.wandb=disabled \
        general.gpus=1 \
        model.encoder_hidden_dim=512 \
        model.encoder_magma_modulo=2048 \
        dataset.name=msg \
        dataset.datadir=\${OLDPWD}/${diffms_input} \
        dataset.spec_folder=\${OLDPWD}/${diffms_input}/spec_files \
        dataset.labels_file=\${OLDPWD}/${diffms_input}/labels.tsv \
        dataset.split_file=\${OLDPWD}/${diffms_input}/split.tsv \
        dataset.subform_folder=\${OLDPWD}/${diffms_input}/subformulae \
        +dataset.allow_none_smiles=true

    cp -r preds \${OLDPWD}/preds

    echo "DiffMS finished"
    """
}
