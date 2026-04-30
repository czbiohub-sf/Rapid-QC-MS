#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

params.manifest_pos  = null
params.manifest_neg  = null
params.params_pos    = null
params.params_neg    = null
params.tool_name     = 'msdial-console'
params.outdir        = 'results'
params.msdial_binary = '/hpc/mydata/anthony.goering/opt/msdial4/MsdialConsoleApp'

include { MSDIAL_CONSOLE as MSDIAL_POS } from './modules/msdial'
include { MSDIAL_CONSOLE as MSDIAL_NEG } from './modules/msdial'
include { STANDARDIZE }                   from './modules/standardize'

workflow {

    if ( !params.manifest_pos && !params.manifest_neg ) {
        error "At least one of --manifest_pos or --manifest_neg is required."
    }

    ch_pos = params.manifest_pos
        ? Channel.of( tuple('pos', file(params.manifest_pos), file(params.params_pos)) )
        : Channel.empty()

    ch_neg = params.manifest_neg
        ? Channel.of( tuple('neg', file(params.manifest_neg), file(params.params_neg)) )
        : Channel.empty()

    // Pos and neg run in parallel as independent SLURM jobs
    MSDIAL_POS( ch_pos )
    MSDIAL_NEG( ch_neg )

    // Collect AlignResult outputs from both polarities, then standardize
    ch_align = MSDIAL_POS.out.align_result
        .mix( MSDIAL_NEG.out.align_result )
        .collect()

    STANDARDIZE( ch_align )
}
