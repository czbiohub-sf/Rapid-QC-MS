#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

params.manifest_pos  = null
params.manifest_neg  = null
params.params_pos    = null
params.params_neg    = null
params.tool_name     = 'msdial-console'
params.outdir        = 'results'
params.msdial_binary = '/hpc/mydata/anthony.goering/opt/msdial4/MsdialConsoleApp'

// Molecular networking (optional)
params.run_msknit         = false
params.msknit_binary      = '/hpc/mydata/anthony.goering/opt/msknit/msknit'
params.msknit_min_cosine  = 0.7
params.msknit_min_matched = 4
params.msknit_top_k       = 10
params.msknit_graphml     = true

// PyCutter QC and preprocessing (optional)
params.run_pycutter          = false
params.pycutter_dir          = '/home/anthony.goering/repos/PyCutterForMetabolites'
params.pycutter_experiment   = 'experiment'

// Tiered annotation pipeline (optional)
params.run_annotation       = false
params.reference_library    = null    // Tier 1: real spectral library MSP (e.g., GNPS, MoNA)
params.predicted_library    = null    // Tier 2: predicted spectral library MSP (e.g., HMDB CFM-ID)
params.search_min_cosine    = 0.7
params.search_min_matched   = 4
params.search_precursor_tol = 0.5
params.sirius_binary        = '/hpc/mydata/anthony.goering/opt/sirius/sirius/bin/sirius'
params.sirius_profile       = 'orbitrap'
params.sirius_candidates    = 5
params.sirius_ppm           = 10
params.diffms_repo          = '/home/anthony.goering/repos/DiffMS'
params.diffms_checkpoint    = null
params.diffms_samples       = 10

include { MSDIAL_CONSOLE as MSDIAL_POS } from './modules/msdial'
include { MSDIAL_CONSOLE as MSDIAL_NEG } from './modules/msdial'
include { STANDARDIZE }                   from './modules/standardize'
include { MSKNIT as MSKNIT_POS }          from './modules/msknit'
include { MSKNIT as MSKNIT_NEG }          from './modules/msknit'
include { PYCUTTER_STEP1 as PYCUTTER_POS } from './modules/pycutter'
include { PYCUTTER_STEP1 as PYCUTTER_NEG } from './modules/pycutter'
include { PYCUTTER_STEP2 }                 from './modules/pycutter'
include { SPECTRAL_SEARCH as SEARCH_T1_POS } from './modules/spectral_search'
include { SPECTRAL_SEARCH as SEARCH_T1_NEG } from './modules/spectral_search'
include { SPECTRAL_SEARCH as SEARCH_T2_POS } from './modules/spectral_search'
include { SPECTRAL_SEARCH as SEARCH_T2_NEG } from './modules/spectral_search'
include { SIRIUS_FORMULAS as SIRIUS_POS } from './modules/sirius'
include { SIRIUS_FORMULAS as SIRIUS_NEG } from './modules/sirius'
include { PREPARE_DIFFMS }                from './modules/prepare_diffms'
include { DIFFMS_PREDICT }                from './modules/diffms'
include { BUILD_PSEUDOLIBRARY }            from './modules/pseudolibrary'
include { MERGE_ANNOTATIONS as MERGE_POS } from './modules/merge_annotations'
include { MERGE_ANNOTATIONS as MERGE_NEG } from './modules/merge_annotations'

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

    // --- Feature detection ---
    MSDIAL_POS( ch_pos )
    MSDIAL_NEG( ch_neg )

    // Standardize feature matrix
    ch_pos_align = MSDIAL_POS.out.align_result.map { pol, f -> f }
        .ifEmpty( file('NO_FILE') )
    ch_neg_align = MSDIAL_NEG.out.align_result.map { pol, f -> f }
        .ifEmpty( file('NO_FILE') )

    STANDARDIZE( ch_pos_align, ch_neg_align )

    // --- Molecular networking (optional) ---
    if ( params.run_msknit ) {
        MSKNIT_POS( MSDIAL_POS.out.msp_library )
        MSKNIT_NEG( MSDIAL_NEG.out.msp_library )
    }

    // --- PyCutter QC and preprocessing (optional) ---
    if ( params.run_pycutter ) {
        PYCUTTER_POS( MSDIAL_POS.out.align_result )
        PYCUTTER_NEG( MSDIAL_NEG.out.align_result )

        ch_pyc_pos = PYCUTTER_POS.out.step1_xlsx.map { pol, f -> f }
            .ifEmpty( file('NO_FILE') )
        ch_pyc_neg = PYCUTTER_NEG.out.step1_xlsx.map { pol, f -> f }
            .ifEmpty( file('NO_FILE') )

        PYCUTTER_STEP2( ch_pyc_pos, ch_pyc_neg )
    }

    // --- Tiered annotation pipeline (optional) ---
    if ( params.run_annotation ) {

        // Tier 1: Search against real spectral library
        ch_t1_pos_hits = Channel.of( tuple('pos', file('NO_FILE')) )
        ch_t1_neg_hits = Channel.of( tuple('neg', file('NO_FILE')) )

        if ( params.reference_library ) {
            ch_ref_lib = Channel.of( tuple('tier1_reference', file(params.reference_library)) )
            SEARCH_T1_POS( MSDIAL_POS.out.msp_library, ch_ref_lib )
            SEARCH_T1_NEG( MSDIAL_NEG.out.msp_library, ch_ref_lib )
            ch_t1_pos_hits = SEARCH_T1_POS.out.hits.map { pol, tier, f -> tuple(pol, f) }
            ch_t1_neg_hits = SEARCH_T1_NEG.out.hits.map { pol, tier, f -> tuple(pol, f) }
        }

        // Tier 2: Search against predicted spectral library (e.g., HMDB CFM-ID)
        ch_t2_pos_hits = Channel.of( tuple('pos', file('NO_FILE')) )
        ch_t2_neg_hits = Channel.of( tuple('neg', file('NO_FILE')) )

        if ( params.predicted_library ) {
            ch_pred_lib = Channel.of( tuple('tier2_predicted', file(params.predicted_library)) )
            SEARCH_T2_POS( MSDIAL_POS.out.msp_library, ch_pred_lib )
            SEARCH_T2_NEG( MSDIAL_NEG.out.msp_library, ch_pred_lib )
            ch_t2_pos_hits = SEARCH_T2_POS.out.hits.map { pol, tier, f -> tuple(pol, f) }
            ch_t2_neg_hits = SEARCH_T2_NEG.out.hits.map { pol, tier, f -> tuple(pol, f) }
        }

        // Tier 3: SIRIUS formula prediction → DiffMS de novo structure generation
        ch_t3_pos_hits = Channel.of( tuple('pos', file('NO_FILE')) )
        ch_t3_neg_hits = Channel.of( tuple('neg', file('NO_FILE')) )

        SIRIUS_POS( MSDIAL_POS.out.msp_library )
        SIRIUS_NEG( MSDIAL_NEG.out.msp_library )

        ch_diffms_pos = MSDIAL_POS.out.msp_library.join( SIRIUS_POS.out.summary )
        ch_diffms_neg = MSDIAL_NEG.out.msp_library.join( SIRIUS_NEG.out.summary )
        ch_prep = ch_diffms_pos.mix( ch_diffms_neg )

        ch_prep.map { polarity, msp, summary -> tuple(polarity, msp) }.set { ch_msp_for_prep }
        ch_prep.map { polarity, msp, summary -> tuple(polarity, summary) }.set { ch_summary_for_prep }

        PREPARE_DIFFMS( ch_msp_for_prep, ch_summary_for_prep )

        if ( params.diffms_checkpoint ) {
            DIFFMS_PREDICT( PREPARE_DIFFMS.out.diffms_dir )

            // Build pseudolibrary from DiffMS predictions
            ch_all_msp = MSDIAL_POS.out.msp_library.map { pol, f -> f }
                .mix( MSDIAL_NEG.out.msp_library.map { pol, f -> f } )
                .collect()

            BUILD_PSEUDOLIBRARY(
                ch_all_msp,
                DIFFMS_PREDICT.out.predictions,
                DIFFMS_PREDICT.out.input_data
            )
        }

        // Merge tiered annotations per polarity
        MERGE_POS( ch_t1_pos_hits, ch_t2_pos_hits, ch_t3_pos_hits )
        MERGE_NEG( ch_t1_neg_hits, ch_t2_neg_hits, ch_t3_neg_hits )
    }
}
