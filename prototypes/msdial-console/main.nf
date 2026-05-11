#!/usr/bin/env nextflow
nextflow.enable.dsl = 2

params.manifest_pos  = null
params.manifest_neg  = null
params.params_pos    = null
params.params_neg    = null
params.tool_name     = 'msdial-console'
params.conda_init    = null
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
params.run_annotation        = false
params.curated_library       = null    // Tier 1: curated experimental spectral library MSP
params.experimental_library  = null    // Tier 2: larger experimental spectral library MSP (e.g., HMDB experimental)
params.predicted_library     = null    // Tier 3: predicted spectral library MSP (e.g., HMDB CFM-ID)
params.search_min_cosine     = 0.7
params.search_min_matched    = 4
params.search_precursor_tol  = 0.5

// MIST-CF formula prediction (Tier 4)
params.mist_cf_dir           = '/hpc/mydata/anthony.goering/opt/mist-cf'
params.mist_cf_model         = '/hpc/mydata/anthony.goering/opt/mist-cf/quickstart/models/mist_cf_best.ckpt'
params.mist_cf_fast_model    = '/hpc/mydata/anthony.goering/opt/mist-cf/quickstart/models/fast_filter_best.ckpt'
params.mist_cf_instrument    = 'Orbitrap (LCMS)'
params.mist_cf_ppm           = 5
params.sirius_decomp_binary  = '/hpc/mydata/anthony.goering/opt/sirius/bin/sirius'

// DiffMS de novo structure prediction
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
include { SPECTRAL_SEARCH as SEARCH_T3_POS } from './modules/spectral_search'
include { SPECTRAL_SEARCH as SEARCH_T3_NEG } from './modules/spectral_search'
include { MIST_CF as MIST_CF_POS }         from './modules/mist_cf'
include { MIST_CF as MIST_CF_NEG }         from './modules/mist_cf'
include { PREPARE_DIFFMS }                  from './modules/prepare_diffms'
include { DIFFMS_PREDICT }                  from './modules/diffms'
include { BUILD_PSEUDOLIBRARY }             from './modules/pseudolibrary'
include { MERGE_ANNOTATIONS as MERGE_POS }  from './modules/merge_annotations'
include { MERGE_ANNOTATIONS as MERGE_NEG }  from './modules/merge_annotations'
include { ANNOTATE_FEATURES as ANNOTATE_POS } from './modules/annotate_features'
include { ANNOTATE_FEATURES as ANNOTATE_NEG } from './modules/annotate_features'
include { ANNOTATE_NETWORK as ANNOTATE_NET_POS } from './modules/annotate_network'
include { ANNOTATE_NETWORK as ANNOTATE_NET_NEG } from './modules/annotate_network'

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

        // Tier 1: curated experimental spectral library
        ch_t1_pos_hits = Channel.of( tuple('pos', file('NO_FILE')) )
        ch_t1_neg_hits = Channel.of( tuple('neg', file('NO_FILE')) )

        if ( params.curated_library ) {
            ch_curated_lib = Channel.of( tuple('tier1_curated', file(params.curated_library)) )
            SEARCH_T1_POS( MSDIAL_POS.out.msp_library, ch_curated_lib )
            SEARCH_T1_NEG( MSDIAL_NEG.out.msp_library, ch_curated_lib )
            ch_t1_pos_hits = SEARCH_T1_POS.out.hits.map { pol, tier, f -> tuple(pol, f) }
            ch_t1_neg_hits = SEARCH_T1_NEG.out.hits.map { pol, tier, f -> tuple(pol, f) }
        }

        // Tier 2: larger experimental spectral library (e.g., HMDB experimental)
        ch_t2_pos_hits = Channel.of( tuple('pos', file('NO_FILE')) )
        ch_t2_neg_hits = Channel.of( tuple('neg', file('NO_FILE')) )

        if ( params.experimental_library ) {
            ch_exp_lib = Channel.of( tuple('tier2_experimental', file(params.experimental_library)) )
            SEARCH_T2_POS( MSDIAL_POS.out.msp_library, ch_exp_lib )
            SEARCH_T2_NEG( MSDIAL_NEG.out.msp_library, ch_exp_lib )
            ch_t2_pos_hits = SEARCH_T2_POS.out.hits.map { pol, tier, f -> tuple(pol, f) }
            ch_t2_neg_hits = SEARCH_T2_NEG.out.hits.map { pol, tier, f -> tuple(pol, f) }
        }

        // Tier 3: predicted spectral library (e.g., HMDB CFM-ID)
        ch_t3_pos_hits = Channel.of( tuple('pos', file('NO_FILE')) )
        ch_t3_neg_hits = Channel.of( tuple('neg', file('NO_FILE')) )

        if ( params.predicted_library ) {
            ch_pred_lib = Channel.of( tuple('tier3_predicted', file(params.predicted_library)) )
            SEARCH_T3_POS( MSDIAL_POS.out.msp_library, ch_pred_lib )
            SEARCH_T3_NEG( MSDIAL_NEG.out.msp_library, ch_pred_lib )
            ch_t3_pos_hits = SEARCH_T3_POS.out.hits.map { pol, tier, f -> tuple(pol, f) }
            ch_t3_neg_hits = SEARCH_T3_NEG.out.hits.map { pol, tier, f -> tuple(pol, f) }
        }

        // Tier 4: MIST-CF molecular formula prediction
        MIST_CF_POS( MSDIAL_POS.out.msp_library )
        MIST_CF_NEG( MSDIAL_NEG.out.msp_library )
        ch_t4_pos_hits = MIST_CF_POS.out.formulas
        ch_t4_neg_hits = MIST_CF_NEG.out.formulas

        // DiffMS de novo structure prediction (disabled — model not suitable
        // for blind inference on unknowns; kept for future use)
        if ( params.diffms_checkpoint ) {
            ch_diffms_pos = MSDIAL_POS.out.msp_library
                .join( MIST_CF_POS.out.formulas )
                .join( MIST_CF_POS.out.subforms )
            ch_diffms_neg = MSDIAL_NEG.out.msp_library
                .join( MIST_CF_NEG.out.formulas )
                .join( MIST_CF_NEG.out.subforms )
            ch_prep = ch_diffms_pos.mix( ch_diffms_neg )

            ch_prep.map { polarity, msp, formulas, subforms -> tuple(polarity, msp) }.set { ch_msp_for_prep }
            ch_prep.map { polarity, msp, formulas, subforms -> tuple(polarity, formulas) }.set { ch_formulas_for_prep }
            ch_prep.map { polarity, msp, formulas, subforms -> tuple(polarity, subforms) }.set { ch_subforms_for_prep }

            PREPARE_DIFFMS( ch_msp_for_prep, ch_formulas_for_prep, ch_subforms_for_prep )
            DIFFMS_PREDICT( PREPARE_DIFFMS.out.diffms_dir )

            BUILD_PSEUDOLIBRARY(
                MSDIAL_POS.out.msp_library.map { pol, f -> f }
                    .mix( MSDIAL_NEG.out.msp_library.map { pol, f -> f } )
                    .collect(),
                DIFFMS_PREDICT.out.predictions,
                DIFFMS_PREDICT.out.input_data
            )
        }

        // Merge tiered annotations per polarity
        MERGE_POS( ch_t1_pos_hits, ch_t2_pos_hits, ch_t3_pos_hits, ch_t4_pos_hits )
        MERGE_NEG( ch_t1_neg_hits, ch_t2_neg_hits, ch_t3_neg_hits, ch_t4_neg_hits )

        // Join annotations onto MS-DIAL feature table
        ANNOTATE_POS( MSDIAL_POS.out.align_result, MERGE_POS.out.merged )
        ANNOTATE_NEG( MSDIAL_NEG.out.align_result, MERGE_NEG.out.merged )

        // Annotate molecular network with tier annotations
        if ( params.run_msknit && params.msknit_graphml ) {
            ANNOTATE_NET_POS( MSKNIT_POS.out.network, MERGE_POS.out.merged )
            ANNOTATE_NET_NEG( MSKNIT_NEG.out.network, MERGE_NEG.out.merged )
        }
    }
}

workflow.onComplete {
    def manifest = """\
    Pipeline:     ${workflow.manifest.name} v${workflow.manifest.version}
    Git commit:   ${workflow.commitId ?: 'unknown'}
    Git branch:   ${workflow.revision ?: 'unknown'}
    Nextflow:     ${nextflow.version} build ${nextflow.build}
    Timestamp:    ${workflow.complete}
    Duration:     ${workflow.duration}
    Success:      ${workflow.success}
    Exit status:  ${workflow.exitStatus}
    Work dir:     ${workflow.workDir}
    Output dir:   ${params.outdir}
    Profile:      ${workflow.profile}
    Config files: ${workflow.configFiles.join(', ')}

    Parameters:
    ${params.collect { k, v -> "  ${k}: ${v}" }.join('\n')}
    """.stripIndent()

    def manifest_file = file("${params.outdir}/run_manifest.txt")
    manifest_file.text = manifest

    // Also record container image checksums if using apptainer
    if ( workflow.profile.contains('apptainer') ) {
        def sif_dir = file("${workflow.projectDir}/containers")
        def checksums = "Container image checksums (md5):\n"
        sif_dir.listFiles().findAll { it.name.endsWith('.sif') }.each { sif ->
            checksums += "  ${sif.name}: ${sif.size()} bytes\n"
        }
        manifest_file.append(checksums)
    }

    log.info manifest
}
