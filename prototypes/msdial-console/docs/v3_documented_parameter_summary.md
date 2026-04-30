# v3 Documentation Certainty Summary

This file summarizes which settings are clearly documented and matched in CLI v3, versus settings that still need investigation to rule out GUI/CLI differences.

## Counts

- Documented settings confirmed and explicitly set: **34**
- Documented settings likely matching via defaults/hardcoded behavior (not fully explicit): **10**
- Documented intentional differences from SOP/GUI defaults: **1**
- Documented settings that still need investigation: **13**
- Additional non-SOP settings/unknowns tracked for parity risk: **16**

## Confirmed Documented + Set (High confidence)

- `MS1 factor` (alignment)
- `MS1 tolerance` (alignment)
- `Peak count filter` (alignment)
- `Reference file` (alignment)
- `Retention time factor` (alignment)
- `Retention time tolerance` (alignment)
- `N% detected in at least one group` (alignment_filter)
- `MS/MS mass range begin` (data_correction)
- `MS/MS mass range end` (data_correction)
- `MS1 mass range begin` (data_correction)
- `MS1 mass range end` (data_correction)
- `MS1 tolerance` (data_correction)
- `MS2 tolerance` (data_correction)
- `Maximum charged number` (data_correction)
- `Retention time begin` (data_correction)
- `Retention time end` (data_correction)
- `MS/MS abundance cut off` (deconvolution)
- `Sigma window value` (deconvolution)
- `Accurate mass tolerance (MS1)` (identification)
- `Accurate mass tolerance (MS2)` (identification)
- `Identification score cut off` (identification)
- `Retention time tolerance` (identification)
- `Use retention time for filtering` (identification)
- `Use retention time for scoring` (identification)
- `Analytical order` (input)
- `Class ID assignment` (input)
- `Import all files` (input)
- `Inject volume` (input)
- `Sample type assignment` (input)
- `Mass slice width` (peak_detection)
- `Minimum peak height` (peak_detection)
- `MS1 data type` (setup)
- `MS2 data type` (setup)
- `Polarity` (setup)

## Documented But Not Fully Explicit (Investigate)

- `Blank filter method` -> default_only (Not configurable from current CLI text keys.)
- `Blank fold change` -> default_only (Not configurable from current CLI text keys.)
- `Gap filling by compulsion` -> default_only (Implicit match via default.)
- `Keep removable features and assign tag` -> default_only (Defaults match doc intent.)
- `Keep suggested (w/o MS2) metabolite features` -> default_only (No dedicated CLI parser key observed.)
- `Consider Cl and Br elements` -> unexposed (Behavior matches doc but cannot be toggled from config.)
- `Number of threads` -> explicit_config (Set explicitly to 14 in v3; doc guidance is n-1 so exact parity is machine-dependent.)
- `Acquisition mode` -> hardcoded_default (Using lcmsdda command path.)
- `Ionization` -> hardcoded_default (Not controlled by config for lcmsdda.)
- `Target omics` -> default_only (Default in project property.)

## Documented Intentional Differences

- `Remove features based on blank info`: CLI v3 uses `FALSE` / `FALSE`

## Documented Settings Still Requiring Clarification

- `Blank filter method` (documented_likely_match_via_default): Not configurable from current CLI text keys.
- `Blank fold change` (documented_likely_match_via_default): Not configurable from current CLI text keys.
- `Gap filling by compulsion` (documented_likely_match_via_default): Implicit match via default.
- `Keep reference matched metabolite features` (documented_but_unexposed_or_unknown): No separate CLI key matching GUI checkbox semantics.
- `Keep removable features and assign tag` (documented_likely_match_via_default): Defaults match doc intent.
- `Keep suggested (w/o MS2) metabolite features` (documented_likely_match_via_default): No dedicated CLI parser key observed.
- `Consider Cl and Br elements` (documented_likely_match_via_default): Behavior matches doc but cannot be toggled from config.
- `Execute retention time corrections` (documented_but_unexposed_or_unknown): Not explicitly controlled in current CLI config crosswalk.
- `Number of threads` (documented_and_set_machine_dependent): Set explicitly to 14 in v3; doc guidance is n-1 so exact parity is machine-dependent.
- `MSP file` (documented_but_unset_verify_gui): CLI supports MSP path; v3 configs do not specify one.
- `Acquisition mode` (documented_match_but_default_or_hardcoded): Using lcmsdda command path.
- `Ionization` (documented_match_but_default_or_hardcoded): Not controlled by config for lcmsdda.
- `Target omics` (documented_match_but_default_or_hardcoded): Default in project property.

## Non-SOP/Artifact Unknowns To Check

- `Alignment score cutoff`: Not exposed in LCMS parser
- `MS2 similarity factor for alignment`: Not exposed in LCMS parser
- `Original GUI project file (.mtd) available`: Need original GUI project/method export for full parity audit
- `QC at least filter`: CLI-specific gate that strongly affected recall in this dataset.
- `Exclude after precursor ion`: Explicit in CLI v3
- `Keep isotope until`: Explicit in CLI v3
- `Keep original precursor isotopes`: Explicit in CLI v3
- `Adduct list override`: Parser supports adduct list; GUI adduct subset could differ
- `Post-identification text library path`: Verify whether GUI loaded additional text library
- `Amplitude noise factor`: Not exposed in LCMS parser
- `Background subtraction`: Exists in AnalysisParametersBean; not exposed in LCMS parser
- `Minimum peak width`: Explicit in CLI v3; affects peak splitting/merging
- `Peak-top noise factor`: Not exposed in LCMS parser
- `Slope noise factor`: Not exposed in LCMS parser
- `Smoothing level`: Explicit in CLI v3; affects peak splitting/merging
- `Smoothing method`: Explicit in CLI v3; need original GUI project to verify

## Source Tables

- `v3_documented_parameter_certainty.csv`
- `docs_to_cli_parameter_mapping.csv`
- `v3_unknown_unexposed_settings.csv`