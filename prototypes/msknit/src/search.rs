use crate::similarity::{modified_cosine, simple_cosine};
use crate::spectrum::Spectrum;
use rayon::prelude::*;
use serde::Serialize;
use std::path::Path;

#[derive(Debug, Clone, Serialize)]
pub struct SearchHit {
    pub query_index: usize,
    pub query_alignment_id: String,
    pub query_precursor_mz: f64,
    pub query_rt: f64,
    pub library_index: usize,
    pub library_name: String,
    pub library_precursor_mz: f64,
    pub library_formula: String,
    pub library_smiles: String,
    pub library_inchikey: String,
    pub cosine_score: f64,
    pub matched_peaks: usize,
    pub precursor_mz_diff: f64,
}

/// Search each query spectrum against a library, returning the top-K hits per query.
pub fn search_library(
    queries: &[Spectrum],
    library: &[Spectrum],
    tolerance: f64,
    min_cosine: f64,
    min_matched: usize,
    top_k: usize,
    use_modified: bool,
    precursor_tol: f64,
) -> Vec<SearchHit> {
    queries
        .par_iter()
        .flat_map(|query| {
            let mut hits: Vec<SearchHit> = Vec::new();

            for lib_spec in library {
                // Skip if precursor masses are too far apart
                let mz_diff = (query.precursor_mz - lib_spec.precursor_mz).abs();
                if precursor_tol > 0.0 && mz_diff > precursor_tol {
                    continue;
                }

                // Skip cross-polarity
                if !query.ion_mode.is_empty()
                    && !lib_spec.ion_mode.is_empty()
                    && query.ion_mode != lib_spec.ion_mode
                {
                    continue;
                }

                if query.peaks.len() < min_matched || lib_spec.peaks.len() < min_matched {
                    continue;
                }

                let result = if use_modified {
                    modified_cosine(query, lib_spec, tolerance)
                } else {
                    simple_cosine(query, lib_spec, tolerance)
                };

                if result.score >= min_cosine && result.matched_peaks >= min_matched {
                    hits.push(SearchHit {
                        query_index: query.index,
                        query_alignment_id: query.alignment_id.clone(),
                        query_precursor_mz: query.precursor_mz,
                        query_rt: query.retention_time,
                        library_index: lib_spec.index,
                        library_name: lib_spec.name.clone(),
                        library_precursor_mz: lib_spec.precursor_mz,
                        library_formula: lib_spec.get_meta("FORMULA"),
                        library_smiles: lib_spec.get_meta("SMILES"),
                        library_inchikey: lib_spec.get_meta("INCHIKEY"),
                        cosine_score: result.score,
                        matched_peaks: result.matched_peaks,
                        precursor_mz_diff: mz_diff,
                    });
                }
            }

            // Keep top-K per query
            hits.sort_by(|a, b| b.cosine_score.partial_cmp(&a.cosine_score).unwrap());
            hits.truncate(top_k);
            hits
        })
        .collect()
}

/// Write search hits to CSV.
pub fn write_hits_csv(hits: &[SearchHit], path: &Path) {
    let mut wtr = csv::Writer::from_path(path).expect("Failed to create hits CSV");
    for h in hits {
        wtr.serialize(h).expect("Failed to write hit");
    }
    wtr.flush().expect("Failed to flush hits CSV");
}
