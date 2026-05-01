use crate::spectrum::Spectrum;

/// Result of a pairwise spectral comparison.
pub struct SimilarityResult {
    pub score: f64,
    pub matched_peaks: usize,
}

/// Modified cosine similarity between two spectra.
///
/// Aligns peaks considering both direct m/z matches and matches shifted
/// by the precursor mass difference. Uses greedy assignment by intensity
/// product to resolve ambiguous matches.
pub fn modified_cosine(a: &Spectrum, b: &Spectrum, tolerance: f64) -> SimilarityResult {
    if a.peaks.is_empty() || b.peaks.is_empty() {
        return SimilarityResult {
            score: 0.0,
            matched_peaks: 0,
        };
    }

    let delta = a.precursor_mz - b.precursor_mz;

    // Collect all candidate matches: (index_a, index_b, product_score)
    let mut candidates: Vec<(usize, usize, f64)> = Vec::new();

    for (i, pa) in a.peaks.iter().enumerate() {
        for (j, pb) in b.peaks.iter().enumerate() {
            // Unshifted match
            if (pa.mz - pb.mz).abs() <= tolerance {
                candidates.push((i, j, pa.intensity * pb.intensity));
            }
            // Shifted match (fragment retains precursor mass difference)
            if (pa.mz - (pb.mz + delta)).abs() <= tolerance {
                candidates.push((i, j, pa.intensity * pb.intensity));
            }
        }
    }

    // Greedy assignment: highest product score first
    candidates.sort_by(|x, y| y.2.partial_cmp(&x.2).unwrap());

    let mut used_a = vec![false; a.peaks.len()];
    let mut used_b = vec![false; b.peaks.len()];
    let mut matched_a = Vec::new();
    let mut matched_b = Vec::new();

    for (ia, ib, _) in &candidates {
        if !used_a[*ia] && !used_b[*ib] {
            used_a[*ia] = true;
            used_b[*ib] = true;
            matched_a.push(a.peaks[*ia].intensity);
            matched_b.push(b.peaks[*ib].intensity);
        }
    }

    let matched_peaks = matched_a.len();
    if matched_peaks == 0 {
        return SimilarityResult {
            score: 0.0,
            matched_peaks: 0,
        };
    }

    let dot: f64 = matched_a.iter().zip(&matched_b).map(|(x, y)| x * y).sum();
    let norm_a: f64 = matched_a.iter().map(|x| x * x).sum::<f64>().sqrt();
    let norm_b: f64 = matched_b.iter().map(|x| x * x).sum::<f64>().sqrt();

    let score = if norm_a > 0.0 && norm_b > 0.0 {
        dot / (norm_a * norm_b)
    } else {
        0.0
    };

    SimilarityResult {
        score,
        matched_peaks,
    }
}

/// Simple cosine similarity (no precursor mass shift).
pub fn simple_cosine(a: &Spectrum, b: &Spectrum, tolerance: f64) -> SimilarityResult {
    if a.peaks.is_empty() || b.peaks.is_empty() {
        return SimilarityResult {
            score: 0.0,
            matched_peaks: 0,
        };
    }

    let mut candidates: Vec<(usize, usize, f64)> = Vec::new();

    for (i, pa) in a.peaks.iter().enumerate() {
        for (j, pb) in b.peaks.iter().enumerate() {
            if (pa.mz - pb.mz).abs() <= tolerance {
                candidates.push((i, j, pa.intensity * pb.intensity));
            }
        }
    }

    candidates.sort_by(|x, y| y.2.partial_cmp(&x.2).unwrap());

    let mut used_a = vec![false; a.peaks.len()];
    let mut used_b = vec![false; b.peaks.len()];
    let mut matched_a = Vec::new();
    let mut matched_b = Vec::new();

    for (ia, ib, _) in &candidates {
        if !used_a[*ia] && !used_b[*ib] {
            used_a[*ia] = true;
            used_b[*ib] = true;
            matched_a.push(a.peaks[*ia].intensity);
            matched_b.push(b.peaks[*ib].intensity);
        }
    }

    let matched_peaks = matched_a.len();
    if matched_peaks == 0 {
        return SimilarityResult {
            score: 0.0,
            matched_peaks: 0,
        };
    }

    let dot: f64 = matched_a.iter().zip(&matched_b).map(|(x, y)| x * y).sum();
    let norm_a: f64 = matched_a.iter().map(|x| x * x).sum::<f64>().sqrt();
    let norm_b: f64 = matched_b.iter().map(|x| x * x).sum::<f64>().sqrt();

    let score = if norm_a > 0.0 && norm_b > 0.0 {
        dot / (norm_a * norm_b)
    } else {
        0.0
    };

    SimilarityResult {
        score,
        matched_peaks,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spectrum::Peak;

    fn make_spectrum(index: usize, precursor_mz: f64, peaks: &[(f64, f64)]) -> Spectrum {
        Spectrum {
            index,
            name: String::new(),
            precursor_mz,
            precursor_type: String::new(),
            retention_time: 0.0,
            ion_mode: "Positive".to_string(),
            alignment_id: String::new(),
            peaks: peaks
                .iter()
                .map(|&(mz, intensity)| Peak { mz, intensity })
                .collect(),
        }
    }

    #[test]
    fn test_identical_spectra() {
        let a = make_spectrum(0, 300.0, &[(100.0, 1000.0), (150.0, 500.0), (200.0, 800.0)]);
        let b = make_spectrum(1, 300.0, &[(100.0, 1000.0), (150.0, 500.0), (200.0, 800.0)]);
        let result = modified_cosine(&a, &b, 0.02);
        assert!((result.score - 1.0).abs() < 1e-6);
        assert_eq!(result.matched_peaks, 3);
    }

    #[test]
    fn test_no_overlap() {
        let a = make_spectrum(0, 300.0, &[(100.0, 1000.0), (150.0, 500.0)]);
        let b = make_spectrum(1, 300.0, &[(200.0, 1000.0), (250.0, 500.0)]);
        let result = modified_cosine(&a, &b, 0.02);
        assert_eq!(result.score, 0.0);
        assert_eq!(result.matched_peaks, 0);
    }

    #[test]
    fn test_shifted_match() {
        // Spectra differ by 14 Da (CH2 unit). Fragments also shifted by 14.
        let a = make_spectrum(0, 300.0, &[(100.0, 1000.0), (200.0, 500.0)]);
        let b = make_spectrum(1, 314.0, &[(114.0, 1000.0), (214.0, 500.0)]);
        let result = modified_cosine(&a, &b, 0.02);
        // Shifted alignment: a[100] matches b[114] with delta=-14, and a[200] matches b[214]
        assert!(result.score > 0.99);
        assert_eq!(result.matched_peaks, 2);
    }

    #[test]
    fn test_simple_cosine_no_shift() {
        let a = make_spectrum(0, 300.0, &[(100.0, 1000.0), (200.0, 500.0)]);
        let b = make_spectrum(1, 314.0, &[(114.0, 1000.0), (214.0, 500.0)]);
        let result = simple_cosine(&a, &b, 0.02);
        // No shift allowed, peaks don't match
        assert_eq!(result.matched_peaks, 0);
    }
}
