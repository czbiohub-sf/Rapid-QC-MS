#[derive(Debug, Clone, Copy)]
pub struct Peak {
    pub mz: f64,
    pub intensity: f64,
}

#[derive(Debug, Clone)]
pub struct Spectrum {
    pub index: usize,
    pub name: String,
    pub precursor_mz: f64,
    pub precursor_type: String,
    pub retention_time: f64,
    pub ion_mode: String,
    pub alignment_id: String,
    pub peaks: Vec<Peak>,
}

impl Spectrum {
    /// Sort peaks by m/z ascending.
    pub fn sort_peaks(&mut self) {
        self.peaks.sort_by(|a, b| a.mz.partial_cmp(&b.mz).unwrap());
    }

    /// Remove peaks below `frac` of the base peak intensity.
    pub fn filter_noise(&mut self, frac: f64) {
        let base = self
            .peaks
            .iter()
            .map(|p| p.intensity)
            .fold(0.0_f64, f64::max);
        if base <= 0.0 {
            return;
        }
        let threshold = base * frac;
        self.peaks.retain(|p| p.intensity >= threshold);
    }

    /// Apply square-root transform to intensities.
    pub fn sqrt_transform(&mut self) {
        for p in &mut self.peaks {
            p.intensity = p.intensity.abs().sqrt();
        }
    }
}
