use crate::spectrum::{Peak, Spectrum};
use std::fs;
use std::path::Path;

/// Parse an MSP file into a vector of Spectrum.
pub fn parse_msp(path: &Path, start_index: usize) -> Vec<Spectrum> {
    let content = fs::read_to_string(path).expect("Failed to read MSP file");
    parse_msp_str(&content, start_index)
}

fn parse_msp_str(content: &str, start_index: usize) -> Vec<Spectrum> {
    let mut spectra = Vec::new();
    let mut name = String::new();
    let mut precursor_mz = 0.0;
    let mut precursor_type = String::new();
    let mut retention_time = 0.0;
    let mut ion_mode = String::new();
    let mut alignment_id = String::new();
    let mut num_peaks: Option<usize> = None;
    let mut peaks: Vec<Peak> = Vec::new();
    let mut reading_peaks = false;

    for line in content.lines() {
        let trimmed = line.trim();

        if trimmed.is_empty() {
            // End of entry — save if we have peaks
            if !peaks.is_empty() || num_peaks.is_some() {
                let mut spec = Spectrum {
                    index: start_index + spectra.len(),
                    name: std::mem::take(&mut name),
                    precursor_mz,
                    precursor_type: std::mem::take(&mut precursor_type),
                    retention_time,
                    ion_mode: std::mem::take(&mut ion_mode),
                    alignment_id: std::mem::take(&mut alignment_id),
                    peaks: std::mem::take(&mut peaks),
                };
                spec.sort_peaks();
                spectra.push(spec);
            }
            precursor_mz = 0.0;
            retention_time = 0.0;
            num_peaks = None;
            reading_peaks = false;
            continue;
        }

        // Try to parse as a peak line (starts with digit or negative sign)
        if reading_peaks {
            if let Some(peak) = try_parse_peak(trimmed) {
                peaks.push(peak);
                continue;
            }
        }

        // Key: Value header line
        if let Some(colon_pos) = trimmed.find(':') {
            let key = trimmed[..colon_pos].trim().to_uppercase();
            let val = trimmed[colon_pos + 1..].trim();

            match key.as_str() {
                "NAME" => name = val.to_string(),
                "PRECURSORMZ" | "PRECURSOR M/Z" | "MW" => {
                    precursor_mz = val.parse().unwrap_or(0.0);
                }
                "PRECURSORTYPE" | "ADDUCT" => precursor_type = val.to_string(),
                "RETENTIONTIME" | "RT" => retention_time = val.parse().unwrap_or(0.0),
                "IONMODE" => ion_mode = val.to_string(),
                "ALIGNMENTID" => alignment_id = val.to_string(),
                "NUM PEAKS" | "NUMPEAKS" => {
                    num_peaks = val.parse().ok();
                    reading_peaks = true;
                }
                _ => {}
            }
        }
    }

    // Handle last entry (no trailing blank line)
    if !peaks.is_empty() || num_peaks.is_some() {
        let mut spec = Spectrum {
            index: start_index + spectra.len(),
            name,
            precursor_mz,
            precursor_type,
            retention_time,
            ion_mode,
            alignment_id,
            peaks,
        };
        spec.sort_peaks();
        spectra.push(spec);
    }

    spectra
}

fn try_parse_peak(line: &str) -> Option<Peak> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() >= 2 {
        let mz: f64 = parts[0].parse().ok()?;
        let intensity: f64 = parts[1].parse().ok()?;
        Some(Peak { mz, intensity })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_msp() {
        let msp = "\
NAME: Compound A
PRECURSORMZ: 302.11472
PRECURSORTYPE: [M+H]+
RETENTIONTIME: 5.123
IONMODE: Positive
ALIGNMENTID: 42
Num Peaks: 3
85.02841\t1200
128.06204\t8900
302.11400\t500

NAME: Compound B
PRECURSORMZ: 456.20145
PRECURSORTYPE: [M+H]+
RETENTIONTIME: 8.5
IONMODE: Positive
ALIGNMENTID: 99
Num Peaks: 2
110.07132\t3500
456.20000\t1000
";
        let spectra = parse_msp_str(msp, 0);
        assert_eq!(spectra.len(), 2);
        assert_eq!(spectra[0].name, "Compound A");
        assert_eq!(spectra[0].peaks.len(), 3);
        assert!((spectra[0].precursor_mz - 302.11472).abs() < 1e-5);
        assert_eq!(spectra[1].alignment_id, "99");
        assert_eq!(spectra[1].peaks.len(), 2);
    }
}
