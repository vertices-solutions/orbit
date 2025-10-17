use std::path::PathBuf;
use std::{fmt, num::ParseIntError, str::FromStr};

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct Hpcfile {
    project: ProjectConfig,
    slurm_config: Option<SlurmConfig>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ProjectConfig {
    // exclude patterns: what to exclude from transfers
    exclude_path: Vec<String>,
    // if true - display interactive progress bars
    interactive: bool,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SlurmConfig {
    // path to slurm sbatch script
    sbatch_script: PathBuf,
    // sbatch time
    time: String,
    cpus: u32,
    mem: Bytes,
}

#[derive(Debug, thiserror::Error)]
pub enum ParseBytesError {
    #[error("missing number")]
    Empty,
    #[error("invalid number: {0}")]
    BadNumber(#[from] ParseIntError),
    #[error("unknown unit: {0}")]
    UnknownUnit(String),
    #[error("value too large")]
    Overflow,
}

impl Default for Hpcfile {
    fn default() -> Self {
        Hpcfile {
            project: ProjectConfig {
                exclude_path: Vec::new(),
                interactive: true,
            },
            slurm_config: None,
        }
    }
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize,
)]
#[repr(transparent)]
pub struct Bytes(pub u64);

impl Bytes {
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    pub fn checked_add(self, rhs: Bytes) -> Option<Bytes> {
        self.0.checked_add(rhs.0).map(Bytes)
    }
    pub fn checked_mul(self, n: u64) -> Option<Bytes> {
        self.0.checked_mul(n).map(Bytes)
    }

    /// Pretty print using IEC binary units (KiB, MiB, GiB, …).
    pub fn to_iec(self) -> String {
        const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
        let mut v = self.0 as f64;
        let mut idx = 0;
        while v >= 1024.0 && idx < UNITS.len() - 1 {
            v /= 1024.0;
            idx += 1;
        }
        if idx == 0 {
            format!("{} {}", self.0, UNITS[idx])
        } else {
            format!("{:.2} {}", v, UNITS[idx])
        }
    }
}

impl From<u64> for Bytes {
    fn from(b: u64) -> Self {
        Bytes(b)
    }
}

impl fmt::Display for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Default to IEC, which is the norm for RAM.
        f.write_str(&self.to_iec())
    }
}

impl FromStr for Bytes {
    type Err = ParseBytesError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.is_empty() {
            return Err(ParseBytesError::Empty);
        }

        // split numeric prefix and unit suffix
        let cut = s
            .find(|c: char| !c.is_ascii_digit() && c != '_')
            .unwrap_or(s.len()); // last digit
        let (num, unit_raw) = s.split_at(cut);

        let n: u128 = num.replace('_', "").parse()?;
        let u = unit_raw.trim().to_ascii_lowercase();

        // Conventions:
        // "", "b" => bytes
        // k, m, g,etc. => binary (KiB/MiB/GiB...); this goes against convention, but that's how
        // Slurm does that
        let multiplier: u128 = match u.as_str() {
            "" | "b" => 1,
            "k" | "kb" => 1024,
            "m" | "mb" => 1024u128.pow(2),
            "g" | "gb" => 1024u128.pow(3),
            "t" | "tb" => 1024u128.pow(4),
            "p" | "pb" => 1024u128.pow(5),
            _ => return Err(ParseBytesError::UnknownUnit(unit_raw.trim().to_string())),
        };

        let bytes = n.checked_mul(multiplier).ok_or(ParseBytesError::Overflow)?;
        let bytes = u64::try_from(bytes).map_err(|_| ParseBytesError::Overflow)?;
        Ok(Bytes(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_parse_gb() {
        let a = Bytes::from_str("8G").unwrap();
        assert_eq!(a.as_u64(), 8_u64 * 1024 * 1024 * 1024);
        let b = Bytes::from_str("512GB").unwrap();
        assert_eq!(b.as_u64(), 512_u64 * 1024 * 1024 * 1024);
    }

    #[test]
    fn underscores_and_whitespace() {
        // Underscores in the number
        let b = Bytes::from_str("1_024KB").unwrap(); // 1024 * 1024
        assert_eq!(b.as_u64(), 1_048_576);

        // Trimming both sides
        let c = Bytes::from_str("   32 GB  ").unwrap();
        assert_eq!(c.as_u64(), 32_u64 * 1024 * 1024 * 1024);
    }
    #[test]
    fn overflow_detection() {
        // u64::MAX + 1 bytes -> conversion overflow
        let too_big = (u64::MAX as u128) + 1;
        let s = too_big.to_string();
        match Bytes::from_str(&s) {
            Err(ParseBytesError::Overflow) => {}
            other => panic!("expected Overflow, got {:?}", other),
        }

        // also overflow in multiplication path
        match Bytes::from_str("18446744073709551615KB") {
            // (u64::MAX)*1024
            Err(ParseBytesError::Overflow) => {}
            other => panic!("expected Overflow, got {:?}", other),
        }
    }
}
