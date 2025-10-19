use std::collections::HashSet;
use std::path::PathBuf;
use std::{fmt, num::ParseIntError, str::FromStr};

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct Hpcfile {
    pub exclude_path: Vec<String>,
    pub interactive: bool,
    pub batch_script: Option<PathBuf>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ProjectConfig {
    // exclude patterns: what to exclude from transfers
    exclude_path: Vec<String>,
    // if true - display interactive progress bars
    interactive: bool,
}

impl Default for Hpcfile {
    fn default() -> Self {
        Hpcfile {
            exclude_path: Vec::new(),
            interactive: true,
            batch_script: None,
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SlurmConfig {
    // path to slurm sbatch script
    // sbatch time
    time: Option<String>,
    account: Option<String>,
    cpus: Option<u32>,
    mem: Option<Bytes>,
    script: String,
}

impl SlurmConfig {
    pub fn to_sbatch_script(&self) -> String {
        let mut res = String::new();
        res.push_str("#!/bin/bash\n");
        if let Some(ref time) = self.time {
            res.push_str(&format!("#SBATCH --time={}\n", time));
        }
        if let Some(ref account) = self.account {
            res.push_str(&format!("#SBATCH --account={}\n", account));
        }
        if let Some(ref cpus) = self.cpus {
            res.push_str(&format!("#SBATCH --cpus={}\n", cpus));
        }
        if let Some(ref mem) = self.mem {
            res.push_str(&format!("#SBATCH --mem={}\n", mem));
        }
        res.push_str(&self.script);
        res.push_str("\n");
        res
    }

    pub fn new(
        time: Option<String>,
        account: Option<String>,
        cpus: Option<u32>,
        mem: Option<Bytes>,
        script: String,
    ) -> Self {
        return SlurmConfig {
            time: time,
            account: account,
            cpus: cpus,
            mem: mem,
            script: script,
        };
    }
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
#[derive(Debug, thiserror::Error)]
pub enum SerializeBytesError {
    #[error("number less than 1K")]
    LessThen1K,
    #[error("suffix not allowed")]
    SuffixNotAllowed,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
pub struct Bytes(pub u64, pub String); // amount of bytes + unit

impl fmt::Display for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Default to slurm-friendly formatting
        f.write_str(&format!("{}{}", self.0, self.1))
    }
}

impl FromStr for Bytes {
    type Err = ParseBytesError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ALLOWED_UNITS: HashSet<&str> = HashSet::from(["K", "M", "G", "T"]);
        let s = s.trim();
        if s.is_empty() {
            return Err(ParseBytesError::Empty);
        }

        // split numeric prefix and unit suffix
        let cut = s
            .find(|c: char| !c.is_ascii_digit() && c != '_')
            .unwrap_or(s.len()); // last digit
        let (num, unit_raw) = s.split_at(cut);

        let n: u64 = match num.replace('_', "").parse() {
            Ok(v) => v,
            Err(e) if *e.kind() == std::num::IntErrorKind::PosOverflow => {
                return Err(ParseBytesError::Overflow);
            }
            Err(e) => return Err(ParseBytesError::BadNumber(e)),
        };
        let u = unit_raw.trim();

        if !ALLOWED_UNITS.contains(u) {
            return Err(ParseBytesError::UnknownUnit(u.to_string()));
        }

        Ok(Bytes(n, u.to_string()))
    }
}

#[cfg(test)]
mod bytes_tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_parse_gb() {
        let a = Bytes::from_str("8G").unwrap();
        assert_eq!(a.0, 8);
        assert_eq!(a.1, "G");
    }

    #[test]
    fn underscores_and_whitespace() {
        // Underscores in the number
        let b = Bytes::from_str("1_024K").unwrap(); // 1024 * 1024
        assert_eq!(b.0, 1024);

        // Trimming both sides
        let c = Bytes::from_str("   32 G  ").unwrap();
        assert_eq!(c.0, 32);
    }
    #[test]
    fn overflow_detection() {
        // u64::MAX + 1 bytes -> conversion overflow
        let too_big = (u64::MAX as u128) + 1;
        let mut s = too_big.to_string();
        s.push_str("G");
        match Bytes::from_str(&s) {
            Err(ParseBytesError::Overflow) => {}
            other => panic!("expected Overflow, got {:?}", other),
        }
    }
    // TODO: test serialization
}

#[cfg(test)]
mod slurm_config_tests {
    use super::*;

    #[test]
    fn test_serialize() {
        let sc = SlurmConfig {
            time: Some("3:00:00".to_string()),
            account: Some("mplanck-def".to_string()),
            cpus: Some(20),
            mem: Some("8G".parse().unwrap()),
            script: "python3 main.py".to_string(),
        };

        let sc_str = sc.to_sbatch_script();
        let expected_str = r#"#!/bin/bash
#SBATCH --time=3:00:00
#SBATCH --account=mplanck-def
#SBATCH --cpus=20
#SBATCH --mem=8G
python3 main.py
"#;
        assert_eq!(sc_str, expected_str);
    }
}
