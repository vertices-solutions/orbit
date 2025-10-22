use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

use crate::state::db::{ParseSlurmVersionError, SlurmVersion};

pub const DETERMINE_SLURM_VERSION_CMD: &str = r#"(scontrol --version 2>/dev/null || srun --version 2>/dev/null || sinfo --version 2>/dev/null || squeue --version 2>/dev/null; )"#;

// SLURM PARTITION INFO GATHERING

/// Represents a single SLURM partition as parsed from `scontrol show partition -o`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partition {
    /// The `PartitionName` value.
    pub name: String,
    /// All key=value pairs for the line (excluding `PartitionName`, which is in `name`).
    pub fields: HashMap<String, String>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ParseError {
    #[error("partition is missing required key: {0}")]
    MissingField(&'static str),
}

/// Parse the output of `scontrol show partition -o` into a vector of `Partition`s.
///
/// Each non-empty line is expected to be a series of `key=value` tokens separated by whitespace.
/// Only the first '=' in a token is considered the key/value delimiter to handle values like
/// `TRESBillingWeights=CPU=1.0,Mem=0`.
pub fn parse_scontrol_partitions(input: &str) -> Result<Vec<Partition>, ParseError> {
    let mut parts = Vec::new();

    for line in input.lines().map(str::trim).filter(|l| !l.is_empty()) {
        let mut map = HashMap::<String, String>::new();

        for tok in line.split_whitespace() {
            if let Some(eq) = tok.find('=') {
                let (k, v_with_eq) = tok.split_at(eq);
                let v = &v_with_eq[1..]; // skip '='
                map.insert(k.to_string(), v.to_string());
            }
        }

        let name = map
            .remove("PartitionName")
            .ok_or(ParseError::MissingField("PartitionName"))?;

        parts.push(Partition { name, fields: map });
    }

    Ok(parts)
}

impl Partition {
    /// Get a raw field value as &str.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.fields.get(key).map(|s| s.as_str())
    }

    /// Parse a field that behaves like a boolean: YES/NO, TRUE/FALSE, ON/OFF, 1/0.
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        let s = self.get(key)?.trim().to_ascii_lowercase();
        match s.as_str() {
            "yes" | "true" | "on" | "1" => Some(true),
            "no" | "false" | "off" | "0" => Some(false),
            _ => None,
        }
    }

    /// Parse a field as u64, returning None for non-numeric values (e.g., UNLIMITED).
    pub fn get_u64(&self, key: &str) -> Option<u64> {
        let s = self.get(key)?.trim();
        if s.chars().all(|c| c.is_ascii_digit()) {
            s.parse::<u64>().ok()
        } else {
            None
        }
    }

    /// Parse a SLURM-style duration (e.g., "21-00:00:00" or "03:00:00") into `Duration`.
    /// Returns None for markers like "UNLIMITED", "NONE", or "N/A".
    pub fn get_duration(&self, key: &str) -> Option<Duration> {
        parse_slurm_duration(self.get(key)?)
    }
}

/// Parse SLURM durations like "D-HH:MM:SS" or "HH:MM:SS".
fn parse_slurm_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.eq_ignore_ascii_case("unlimited")
        || s.eq_ignore_ascii_case("none")
        || s.eq_ignore_ascii_case("n/a")
    {
        return None;
    }

    // Split optional days: "D-HH:MM:SS" or just "HH:MM:SS"
    let (days, hms) = if let Some(dash) = s.find('-') {
        let (d, rest) = s.split_at(dash);
        let d: u64 = d.parse().ok()?;
        (d, &rest[1..])
    } else {
        (0, s)
    };

    let mut it = hms.split(':');
    let (h, m, sec) = (it.next()?, it.next()?, it.next()?);
    if it.next().is_some() {
        return None; // too many components
    }

    let (h, m, sec): (u64, u64, u64) = (h.parse().ok()?, m.parse().ok()?, sec.parse().ok()?);
    let total = days
        .saturating_mul(24 * 3600)
        .saturating_add(h * 3600)
        .saturating_add(m * 60)
        .saturating_add(sec);

    Some(Duration::from_secs(total))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"PartitionName=skylake AllowGroups=ALL AllowAccounts=ALL AllowQos=normal,high AllocNodes=aurochs,bison,yak,ood,ood-test,cryosparc-01,g32[1-5],g338,g383,n[326-337],n[339-381],n[388-422],g[384-387] Default=YES QoS=N/A DefaultTime=03:00:00 DisableRootJobs=NO ExclusiveUser=NO ExclusiveTopo=NO GraceTime=0 Hidden=NO MaxNodes=UNLIMITED MaxTime=21-00:00:00 MinNodes=0 LLN=NO MaxCPUsPerNode=UNLIMITED MaxCPUsPerSocket=UNLIMITED Nodes=n[339-381] PriorityJobFactor=0 PriorityTier=1 RootOnly=NO ReqResv=NO OverSubscribe=NO OverTimeLimit=NONE PreemptMode=OFF State=UP TotalCPUs=2236 TotalNodes=43 SelectTypeParameters=NONE JobDefaults=(null) DefMemPerCPU=2500 MaxMemPerNode=UNLIMITED TRES=cpu=2236,mem=7998000M,node=43,billing=2236 TRESBillingWeights=CPU=1.0,Mem=0
PartitionName=chrim AllowGroups=ALL AllowAccounts=def-rleduc42,def-zovoilis-ab AllowQos=normal,high AllocNodes=aurochs,bison,yak,ood,ood-test,cryosparc-01,g32[1-5],g338,g383,n[326-337],n[339-381],n[388-422],g[384-387] Default=NO QoS=normal DefaultTime=03:00:00 DisableRootJobs=NO ExclusiveUser=NO ExclusiveTopo=NO GraceTime=0 Hidden=NO MaxNodes=UNLIMITED MaxTime=21-00:00:00 MinNodes=0 LLN=NO MaxCPUsPerNode=UNLIMITED MaxCPUsPerSocket=UNLIMITED Nodes=n[424-427] PriorityJobFactor=0 PriorityTier=9 RootOnly=NO ReqResv=NO OverSubscribe=NO OverTimeLimit=NONE PreemptMode=OFF State=UP TotalCPUs=768 TotalNodes=4 SelectTypeParameters=NONE JobDefaults=(null) DefMemPerCPU=4000 MaxMemPerNode=UNLIMITED TRES=cpu=768,mem=3000000M,node=4,billing=768 TRESBillingWeights=CPU=1.0,Mem=0
PartitionName=chrimlm AllowGroups=ALL AllowAccounts=def-rleduc42,def-zovoilis-ab AllowQos=normal,high AllocNodes=aurochs,bison,yak,ood,ood-test,cryosparc-01,g32[1-5],g338,g383,n[326-337],n[339-381],n[388-422],g[384-387] Default=NO QoS=normal DefaultTime=03:00:00 DisableRootJobs=NO ExclusiveUser=NO ExclusiveTopo=NO GraceTime=0 Hidden=NO MaxNodes=UNLIMITED MaxTime=21-00:00:00 MinNodes=0 LLN=NO MaxCPUsPerNode=UNLIMITED MaxCPUsPerSocket=UNLIMITED Nodes=n423 PriorityJobFactor=0 PriorityTier=9 RootOnly=NO ReqResv=NO OverSubscribe=NO OverTimeLimit=NONE PreemptMode=OFF State=UP TotalCPUs=192 TotalNodes=1 SelectTypeParameters=NONE JobDefaults=(null) DefMemPerCPU=7000 MaxMemPerNode=UNLIMITED TRES=cpu=192,mem=1500000M,node=1,billing=384 TRESBillingWeights=CPU=2.0,Mem=0
PartitionName=genlm AllowGroups=ALL AllowAccounts=ALL AllowQos=normal,high AllocNodes=aurochs,bison,yak,ood,ood-test,cryosparc-01,g32[1-5],g338,g383,n[326-337],n[339-381],n[388-422],g[384-387] Default=NO QoS=normal DefaultTime=03:00:00 DisableRootJobs=NO ExclusiveUser=NO ExclusiveTopo=NO GraceTime=0 Hidden=NO MaxNodes=UNLIMITED MaxTime=21-00:00:00 MinNodes=0 LLN=NO MaxCPUsPerNode=UNLIMITED MaxCPUsPerSocket=UNLIMITED Nodes=n[420-422] PriorityJobFactor=0 PriorityTier=1 RootOnly=NO ReqResv=NO OverSubscribe=NO OverTimeLimit=NONE PreemptMode=OFF State=UP TotalCPUs=576 TotalNodes=3 SelectTypeParameters=NONE JobDefaults=(null) DefMemPerCPU=8000 MaxMemPerNode=UNLIMITED TRES=cpu=576,mem=4500000M,node=3,billing=1152 TRESBillingWeights=CPU=2.0,Mem=0
PartitionName=genoa AllowGroups=ALL AllowAccounts=ALL AllowQos=normal,high AllocNodes=aurochs,bison,yak,ood,ood-test,cryosparc-01,g32[1-5],g338,g383,n[326-337],n[339-381],n[388-422],g[384-387] Default=NO QoS=normal DefaultTime=03:00:00 DisableRootJobs=NO ExclusiveUser=NO ExclusiveTopo=NO GraceTime=0 Hidden=NO MaxNodes=UNLIMITED MaxTime=21-00:00:00 MinNodes=0 LLN=NO MaxCPUsPerNode=UNLIMITED MaxCPUsPerSocket=UNLIMITED Nodes=n[393-419] PriorityJobFactor=0 PriorityTier=1 RootOnly=NO ReqResv=NO OverSubscribe=NO OverTimeLimit=NONE PreemptMode=OFF State=UP TotalCPUs=5184 TotalNodes=27 SelectTypeParameters=NONE JobDefaults=(null) DefMemPerCPU=4000 MaxMemPerNode=UNLIMITED TRES=cpu=5184,mem=20250000M,node=27,billing=5184 TRESBillingWeights=CPU=1.0,Mem=0
PartitionName=genoacpu-b AllowGroups=ALL AllowAccounts=ALL AllowQos=normal,cpupreempt AllocNodes=aurochs,bison,yak,ood,ood-test,cryosparc-01,g32[1-5],g338,g38[3-7],n[326-337],n[339-381],n3[88-92] Default=NO QoS=normal DefaultTime=03:00:00 DisableRootJobs=NO ExclusiveUser=NO ExclusiveTopo=NO GraceTime=100 Hidden=NO MaxNodes=UNLIMITED MaxTime=7-00:00:00 MinNodes=0 LLN=NO MaxCPUsPerNode=UNLIMITED MaxCPUsPerSocket=UNLIMITED Nodes=n[388-392,423-427] PriorityJobFactor=0 PriorityTier=2 RootOnly=NO ReqResv=NO OverSubscribe=NO OverTimeLimit=NONE PreemptMode=CANCEL State=UP TotalCPUs=1800 TotalNodes=10 SelectTypeParameters=NONE JobDefaults=(null) DefMemPerCPU=4000 MaxMemPerNode=UNLIMITED TRES=cpu=1800,mem=12000000M,node=10,billing=1800 TRESBillingWeights=CPU=1.0,Mem=0
PartitionName=largemem AllowGroups=ALL AllowAccounts=ALL AllowQos=normal,high AllocNodes=aurochs,bison,yak,ood,ood-test,cryosparc-01,g32[1-5],g338,g383,n[326-337],n[339-381],n[388-422],g[384-387] Default=NO QoS=N/A DefaultTime=03:00:00 DisableRootJobs=NO ExclusiveUser=NO ExclusiveTopo=NO GraceTime=0 Hidden=NO MaxNodes=UNLIMITED MaxTime=21-00:00:00 MinNodes=0 LLN=NO MaxCPUsPerNode=UNLIMITED MaxCPUsPerSocket=UNLIMITED Nodes=n[326-337] PriorityJobFactor=0 PriorityTier=1 RootOnly=NO ReqResv=NO OverSubscribe=NO OverTimeLimit=NONE PreemptMode=OFF State=UP TotalCPUs=480 TotalNodes=12 SelectTypeParameters=NONE JobDefaults=(null) DefMemPerCPU=7500 MaxMemPerNode=UNLIMITED TRES=cpu=480,mem=4578000M,node=12,billing=480 TRESBillingWeights=CPU=1.0,Mem=0
PartitionName=mcordcpu AllowGroups=ALL AllowAccounts=def-cordeiro,def-gshamov,def-kerrache AllowQos=normal AllocNodes=aurochs,bison,yak,ood,ood-test,cryosparc-01,g32[1-5],g338,g38[3-7],n[326-337],n[339-381],n3[88-92] Default=NO QoS=normal DefaultTime=03:00:00 DisableRootJobs=NO ExclusiveUser=NO ExclusiveTopo=NO GraceTime=0 Hidden=NO MaxNodes=UNLIMITED MaxTime=21-00:00:00 MinNodes=0 LLN=NO MaxCPUsPerNode=UNLIMITED MaxCPUsPerSocket=UNLIMITED Nodes=n[388-392] PriorityJobFactor=0 PriorityTier=9 RootOnly=NO ReqResv=NO OverSubscribe=NO OverTimeLimit=NONE PreemptMode=OFF State=UP TotalCPUs=840 TotalNodes=5 SelectTypeParameters=NONE JobDefaults=(null) DefMemPerCPU=8000 MaxMemPerNode=UNLIMITED TRES=cpu=840,mem=7500000M,node=5,billing=840 TRESBillingWeights=CPU=1.0,Mem=0
PartitionName=agro AllowGroups=ALL AllowAccounts=def-nasem,def-mccartca,def-cordeiro,def-jfrank,def-gshamov,def-kerrache AllowQos=normal AllocNodes=aurochs,bison,yak,ood,ood-test,cryosparc-01,g32[1-5],n[326-337],n[339-381],g383 Default=NO QoS=N/A DefaultTime=03:00:00 DisableRootJobs=NO ExclusiveUser=NO ExclusiveTopo=NO GraceTime=0 Hidden=NO MaxNodes=UNLIMITED MaxTime=21-00:00:00 MinNodes=0 LLN=NO MaxCPUsPerNode=UNLIMITED MaxCPUsPerSocket=UNLIMITED Nodes=g[384-385] PriorityJobFactor=0 PriorityTier=10 RootOnly=NO ReqResv=NO OverSubscribe=NO OverTimeLimit=NONE PreemptMode=OFF State=UP TotalCPUs=48 TotalNodes=2 SelectTypeParameters=NONE JobDefaults=(null) DefMemPerCPU=9000 MaxMemPerNode=UNLIMITED TRES=cpu=48,mem=496000M,node=2,billing=48,gres/gpu=4 TRESBillingWeights=CPU=1.0,Mem=0
PartitionName=agro-b AllowGroups=ALL AllowAccounts=ALL AllowQos=normal,gpupreempt AllocNodes=aurochs,bison,yak,ood,ood-test,cryosparc-01,g32[1-5],g383,n[326-337],n[339-381] Default=NO QoS=N/A DefaultTime=03:00:00 DisableRootJobs=NO ExclusiveUser=NO ExclusiveTopo=NO GraceTime=100 Hidden=NO MaxNodes=UNLIMITED MaxTime=7-00:00:00 MinNodes=0 LLN=NO MaxCPUsPerNode=UNLIMITED MaxCPUsPerSocket=UNLIMITED Nodes=g[384-385] PriorityJobFactor=0 PriorityTier=1 RootOnly=NO ReqResv=NO OverSubscribe=NO OverTimeLimit=NONE PreemptMode=CANCEL State=UP TotalCPUs=48 TotalNodes=2 SelectTypeParameters=NONE JobDefaults=(null) DefMemPerCPU=9000 MaxMemPerNode=UNLIMITED TRES=cpu=48,mem=496000M,node=2,billing=72,gres/gpu=4 TRESBillingWeights=CPU=1.0,Mem=0,GRES/gpu=6.0
PartitionName=gpu AllowGroups=ALL AllowAccounts=ALL AllowQos=normal,high AllocNodes=aurochs,bison,yak,ood,ood-test,cryosparc-01,g32[1-5],g338,g383,n[326-337],n[339-381],n[388-422],g[384-387] Default=NO QoS=N/A DefaultTime=03:00:00 DisableRootJobs=NO ExclusiveUser=NO ExclusiveTopo=NO GraceTime=0 Hidden=NO MaxNodes=UNLIMITED MaxTime=7-00:00:00 MinNodes=0 LLN=NO MaxCPUsPerNode=UNLIMITED MaxCPUsPerSocket=UNLIMITED Nodes=g[324-325] PriorityJobFactor=0 PriorityTier=1 RootOnly=NO ReqResv=NO OverSubscribe=NO OverTimeLimit=NONE PreemptMode=OFF State=UP TotalCPUs=64 TotalNodes=2 SelectTypeParameters=NONE JobDefaults=(null) DefMemPerCPU=5000 MaxMemPerNode=UNLIMITED TRES=cpu=64,mem=382000M,node=2,billing=128,gres/gpu=8 TRESBillingWeights=CPU=2.0,Mem=0
PartitionName=lgpu AllowGroups=ALL AllowAccounts=ALL AllowQos=normal,high AllocNodes=aurochs,bison,yak,ood,ood-test,cryosparc-01,g32[1-5],g338,g383,n[326-337],n[339-381],n[388-422],g[384-387] Default=NO QoS=N/A DefaultTime=03:00:00 DisableRootJobs=NO ExclusiveUser=NO ExclusiveTopo=NO GraceTime=0 Hidden=NO MaxNodes=UNLIMITED MaxTime=3-00:00:00 MinNodes=0 LLN=NO MaxCPUsPerNode=UNLIMITED MaxCPUsPerSocket=UNLIMITED Nodes=g428 PriorityJobFactor=0 PriorityTier=1 RootOnly=NO ReqResv=NO OverSubscribe=NO OverTimeLimit=NONE PreemptMode=OFF State=UP TotalCPUs=64 TotalNodes=1 SelectTypeParameters=NONE JobDefaults=(null) DefMemPerCPU=5000 MaxMemPerNode=UNLIMITED TRES=cpu=64,mem=381500M,node=1,billing=128,gres/gpu=2 TRESBillingWeights=CPU=2.0,Mem=0
PartitionName=livi AllowGroups=ALL AllowAccounts=def-cjhuofw-ab,def-gshamov,def-kerrache AllowQos=normal AllocNodes=aurochs,bison,yak,ood,ood-test,cryosparc-01,g32[1-5],g338,g38[3-7],n[326-337],n[339-381] Default=NO QoS=N/A DefaultTime=03:00:00 DisableRootJobs=NO ExclusiveUser=NO ExclusiveTopo=NO GraceTime=0 Hidden=NO MaxNodes=UNLIMITED MaxTime=21-00:00:00 MinNodes=0 LLN=NO MaxCPUsPerNode=UNLIMITED MaxCPUsPerSocket=UNLIMITED Nodes=g338 PriorityJobFactor=0 PriorityTier=10 RootOnly=NO ReqResv=NO OverSubscribe=NO OverTimeLimit=NONE PreemptMode=OFF State=UP TotalCPUs=48 TotalNodes=1 SelectTypeParameters=NONE JobDefaults=(null) DefMemPerCPU=29000 MaxMemPerNode=UNLIMITED TRES=cpu=48,mem=1500000M,node=1,billing=48,gres/gpu=16 TRESBillingWeights=CPU=1.0,Mem=0
"#;

    #[test]
    fn parses_all_partitions() {
        let v = parse_scontrol_partitions(SAMPLE).unwrap();
        assert_eq!(v.len(), 13);
        assert_eq!(v[0].name, "skylake");
        assert_eq!(v[12].name, "livi");
    }

    #[test]
    fn keeps_values_with_internal_equals() {
        let v = parse_scontrol_partitions(SAMPLE).unwrap();
        let chrimlm = v.iter().find(|p| p.name == "chrimlm").unwrap();
        assert_eq!(chrimlm.get("TRESBillingWeights"), Some("CPU=2.0,Mem=0"));
    }

    #[test]
    fn extracts_some_known_fields() {
        let v = parse_scontrol_partitions(SAMPLE).unwrap();
        let genoa = v.iter().find(|p| p.name == "genoa").unwrap();
        assert_eq!(genoa.get("Nodes"), Some("n[393-419]"));
        assert_eq!(genoa.get("TotalNodes"), Some("27"));
        assert_eq!(genoa.get_u64("TotalNodes"), Some(27));
        assert_eq!(genoa.get_bool("ExclusiveUser"), Some(false));
    }

    #[test]
    fn parses_slurm_durations() {
        // From skylake
        let v = parse_scontrol_partitions(SAMPLE).unwrap();
        let skylake = v.iter().find(|p| p.name == "skylake").unwrap();

        let dt = skylake.get_duration("DefaultTime").unwrap();
        assert_eq!(dt.as_secs(), 3 * 3600);

        let mt = skylake.get_duration("MaxTime").unwrap();
        assert_eq!(mt.as_secs(), 21 * 24 * 3600);

        // Markers become None
        assert_eq!(skylake.get_duration("OverTimeLimit"), None);
        assert_eq!(skylake.get_duration("QoS"), None);
    }

    #[test]
    fn gpu_related_fields_present() {
        let v = parse_scontrol_partitions(SAMPLE).unwrap();
        let gpu = v.iter().find(|p| p.name == "gpu").unwrap();
        assert!(gpu.get("TRES").unwrap().contains("gres/gpu=8"));
        assert_eq!(gpu.get("PreemptMode"), Some("OFF"));

        let agro_b = v.iter().find(|p| p.name == "agro-b").unwrap();
        assert_eq!(agro_b.get("PreemptMode"), Some("CANCEL"));
        assert_eq!(
            agro_b.get("TRESBillingWeights"),
            Some("CPU=1.0,Mem=0,GRES/gpu=6.0")
        );
    }

    #[test]
    fn error_if_partition_name_missing() {
        let bad = "AllowGroups=ALL QoS=N/A";
        let err = parse_scontrol_partitions(bad).unwrap_err();
        assert_eq!(err, ParseError::MissingField("PartitionName"));
    }
}
