// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Address for a host: either hostname or IP.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Address {
    Hostname(String),
    Ip(IpAddr),
}

#[derive(Debug)]
pub enum ParseSlurmVersionError {
    WrongFormat, // not exactly 3 dot-separated parts
    NotANumber,  // one part isn’t an integer
}

/// Slurm version triplet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SlurmVersion {
    pub major: i64,
    pub minor: i64,
    pub patch: i64,
}

impl std::fmt::Display for SlurmVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl FromStr for SlurmVersion {
    type Err = ParseSlurmVersionError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let mut parts = s.split(".");

        let major = parts
            .next()
            .ok_or(ParseSlurmVersionError::WrongFormat)?
            .parse::<i64>()
            .map_err(|_| ParseSlurmVersionError::NotANumber)?;
        let minor = parts
            .next()
            .ok_or(ParseSlurmVersionError::WrongFormat)?
            .parse::<i64>()
            .map_err(|_| ParseSlurmVersionError::NotANumber)?;
        let patch = parts
            .next()
            .ok_or(ParseSlurmVersionError::WrongFormat)?
            .parse::<i64>()
            .map_err(|_| ParseSlurmVersionError::NotANumber)?;

        if parts.next().is_some() {
            return Err(ParseSlurmVersionError::WrongFormat);
        }
        Ok(SlurmVersion {
            major,
            minor,
            patch,
        })
    }
}

/// Linux distribution info.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Distro {
    pub name: String,
    pub version: String,
}

/// Payload for creating or upserting a host.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct NewHost {
    /// Short, memorable, globally-unique name (e.g., "gpu01", "c1", "node-a")
    pub name: String,
    /// Username for server
    pub username: String,
    /// hostname or IP address
    pub address: Address,
    // ssh port
    pub port: u16,
    // ssh identity path
    pub identity_path: Option<String>,
    // WLM version, TODO: make this more general
    pub slurm: SlurmVersion,
    /// Linux distribution installed on cluster head
    pub distro: Distro,
    /// version of kernel on cluster host
    pub kernel_version: String,

    /// availability of accounting (to be used for more fine-grained control over jobs)
    pub accounting_available: bool,

    pub default_base_path: Option<String>,
}

/// Full stored host record.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct HostRecord {
    pub id: i64,
    pub name: String,
    pub username: String,
    pub address: Address,
    pub port: u16,
    pub identity_path: Option<String>,
    pub slurm: SlurmVersion,
    pub distro: Distro,
    pub kernel_version: String,
    pub created_at: String, // RFC3339
    pub updated_at: String, // RFC3339
    pub accounting_available: bool,
    pub default_base_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct NewJob {
    /// Scheduler job ID; host-specific. Database will additionally keep its own, internal id.
    pub scheduler_id: Option<i64>,
    /// Host row id on which the job is submitted.
    pub host_id: i64,
    pub local_path: String,
    pub remote_path: String,
    pub stdout_path: String,
    pub stderr_path: Option<String>,
    pub project_name: Option<String>,
    pub default_retrieve_path: Option<String>,
    pub template_values: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct JobRecord {
    pub id: i64,
    pub scheduler_id: Option<i64>,
    pub name: String,
    pub created_at: String,
    pub finished_at: Option<String>,
    pub is_completed: bool,
    pub terminal_state: Option<String>,
    pub scheduler_state: Option<String>,
    pub local_path: String,
    pub remote_path: String,
    pub stdout_path: String,
    pub stderr_path: Option<String>,
    pub project_name: Option<String>,
    pub default_retrieve_path: Option<String>,
    pub template_values: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ProjectRecord {
    pub name: String,
    pub path: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshConfig {
    pub session_name: Option<String>,
    pub host: String,
    pub addr: SocketAddr,
    pub username: String,
    pub identity_path: Option<String>,
    pub ki_submethods: Option<String>,
    pub keepalive_secs: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncFilterAction {
    Include,
    Exclude,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncFilterRule {
    pub action: SyncFilterAction,
    pub pattern: String,
}

#[derive(Clone, Debug, Default)]
pub struct SyncOptions {
    pub block_size: Option<usize>,
    pub parallelism: Option<usize>,
    pub filters: Vec<SyncFilterRule>,
}

#[derive(Clone, Debug)]
pub struct ClusterStatus {
    pub host: HostRecord,
    pub connected: bool,
    pub reachable: bool,
}
