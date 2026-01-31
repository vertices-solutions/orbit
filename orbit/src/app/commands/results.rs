// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::path::PathBuf;

use proto::{ListClustersUnitResponse, ListJobsUnitResponse};

#[derive(Debug, Default, Clone)]
pub struct StreamCapture {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: Option<i32>,
    pub error_code: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct SubmitCapture {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: Option<i32>,
    pub job_id: Option<i64>,
    pub remote_path: Option<String>,
    pub detail: Option<String>,
    pub error_code: Option<String>,
}

#[derive(Debug, Clone)]
pub enum CommandResult {
    Message { message: String },
    Pong { message: String },
    JobList { jobs: Vec<ListJobsUnitResponse> },
    JobDetails { job: ListJobsUnitResponse },
    JobSubmit {
        cluster: String,
        local_path: String,
        sbatchscript: String,
        capture: SubmitCapture,
    },
    JobLogs { capture: StreamCapture },
    JobCancel { job_id: i64, capture: StreamCapture },
    JobCleanup {
        job_id: i64,
        force: bool,
        full: bool,
        capture: StreamCapture,
    },
    JobLs { capture: StreamCapture },
    JobRetrieve {
        job_id: i64,
        path: String,
        output: PathBuf,
        capture: StreamCapture,
    },
    ClusterList { clusters: Vec<ListClustersUnitResponse> },
    ClusterDetails { cluster: ListClustersUnitResponse },
    ClusterLs { capture: StreamCapture },
    ClusterAdd {
        name: String,
        username: String,
        hostname: Option<String>,
        ip: Option<String>,
        port: u32,
        identity_path: String,
        default_base_path: Option<String>,
    },
    ClusterSet {
        name: String,
        updated_fields: Vec<(String, String)>,
    },
    ClusterDelete { name: String },
}
