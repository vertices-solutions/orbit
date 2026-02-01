// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use async_trait::async_trait;

use crate::app::errors::AppResult;
use crate::app::types::{JobRecord, NewJob};

#[async_trait]
pub trait JobStorePort: Send + Sync {
    async fn insert_job(&self, job: &NewJob) -> AppResult<i64>;
    async fn list_jobs_for_host(&self, host_id: i64) -> AppResult<Vec<JobRecord>>;
    async fn list_all_jobs(&self) -> AppResult<Vec<JobRecord>>;
    async fn list_running_jobs(&self) -> AppResult<Vec<JobRecord>>;
    async fn latest_remote_path_for_local_path(
        &self,
        host_name: &str,
        local_path: &str,
    ) -> AppResult<Option<String>>;
    async fn running_job_id_for_remote_path(
        &self,
        host_name: &str,
        remote_path: &str,
    ) -> AppResult<Option<i64>>;
    async fn get_job_by_job_id(&self, id: i64) -> AppResult<Option<JobRecord>>;
    async fn mark_job_completed(&self, id: i64, terminal_state: Option<&str>) -> AppResult<()>;
    async fn delete_job_by_job_id(&self, id: i64) -> AppResult<bool>;
    async fn update_job_scheduler_state(
        &self,
        id: i64,
        scheduler_state: Option<&str>,
    ) -> AppResult<()>;
}
