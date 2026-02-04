// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::sync::Arc;

use async_trait::async_trait;

use crate::adapters::db::{HostStore, HostStoreError};
use crate::app::errors::{AppError, AppErrorKind, AppResult, codes};
use crate::app::ports::{ClusterStorePort, JobStorePort, ProjectStorePort};
use crate::app::types::{HostRecord, JobRecord, NewHost, NewJob, ProjectRecord};

#[derive(Clone)]
pub struct SqliteStoreAdapter {
    store: Arc<HostStore>,
}

impl SqliteStoreAdapter {
    pub fn new(store: HostStore) -> Self {
        Self {
            store: Arc::new(store),
        }
    }
}
/// SqliteStoreAdapter is an outbound adapter implementing ports,
/// so it’s the right place to translate persistence‑specific errors
/// (HostStoreError, sqlx) into app-level errors (AppError) and keep the domain/app core free of DB details.
fn map_store_error(err: HostStoreError) -> AppError {
    match err {
        HostStoreError::EmptyName
        | HostStoreError::InvalidAddress
        | HostStoreError::EmptyProjectName
        | HostStoreError::EmptyProjectPath => {
            AppError::new(AppErrorKind::InvalidArgument, codes::INVALID_ARGUMENT)
        }
        HostStoreError::ProjectPathConflict {
            name,
            existing_path,
            new_path,
        } => AppError::with_message(
            AppErrorKind::Conflict,
            codes::CONFLICT,
            format!(
                "project '{}' is already registered at '{}'; cannot register '{}'",
                name, existing_path, new_path
            ),
        ),
        HostStoreError::HostNotFound(_) => {
            AppError::new(AppErrorKind::InvalidArgument, codes::NOT_FOUND)
        }
        HostStoreError::Sqlx(_) => AppError::new(AppErrorKind::Internal, codes::INTERNAL_ERROR),
    }
}

#[async_trait]
impl ClusterStorePort for SqliteStoreAdapter {
    async fn insert_host(&self, host: &NewHost) -> AppResult<i64> {
        self.store.insert_host(host).await.map_err(map_store_error)
    }

    async fn upsert_host(&self, host: &NewHost) -> AppResult<i64> {
        self.store.upsert_host(host).await.map_err(map_store_error)
    }

    async fn update_host(&self, id: i64, host: &NewHost) -> AppResult<()> {
        self.store
            .update_host(id, host)
            .await
            .map_err(map_store_error)
    }

    async fn delete_by_name(&self, name: &str) -> AppResult<usize> {
        self.store
            .delete_by_name(name)
            .await
            .map_err(map_store_error)
    }

    async fn get_by_name(&self, name: &str) -> AppResult<Option<HostRecord>> {
        self.store.get_by_name(name).await.map_err(map_store_error)
    }

    async fn list_hosts(&self, username: Option<&str>) -> AppResult<Vec<HostRecord>> {
        self.store
            .list_hosts(username)
            .await
            .map_err(map_store_error)
    }
}

#[async_trait]
impl JobStorePort for SqliteStoreAdapter {
    async fn insert_job(&self, job: &NewJob) -> AppResult<i64> {
        self.store.insert_job(job).await.map_err(map_store_error)
    }

    async fn list_jobs_for_host(&self, host_id: i64) -> AppResult<Vec<JobRecord>> {
        self.store
            .list_jobs_for_host(host_id)
            .await
            .map_err(map_store_error)
    }

    async fn list_all_jobs(&self) -> AppResult<Vec<JobRecord>> {
        self.store.list_all_jobs().await.map_err(map_store_error)
    }

    async fn list_running_jobs(&self) -> AppResult<Vec<JobRecord>> {
        self.store
            .list_running_jobs()
            .await
            .map_err(map_store_error)
    }

    async fn latest_remote_path_for_local_path(
        &self,
        host_name: &str,
        local_path: &str,
        template_values: Option<&str>,
    ) -> AppResult<Option<String>> {
        self.store
            .latest_remote_path_for_local_path(host_name, local_path, template_values)
            .await
            .map_err(map_store_error)
    }

    async fn running_job_id_for_remote_path(
        &self,
        host_name: &str,
        remote_path: &str,
    ) -> AppResult<Option<i64>> {
        self.store
            .running_job_id_for_remote_path(host_name, remote_path)
            .await
            .map_err(map_store_error)
    }

    async fn get_job_by_job_id(&self, id: i64) -> AppResult<Option<JobRecord>> {
        self.store
            .get_job_by_job_id(id)
            .await
            .map_err(map_store_error)
    }

    async fn mark_job_completed(&self, id: i64, terminal_state: Option<&str>) -> AppResult<()> {
        self.store
            .mark_job_completed(id, terminal_state)
            .await
            .map_err(map_store_error)
    }

    async fn delete_job_by_job_id(&self, id: i64) -> AppResult<bool> {
        self.store
            .delete_job_by_job_id(id)
            .await
            .map_err(map_store_error)
    }

    async fn update_job_scheduler_state(
        &self,
        id: i64,
        scheduler_state: Option<&str>,
    ) -> AppResult<()> {
        self.store
            .update_job_scheduler_state(id, scheduler_state)
            .await
            .map_err(map_store_error)
    }
}

#[async_trait]
impl ProjectStorePort for SqliteStoreAdapter {
    async fn upsert_project(&self, name: &str, path: &str) -> AppResult<ProjectRecord> {
        self.store
            .upsert_project(name, path)
            .await
            .map_err(map_store_error)
    }

    async fn get_project_by_name(&self, name: &str) -> AppResult<Option<ProjectRecord>> {
        self.store
            .get_project_by_name(name)
            .await
            .map_err(map_store_error)
    }

    async fn list_projects(&self) -> AppResult<Vec<ProjectRecord>> {
        self.store.list_projects().await.map_err(map_store_error)
    }

    async fn delete_project_by_name(&self, name: &str) -> AppResult<usize> {
        self.store
            .delete_project_by_name(name)
            .await
            .map_err(map_store_error)
    }
}
