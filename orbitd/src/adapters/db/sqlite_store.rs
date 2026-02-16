// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::sync::Arc;

use async_trait::async_trait;

use crate::adapters::db::{HostStore, HostStoreError};
use crate::app::errors::{AppError, AppErrorKind, AppResult, codes};
use crate::app::ports::{BlueprintStorePort, ClusterStorePort, JobStorePort};
use crate::app::types::{
    BlueprintRecord, HostRecord, JobRecord, NewBlueprintBuild, NewHost, NewJob,
};

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
        | HostStoreError::EmptyBlueprintName
        | HostStoreError::EmptyBlueprintPath => {
            AppError::new(AppErrorKind::InvalidArgument, codes::INVALID_ARGUMENT)
        }
        HostStoreError::BlueprintPathConflict {
            name,
            existing_path,
            new_path,
        } => AppError::with_message(
            AppErrorKind::Conflict,
            codes::CONFLICT,
            format!(
                "blueprint '{}' is already registered at '{}'; cannot register '{}'",
                name, existing_path, new_path
            ),
        ),
        HostStoreError::HostNotFound(_) => {
            AppError::new(AppErrorKind::InvalidArgument, codes::NOT_FOUND)
        }
        HostStoreError::Serde(_) => AppError::new(AppErrorKind::Internal, codes::INTERNAL_ERROR),
        HostStoreError::Sqlx(_) => AppError::new(AppErrorKind::Internal, codes::INTERNAL_ERROR),
    }
}

#[async_trait]
impl ClusterStorePort for SqliteStoreAdapter {
    #[tracing::instrument(
        level = "debug",
        skip(self, host),
        fields(op = "insert_host", table = "hosts")
    )]
    async fn insert_host(&self, host: &NewHost) -> AppResult<i64> {
        self.store.insert_host(host).await.map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, host),
        fields(op = "upsert_host", table = "hosts")
    )]
    async fn upsert_host(&self, host: &NewHost) -> AppResult<i64> {
        self.store.upsert_host(host).await.map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, host),
        fields(op = "update_host", table = "hosts")
    )]
    async fn update_host(&self, id: i64, host: &NewHost) -> AppResult<()> {
        self.store
            .update_host(id, host)
            .await
            .map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, name),
        fields(op = "delete_by_name", table = "hosts")
    )]
    async fn delete_by_name(&self, name: &str) -> AppResult<usize> {
        self.store
            .delete_by_name(name)
            .await
            .map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, name),
        fields(op = "get_by_name", table = "hosts")
    )]
    async fn get_by_name(&self, name: &str) -> AppResult<Option<HostRecord>> {
        self.store.get_by_name(name).await.map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, username),
        fields(op = "list_hosts", table = "hosts")
    )]
    async fn list_hosts(&self, username: Option<&str>) -> AppResult<Vec<HostRecord>> {
        self.store
            .list_hosts(username)
            .await
            .map_err(map_store_error)
    }
}

#[async_trait]
impl JobStorePort for SqliteStoreAdapter {
    #[tracing::instrument(
        level = "debug",
        skip(self, job),
        fields(op = "insert_job", table = "jobs")
    )]
    async fn insert_job(&self, job: &NewJob) -> AppResult<i64> {
        self.store.insert_job(job).await.map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(op = "list_jobs_for_host", table = "jobs")
    )]
    async fn list_jobs_for_host(&self, host_id: i64) -> AppResult<Vec<JobRecord>> {
        self.store
            .list_jobs_for_host(host_id)
            .await
            .map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(op = "list_all_jobs", table = "jobs")
    )]
    async fn list_all_jobs(&self) -> AppResult<Vec<JobRecord>> {
        self.store.list_all_jobs().await.map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(op = "list_running_jobs", table = "jobs")
    )]
    async fn list_running_jobs(&self) -> AppResult<Vec<JobRecord>> {
        self.store
            .list_running_jobs()
            .await
            .map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, host_name, local_path, template_values),
        fields(op = "latest_remote_path_for_local_path", table = "jobs")
    )]
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

    #[tracing::instrument(
        level = "debug",
        skip(self, host_name, remote_path),
        fields(op = "running_job_id_for_remote_path", table = "jobs")
    )]
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

    #[tracing::instrument(
        level = "debug",
        skip(self, host_name, blueprint_name, template_values),
        fields(op = "latest_remote_path_for_blueprint", table = "jobs")
    )]
    async fn latest_remote_path_for_blueprint(
        &self,
        host_name: &str,
        blueprint_name: &str,
        template_values: Option<&str>,
    ) -> AppResult<Option<String>> {
        self.store
            .latest_remote_path_for_blueprint(host_name, blueprint_name, template_values)
            .await
            .map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, host_name, local_path, template_values),
        fields(op = "running_job_id_for_local_path", table = "jobs")
    )]
    async fn running_job_id_for_local_path(
        &self,
        host_name: &str,
        local_path: &str,
        template_values: Option<&str>,
    ) -> AppResult<Option<i64>> {
        self.store
            .running_job_id_for_local_path(host_name, local_path, template_values)
            .await
            .map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, host_name, blueprint_name, template_values),
        fields(op = "running_job_id_for_blueprint", table = "jobs")
    )]
    async fn running_job_id_for_blueprint(
        &self,
        host_name: &str,
        blueprint_name: &str,
        template_values: Option<&str>,
    ) -> AppResult<Option<i64>> {
        self.store
            .running_job_id_for_blueprint(host_name, blueprint_name, template_values)
            .await
            .map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(op = "get_job_by_job_id", table = "jobs")
    )]
    async fn get_job_by_job_id(&self, id: i64) -> AppResult<Option<JobRecord>> {
        self.store
            .get_job_by_job_id(id)
            .await
            .map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, terminal_state),
        fields(op = "mark_job_completed", table = "jobs")
    )]
    async fn mark_job_completed(&self, id: i64, terminal_state: Option<&str>) -> AppResult<()> {
        self.store
            .mark_job_completed(id, terminal_state)
            .await
            .map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(op = "delete_job_by_job_id", table = "jobs")
    )]
    async fn delete_job_by_job_id(&self, id: i64) -> AppResult<bool> {
        self.store
            .delete_job_by_job_id(id)
            .await
            .map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, scheduler_state),
        fields(op = "update_job_scheduler_state", table = "jobs")
    )]
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
impl BlueprintStorePort for SqliteStoreAdapter {
    #[tracing::instrument(
        level = "debug",
        skip(self, name, path),
        fields(op = "upsert_blueprint", table = "blueprints")
    )]
    async fn upsert_blueprint(&self, name: &str, path: &str) -> AppResult<BlueprintRecord> {
        self.store
            .upsert_blueprint(name, path)
            .await
            .map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, build),
        fields(op = "upsert_blueprint_build", table = "blueprints")
    )]
    async fn upsert_blueprint_build(
        &self,
        build: &NewBlueprintBuild,
    ) -> AppResult<BlueprintRecord> {
        self.store
            .upsert_blueprint_build(build)
            .await
            .map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, name),
        fields(op = "get_blueprint_by_name", table = "blueprints")
    )]
    async fn get_blueprint_by_name(&self, name: &str) -> AppResult<Option<BlueprintRecord>> {
        self.store
            .get_blueprint_by_name(name)
            .await
            .map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, blueprint_name),
        fields(op = "get_latest_blueprint_build", table = "blueprints")
    )]
    async fn get_latest_blueprint_build(
        &self,
        blueprint_name: &str,
    ) -> AppResult<Option<BlueprintRecord>> {
        self.store
            .get_latest_blueprint_build(blueprint_name)
            .await
            .map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(op = "list_blueprints", table = "blueprints")
    )]
    async fn list_blueprints(&self) -> AppResult<Vec<BlueprintRecord>> {
        self.store.list_blueprints().await.map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, name),
        fields(op = "delete_blueprint_by_name", table = "blueprints")
    )]
    async fn delete_blueprint_by_name(&self, name: &str) -> AppResult<usize> {
        self.store
            .delete_blueprint_by_name(name)
            .await
            .map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, name),
        fields(op = "delete_blueprints_by_base_name", table = "blueprints")
    )]
    async fn delete_blueprints_by_base_name(&self, name: &str) -> AppResult<usize> {
        self.store
            .delete_blueprints_by_base_name(name)
            .await
            .map_err(map_store_error)
    }

    #[tracing::instrument(
        level = "debug",
        skip(self, blueprint_name),
        fields(op = "max_build_number_for_date", table = "blueprints")
    )]
    async fn max_build_number_for_date(
        &self,
        blueprint_name: &str,
        date_prefix: &str,
    ) -> AppResult<Option<u16>> {
        self.store
            .max_build_number_for_date(blueprint_name, date_prefix)
            .await
            .map_err(map_store_error)
    }
}
