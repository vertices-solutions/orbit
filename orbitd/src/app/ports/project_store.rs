// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use async_trait::async_trait;

use crate::app::errors::AppResult;
use crate::app::types::{NewProjectBuild, ProjectRecord};

#[async_trait]
/// Persistence boundary for local project registry records.
pub trait ProjectStorePort: Send + Sync {
    async fn upsert_project(&self, name: &str, path: &str) -> AppResult<ProjectRecord>;
    async fn upsert_project_build(&self, build: &NewProjectBuild) -> AppResult<ProjectRecord>;
    async fn get_project_by_name(&self, name: &str) -> AppResult<Option<ProjectRecord>>;
    async fn get_latest_project_build(
        &self,
        project_name: &str,
    ) -> AppResult<Option<ProjectRecord>>;
    async fn list_projects(&self) -> AppResult<Vec<ProjectRecord>>;
    async fn delete_project_by_name(&self, name: &str) -> AppResult<usize>;
    async fn delete_projects_by_base_name(&self, name: &str) -> AppResult<usize>;
    async fn max_build_number_for_date(
        &self,
        project_name: &str,
        date_prefix: &str,
    ) -> AppResult<Option<u16>>;
}
