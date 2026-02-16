// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use async_trait::async_trait;

use crate::app::errors::AppResult;
use crate::app::types::{BlueprintRecord, NewBlueprintBuild};

#[async_trait]
/// Persistence boundary for local blueprint registry records.
pub trait BlueprintStorePort: Send + Sync {
    async fn upsert_blueprint(&self, name: &str, path: &str) -> AppResult<BlueprintRecord>;
    async fn upsert_blueprint_build(&self, build: &NewBlueprintBuild)
    -> AppResult<BlueprintRecord>;
    async fn get_blueprint_by_name(&self, name: &str) -> AppResult<Option<BlueprintRecord>>;
    async fn get_latest_blueprint_build(
        &self,
        blueprint_name: &str,
    ) -> AppResult<Option<BlueprintRecord>>;
    // This returns a unique record for each blueprint-record pair.
    async fn list_blueprints(&self) -> AppResult<Vec<BlueprintRecord>>;
    async fn delete_blueprint_by_name(&self, name: &str) -> AppResult<usize>;
    async fn delete_blueprints_by_base_name(&self, name: &str) -> AppResult<usize>;
    async fn max_build_number_for_date(
        &self,
        blueprint_name: &str,
        date_prefix: &str,
    ) -> AppResult<Option<u16>>;
}
