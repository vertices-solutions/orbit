// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use async_trait::async_trait;

use crate::app::errors::AppResult;
use crate::app::types::{HostRecord, NewHost};

#[async_trait]
pub trait ClusterStorePort: Send + Sync {
    async fn insert_host(&self, host: &NewHost) -> AppResult<i64>;
    async fn upsert_host(&self, host: &NewHost) -> AppResult<i64>;
    async fn update_host(&self, id: i64, host: &NewHost) -> AppResult<()>;
    async fn delete_by_name(&self, name: &str) -> AppResult<usize>;
    async fn get_by_name(&self, name: &str) -> AppResult<Option<HostRecord>>;
    async fn list_hosts(&self, username: Option<&str>) -> AppResult<Vec<HostRecord>>;
}
