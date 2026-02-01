// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use async_trait::async_trait;
use std::net::SocketAddr;

use crate::app::errors::AppResult;
use crate::app::types::Address;

#[async_trait]
pub trait NetworkProbePort: Send + Sync {
    async fn resolve_host_addr(&self, address: &Address, port: u16) -> AppResult<SocketAddr>;
    async fn check_host_reachable(&self, address: &Address, port: u16) -> AppResult<bool>;
}
