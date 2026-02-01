// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use async_trait::async_trait;
use proto::{StreamEvent, SubmitStreamEvent};

use crate::app::errors::AppResult;

#[async_trait]
pub trait StreamOutputPort: Send + Sync {
    async fn send(&self, event: StreamEvent) -> AppResult<()>;
    fn is_closed(&self) -> bool;
}

#[async_trait]
pub trait SubmitStreamOutputPort: Send + Sync {
    async fn send(&self, event: SubmitStreamEvent) -> AppResult<()>;
    fn is_closed(&self) -> bool;
}
