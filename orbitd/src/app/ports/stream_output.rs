// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use async_trait::async_trait;
use proto::{RunStreamEvent, StreamEvent};

use crate::app::errors::AppResult;

#[async_trait]
/// Streaming output sink for non-run RPCs.
/// Emits `StreamEvent` without tying use-cases to transport.
pub trait StreamOutputPort: Send + Sync {
    async fn send(&self, event: StreamEvent) -> AppResult<()>;
    fn is_closed(&self) -> bool;
}

#[async_trait]
/// Streaming output sink for run flows.
/// Emits `RunStreamEvent` for run-specific progress/output.
pub trait RunStreamOutputPort: Send + Sync {
    async fn send(&self, event: RunStreamEvent) -> AppResult<()>;
    fn is_closed(&self) -> bool;
}
