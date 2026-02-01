// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use async_trait::async_trait;
use proto::{StreamEvent, SubmitStreamEvent};

use crate::app::errors::AppResult;

#[async_trait]
/// Streaming output sink for non-submit RPCs.
/// Emits `StreamEvent` without tying use-cases to transport.
pub trait StreamOutputPort: Send + Sync {
    async fn send(&self, event: StreamEvent) -> AppResult<()>;
    fn is_closed(&self) -> bool;
}

#[async_trait]
/// Streaming output sink for submit flows.
/// Emits `SubmitStreamEvent` for submit-specific progress/output.
pub trait SubmitStreamOutputPort: Send + Sync {
    async fn send(&self, event: SubmitStreamEvent) -> AppResult<()>;
    fn is_closed(&self) -> bool;
}
