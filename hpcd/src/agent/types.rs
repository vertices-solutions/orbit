// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use proto::{StreamEvent, SubmitStreamEvent};
use std::pin::Pin;
use thiserror::Error as ThisError;
use tokio_stream::Stream;
use tonic::Status;

use crate::agent::error_codes;

pub type OutStream =
    Pin<Box<dyn Stream<Item = Result<StreamEvent, Status>> + Send + Sync + 'static>>;
pub type SubmitOutStream =
    Pin<Box<dyn Stream<Item = Result<SubmitStreamEvent, Status>> + Send + Sync + 'static>>;

#[derive(Debug, PartialEq, Eq, ThisError)]
pub enum AgentSvcError {
    #[error("unknown name")]
    UnknownName,

    #[error("network error: {0}")]
    NetworkError(String),

    #[error("database error: {error}")]
    DatabaseError { error: String },
}

impl AgentSvcError {
    pub fn code(&self) -> &'static str {
        match self {
            AgentSvcError::UnknownName => error_codes::NOT_FOUND,
            AgentSvcError::NetworkError(_) => error_codes::NETWORK_ERROR,
            AgentSvcError::DatabaseError { .. } => error_codes::INTERNAL_ERROR,
        }
    }
}
