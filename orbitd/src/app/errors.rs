// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::fmt;

pub mod codes {
    pub const AUTHENTICATION_FAILURE: &str = "authentication_failure";
    pub const CONNECTION_FAILURE: &str = "connection_failure";
    pub const NETWORK_ERROR: &str = "network_error";
    pub const INVALID_ARGUMENT: &str = "invalid_argument";
    pub const NOT_FOUND: &str = "not_found";
    pub const CONFLICT: &str = "conflict";
    pub const INTERNAL_ERROR: &str = "internal_error";
    pub const CANCELED: &str = "canceled";
    pub const REMOTE_ERROR: &str = "remote_error";
    pub const LOCAL_ERROR: &str = "local_error";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppErrorKind {
    InvalidArgument,
    NotFound,
    Conflict,
    AlreadyExists,
    Internal,
    Aborted,
    Cancelled,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct AppError {
    kind: AppErrorKind,
    code: &'static str,
    message: String,
    context: Option<String>,
}

impl AppError {
    pub fn new(kind: AppErrorKind, code: &'static str) -> Self {
        Self {
            kind,
            code,
            message: code.to_string(),
            context: None,
        }
    }

    pub fn with_message(
        kind: AppErrorKind,
        code: &'static str,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            code,
            message: message.into(),
            context: None,
        }
    }

    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.context = Some(context.into());
        self
    }

    pub fn kind(&self) -> AppErrorKind {
        self.kind
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn context(&self) -> Option<&str> {
        self.context.as_deref()
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ctx) = &self.context {
            write!(f, "{} ({})", self.message, ctx)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

impl std::error::Error for AppError {}

pub type AppResult<T> = Result<T, AppError>;
