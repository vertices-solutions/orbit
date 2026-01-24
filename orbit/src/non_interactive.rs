// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use serde_json::{Value, json};
use thiserror::Error;

pub const EXIT_CODE_USAGE: i32 = 2;
pub const EXIT_CODE_MFA_REQUIRED: i32 = 5;
pub const EXIT_CODE_OTHER: i32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NonInteractiveErrorKind {
    MissingInput,
    ConfirmationRequired,
    MfaRequired,
    Other,
}

impl NonInteractiveErrorKind {
    pub fn code(self) -> &'static str {
        match self {
            NonInteractiveErrorKind::MissingInput => "missing_input",
            NonInteractiveErrorKind::ConfirmationRequired => "confirmation_required",
            NonInteractiveErrorKind::MfaRequired => "mfa_required",
            NonInteractiveErrorKind::Other => "error",
        }
    }
}

#[derive(Debug, Clone, Error)]
#[error("{message}")]
pub struct NonInteractiveError {
    pub kind: NonInteractiveErrorKind,
    pub message: String,
    pub exit_code: i32,
}

impl NonInteractiveError {
    pub fn missing_input(message: impl Into<String>) -> Self {
        Self {
            kind: NonInteractiveErrorKind::MissingInput,
            message: message.into(),
            exit_code: EXIT_CODE_USAGE,
        }
    }

    pub fn confirmation_required(message: impl Into<String>) -> Self {
        Self {
            kind: NonInteractiveErrorKind::ConfirmationRequired,
            message: message.into(),
            exit_code: EXIT_CODE_USAGE,
        }
    }

    pub fn mfa_required(message: impl Into<String>) -> Self {
        Self {
            kind: NonInteractiveErrorKind::MfaRequired,
            message: message.into(),
            exit_code: EXIT_CODE_MFA_REQUIRED,
        }
    }

    pub fn other(message: impl Into<String>) -> Self {
        Self {
            kind: NonInteractiveErrorKind::Other,
            message: message.into(),
            exit_code: EXIT_CODE_OTHER,
        }
    }

    pub fn other_with_exit_code(message: impl Into<String>, exit_code: i32) -> Self {
        Self {
            kind: NonInteractiveErrorKind::Other,
            message: message.into(),
            exit_code,
        }
    }
}

pub fn json_ok(result: Value) -> Value {
    json!({
        "ok": true,
        "result": result,
    })
}

pub fn json_error(err: &NonInteractiveError) -> Value {
    json!({
        "ok": false,
        "error": {
            "code": err.kind.code(),
            "message": err.message.as_str(),
        }
    })
}
