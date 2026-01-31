// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::fmt;

pub const EXIT_CODE_USAGE: i32 = 2;
pub const EXIT_CODE_MFA_REQUIRED: i32 = 5;
pub const EXIT_CODE_OTHER: i32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorType {
    InvalidArgument,
    ConfirmationRequired,
    MfaRequired,
    PermissionDenied,
    ClusterNotFound,
    JobNotFound,
    Conflict,
    DaemonUnavailable,
    NetworkError,
    RemoteError,
    LocalError,
    InternalError,
}

impl ErrorType {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorType::InvalidArgument => "INVALID_ARGUMENT",
            ErrorType::ConfirmationRequired => "CONFIRMATION_REQUIRED",
            ErrorType::MfaRequired => "MFA_REQUIRED",
            ErrorType::PermissionDenied => "PERMISSION_DENIED",
            ErrorType::ClusterNotFound => "CLUSTER_NOT_FOUND",
            ErrorType::JobNotFound => "JOB_NOT_FOUND",
            ErrorType::Conflict => "CONFLICT",
            ErrorType::DaemonUnavailable => "DAEMON_UNAVAILABLE",
            ErrorType::NetworkError => "NETWORK_ERROR",
            ErrorType::RemoteError => "REMOTE_ERROR",
            ErrorType::LocalError => "LOCAL_ERROR",
            ErrorType::InternalError => "INTERNAL_ERROR",
        }
    }

    pub fn default_exit_code(self) -> i32 {
        match self {
            ErrorType::InvalidArgument | ErrorType::ConfirmationRequired => EXIT_CODE_USAGE,
            ErrorType::MfaRequired => EXIT_CODE_MFA_REQUIRED,
            _ => EXIT_CODE_OTHER,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppError {
    pub kind: ErrorType,
    pub message: String,
    pub exit_code: i32,
}

impl AppError {
    pub fn new(kind: ErrorType, message: impl Into<String>) -> Self {
        let message = message.into();
        let exit_code = kind.default_exit_code();
        Self {
            kind,
            message,
            exit_code,
        }
    }

    pub fn with_exit_code(kind: ErrorType, message: impl Into<String>, exit_code: i32) -> Self {
        Self {
            kind,
            message: message.into(),
            exit_code,
        }
    }

    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(ErrorType::InvalidArgument, message)
    }

    pub fn confirmation_required(message: impl Into<String>) -> Self {
        Self::new(ErrorType::ConfirmationRequired, message)
    }

    pub fn mfa_required(message: impl Into<String>) -> Self {
        Self::new(ErrorType::MfaRequired, message)
    }

    pub fn permission_denied(message: impl Into<String>) -> Self {
        Self::new(ErrorType::PermissionDenied, message)
    }

    pub fn cluster_not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorType::ClusterNotFound, message)
    }

    pub fn job_not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorType::JobNotFound, message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(ErrorType::Conflict, message)
    }

    pub fn daemon_unavailable(message: impl Into<String>) -> Self {
        Self::new(ErrorType::DaemonUnavailable, message)
    }

    pub fn network_error(message: impl Into<String>) -> Self {
        Self::new(ErrorType::NetworkError, message)
    }

    pub fn remote_error(message: impl Into<String>) -> Self {
        Self::new(ErrorType::RemoteError, message)
    }

    pub fn local_error(message: impl Into<String>) -> Self {
        Self::new(ErrorType::LocalError, message)
    }

    pub fn internal_error(message: impl Into<String>) -> Self {
        Self::new(ErrorType::InternalError, message)
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for AppError {}

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorContext {
    Cluster,
    Job,
    Generic,
}

pub fn error_type_for_remote_code(code: &str, context: ErrorContext) -> ErrorType {
    match code {
        "invalid_argument" => ErrorType::InvalidArgument,
        "permission_denied" => ErrorType::PermissionDenied,
        "conflict" => ErrorType::Conflict,
        "not_found" => match context {
            ErrorContext::Cluster => ErrorType::ClusterNotFound,
            ErrorContext::Job => ErrorType::JobNotFound,
            ErrorContext::Generic => ErrorType::InternalError,
        },
        _ => ErrorType::RemoteError,
    }
}

const AUTHENTICATION_FAILURE: &str = "authentication_failure";
const CONNECTION_FAILURE: &str = "connection_failure";
const NETWORK_ERROR: &str = "network_error";
const INVALID_ARGUMENT: &str = "invalid_argument";
const NOT_FOUND: &str = "not_found";
const CONFLICT: &str = "conflict";
const INTERNAL_ERROR: &str = "internal_error";
const CANCELED: &str = "canceled";
const REMOTE_ERROR: &str = "remote_error";
const LOCAL_ERROR: &str = "local_error";
const PERMISSION_DENIED: &str = "permission_denied";

pub(crate) fn format_server_error(raw: &str) -> String {
    if let Some(message) = describe_error_code(raw) {
        return message.to_string();
    }
    raw.to_string()
}

pub(crate) fn describe_error_code(code: &str) -> Option<&'static str> {
    match code {
        AUTHENTICATION_FAILURE => {
            Some("Authentication failed. Check your username, SSH key, and MFA responses.")
        }
        CONNECTION_FAILURE => Some("Could not establish an SSH connection to the cluster."),
        NETWORK_ERROR => Some("Network error while resolving or reaching the cluster."),
        INVALID_ARGUMENT => Some("Invalid input; check the command arguments."),
        NOT_FOUND => Some("Requested item not found."),
        CONFLICT => Some("The requested resource already exists."),
        INTERNAL_ERROR => Some("Internal server error."),
        CANCELED => Some("Operation canceled."),
        REMOTE_ERROR => Some("Remote operation failed."),
        LOCAL_ERROR => Some("Local operation failed."),
        PERMISSION_DENIED => Some("Permission denied."),
        _ => None,
    }
}
