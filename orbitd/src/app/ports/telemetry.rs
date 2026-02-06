// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

#[derive(Clone, Debug, Default)]
pub struct TelemetryEvent {
    pub project: Option<String>,
    pub cluster: Option<String>,
    pub job_id: Option<i64>,
    pub job_name: Option<String>,
    pub user: Option<String>,
    pub remote_path: Option<String>,
}

pub trait TelemetryPort: Send + Sync {
    fn event(&self, name: &'static str, fields: TelemetryEvent);
}

#[derive(Clone, Default)]


// Dummy telemetry port for tests.
pub struct NoopTelemetry;

impl TelemetryPort for NoopTelemetry {
    fn event(&self, _name: &'static str, _fields: TelemetryEvent) {}
}
