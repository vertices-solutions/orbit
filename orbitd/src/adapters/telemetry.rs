// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use crate::app::ports::{TelemetryEvent, TelemetryPort};

#[derive(Clone, Default)]
pub struct TracingTelemetry;

impl TracingTelemetry {
    pub fn new() -> Self {
        Self
    }
}

impl TelemetryPort for TracingTelemetry {
    fn event(&self, name: &'static str, fields: TelemetryEvent) {
        let TelemetryEvent {
            project,
            cluster,
            job_id,
            job_name,
            user,
            remote_path,
        } = fields;

        tracing::info!(
            target: "orbitd::telemetry",
            event = name,
            project = project.as_deref(),
            cluster = cluster.as_deref(),
            job_id = job_id,
            job_name = job_name.as_deref(),
            user = user.as_deref(),
            remote_path = remote_path.as_deref(),
        );
    }
}
