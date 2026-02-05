// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::env;
use std::path::Path;
use std::sync::OnceLock;

use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::time::UtcTime;
use tracing_subscriber::{EnvFilter, Registry};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

static FILE_GUARD: OnceLock<WorkerGuard> = OnceLock::new();

#[derive(Copy, Clone, Debug)]
enum LogFormat {
    Json,
    Pretty,
    Compact,
}

pub fn init(verbose: bool) {
    let filter = build_filter(verbose);
    let span_events = parse_span_events();
    let format = parse_format();
    let registry = Registry::default()
        .with(filter)
        .with(tracing_error::ErrorLayer::default());

    match format {
        LogFormat::Json => {
            let stdout_layer = tracing_subscriber::fmt::layer()
                .json()
                .with_timer(UtcTime::rfc_3339())
                .with_span_events(span_events.clone());
            if let Some((writer, guard)) = build_file_writer() {
                let file_layer = tracing_subscriber::fmt::layer()
                    .json()
                    .with_ansi(false)
                    .with_timer(UtcTime::rfc_3339())
                    .with_span_events(span_events)
                    .with_writer(writer);
                registry.with(stdout_layer).with(file_layer).init();
                let _ = FILE_GUARD.set(guard);
            } else {
                registry.with(stdout_layer).init();
            }
        }
        LogFormat::Pretty => {
            let stdout_layer = tracing_subscriber::fmt::layer()
                .pretty()
                .with_timer(UtcTime::rfc_3339())
                .with_span_events(span_events.clone());
            if let Some((writer, guard)) = build_file_writer() {
                let file_layer = tracing_subscriber::fmt::layer()
                    .pretty()
                    .with_ansi(false)
                    .with_timer(UtcTime::rfc_3339())
                    .with_span_events(span_events)
                    .with_writer(writer);
                registry.with(stdout_layer).with(file_layer).init();
                let _ = FILE_GUARD.set(guard);
            } else {
                registry.with(stdout_layer).init();
            }
        }
        LogFormat::Compact => {
            let stdout_layer = tracing_subscriber::fmt::layer()
                .compact()
                .with_timer(UtcTime::rfc_3339())
                .with_span_events(span_events.clone());
            if let Some((writer, guard)) = build_file_writer() {
                let file_layer = tracing_subscriber::fmt::layer()
                    .compact()
                    .with_ansi(false)
                    .with_timer(UtcTime::rfc_3339())
                    .with_span_events(span_events)
                    .with_writer(writer);
                registry.with(stdout_layer).with(file_layer).init();
                let _ = FILE_GUARD.set(guard);
            } else {
                registry.with(stdout_layer).init();
            }
        }
    }
}

fn build_filter(verbose: bool) -> EnvFilter {
    match env::var("ORBIT_LOG") {
        Ok(value) => EnvFilter::new(value),
        Err(_) => {
            if verbose {
                EnvFilter::new("debug")
            } else {
                EnvFilter::new("info")
            }
        }
    }
}

fn parse_format() -> LogFormat {
    match env::var("ORBIT_LOG_FORMAT")
        .ok()
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase())
    {
        Some(value) if value == "json" => LogFormat::Json,
        Some(value) if value == "pretty" => LogFormat::Pretty,
        Some(value) if value == "compact" => LogFormat::Compact,
        _ => LogFormat::Compact,
    }
}

fn parse_span_events() -> FmtSpan {
    match env::var("ORBIT_LOG_SPAN_EVENTS")
        .ok()
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase())
    {
        Some(value) if value == "new" => FmtSpan::NEW,
        Some(value) if value == "enter" => FmtSpan::ENTER,
        Some(value) if value == "exit" => FmtSpan::EXIT,
        Some(value) if value == "close" => FmtSpan::CLOSE,
        _ => FmtSpan::NONE,
    }
}

fn build_file_writer() -> Option<(NonBlocking, WorkerGuard)> {
    let file_path = env::var("ORBIT_LOG_FILE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())?;

    let path = Path::new(&file_path);
    let file_name = path.file_name()?.to_string_lossy().to_string();
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let appender = tracing_appender::rolling::never(dir, file_name);
    let (writer, guard) = tracing_appender::non_blocking(appender);
    Some((writer, guard))
}
