// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use proto::agent_server::AgentServer;
use std::net::{Ipv4Addr, SocketAddr};
use tokio::time::Duration;
use tonic::transport::Server;

mod adapters;
mod app;
mod config;
mod logging;

fn log_config_report(report: &config::ConfigReport) {
    match (&report.config_path, report.config_path_source) {
        (Some(path), Some(source)) => {
            tracing::info!(
                "config path: {} (source={}, present={})",
                path.display(),
                source.as_str(),
                report.config_file_present
            );
        }
        (Some(path), None) => {
            tracing::info!(
                "config path: {} (present={})",
                path.display(),
                report.config_file_present
            );
        }
        (None, _) => {
            tracing::info!("config path: (none)");
        }
    }
    tracing::info!(
        "config database_path: {} (source={})",
        report.database_path.value.display(),
        report.database_path.source.as_str()
    );
    tracing::info!(
        "config job_check_interval_secs: {} (source={})",
        report.job_check_interval_secs.value,
        report.job_check_interval_secs.source.as_str()
    );
    tracing::info!(
        "config tarballs_dir: {} (source={})",
        report.tarballs_dir.value.display(),
        report.tarballs_dir.source.as_str()
    );
    tracing::info!(
        "config port: {} (source={})",
        report.port.value,
        report.port.source.as_str()
    );
    tracing::info!(
        "config verbose: {} (source={})",
        report.verbose.value,
        report.verbose.source.as_str()
    );
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let parsed = adapters::cli::parse_opts();
    let opts = parsed.opts;
    let verbose_override = parsed.verbose_override;
    let config::LoadResult { config, report } = config::load_with_report(
        opts.config,
        config::Overrides {
            database_path: opts.database_path,
            job_check_interval_secs: opts.job_check_interval_secs,
            port: opts.port,
            verbose: verbose_override,
            tarballs_dir: None,
        },
    )?;
    logging::init(config.verbose);
    log_config_report(&report);
    config::ensure_database_dir(&config.database_path)?;
    config::ensure_tarballs_dir(&config.tarballs_dir)?;
    let db = adapters::db::HostStore::open(&config.database_path).await?;
    let server_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, config.port));

    let store_adapter = adapters::db::SqliteStoreAdapter::new(db);
    let store = std::sync::Arc::new(store_adapter);
    let ssh_adapter = std::sync::Arc::new(adapters::ssh::SshAdapter::with_defaults());
    let local_fs = std::sync::Arc::new(adapters::fs::LocalFilesystem::new());
    let network = std::sync::Arc::new(adapters::network::NetworkAdapter::new());
    let clock = std::sync::Arc::new(adapters::time::SystemClock::new());
    let telemetry = std::sync::Arc::new(adapters::telemetry::TracingTelemetry::new());

    let usecases = app::usecases::UseCases::new(
        store.clone(),
        store.clone(),
        store.clone(),
        ssh_adapter.clone(),
        ssh_adapter.clone(),
        local_fs,
        network,
        clock,
        telemetry,
        config.tarballs_dir.clone(),
    );
    let checker = usecases.clone();
    let interval = Duration::from_secs(config.job_check_interval_secs);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            if let Err(err) = checker.check_running_jobs(interval).await {
                tracing::warn!("job check failed: {err}");
            }
        }
    });

    let svc = adapters::grpc::GrpcAgent::new(usecases);
    tracing::info!("server listening on {}", server_addr);
    Server::builder()
        .add_service(AgentServer::new(svc))
        .serve(server_addr)
        .await?;
    Ok(())
}
