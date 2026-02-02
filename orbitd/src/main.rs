// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use log::LevelFilter;
use proto::agent_server::AgentServer;
use std::net::{Ipv4Addr, SocketAddr};
use tokio::time::Duration;
use tonic::transport::Server;

mod adapters;
mod app;
mod config;

fn init_logging(verbose: bool) {
    let mut builder = env_logger::builder();
    builder.format_timestamp_secs();
    if verbose {
        builder.filter_level(LevelFilter::Debug);
    } else {
        builder
            .filter_level(LevelFilter::Off)
            .filter_module("orbitd", LevelFilter::Info);
    }
    builder.init();
}

fn log_config_report(report: &config::ConfigReport) {
    match (&report.config_path, report.config_path_source) {
        (Some(path), Some(source)) => {
            log::info!(
                "config path: {} (source={}, present={})",
                path.display(),
                source.as_str(),
                report.config_file_present
            );
        }
        (Some(path), None) => {
            log::info!(
                "config path: {} (present={})",
                path.display(),
                report.config_file_present
            );
        }
        (None, _) => {
            log::info!("config path: (none)");
        }
    }
    log::info!(
        "config database_path: {} (source={})",
        report.database_path.value.display(),
        report.database_path.source.as_str()
    );
    log::info!(
        "config job_check_interval_secs: {} (source={})",
        report.job_check_interval_secs.value,
        report.job_check_interval_secs.source.as_str()
    );
    log::info!(
        "config port: {} (source={})",
        report.port.value,
        report.port.source.as_str()
    );
    log::info!(
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
        },
    )?;
    init_logging(config.verbose);
    log_config_report(&report);
    config::ensure_database_dir(&config.database_path)?;
    let db = adapters::db::HostStore::open(&config.database_path).await?;
    let server_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, config.port));

    let store_adapter = adapters::db::SqliteStoreAdapter::new(db);
    let store = std::sync::Arc::new(store_adapter);
    let ssh_adapter = std::sync::Arc::new(adapters::ssh::SshAdapter::with_defaults());
    let local_fs = std::sync::Arc::new(adapters::fs::LocalFilesystem::new());
    let network = std::sync::Arc::new(adapters::network::NetworkAdapter::new());
    let clock = std::sync::Arc::new(adapters::time::SystemClock::new());

    let usecases = app::usecases::UseCases::new(
        store.clone(),
        store.clone(),
        store.clone(),
        ssh_adapter.clone(),
        ssh_adapter.clone(),
        local_fs,
        network,
        clock,
    );
    let checker = usecases.clone();
    let interval = Duration::from_secs(config.job_check_interval_secs);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            if let Err(err) = checker.check_running_jobs(interval).await {
                log::warn!("job check failed: {err}");
            }
        }
    });

    let svc = adapters::grpc::GrpcAgent::new(usecases);
    log::info!("server listening on {}", server_addr);
    Server::builder()
        .add_service(AgentServer::new(svc))
        .serve(server_addr)
        .await?;
    Ok(())
}
