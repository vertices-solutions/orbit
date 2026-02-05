// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use async_trait::async_trait;
use std::net::SocketAddr;

use crate::app::errors::{AppError, AppErrorKind, AppResult, codes};
use crate::app::ports::NetworkProbePort;
use crate::app::types::Address;

mod net;
mod reachability;

#[derive(Clone, Default)]
pub struct NetworkAdapter;

impl NetworkAdapter {
    pub fn new() -> Self {
        Self
    }
}

fn map_net_error(err: net::NetError) -> AppError {
    match err {
        net::NetError::DnsNotFound(_) | net::NetError::NoAddrs(_) => {
            AppError::new(AppErrorKind::InvalidArgument, codes::NETWORK_ERROR)
        }
        net::NetError::Resolve(_) => AppError::new(AppErrorKind::Internal, codes::NETWORK_ERROR),
    }
}

#[async_trait]
impl NetworkProbePort for NetworkAdapter {
    #[tracing::instrument(
        name = "network",
        level = "debug",
        skip(self, address),
        fields(op = "resolve_host_addr", address = ?address, port = port)
    )]
    async fn resolve_host_addr(&self, address: &Address, port: u16) -> AppResult<SocketAddr> {
        match address {
            Address::Ip(ip) => Ok(SocketAddr::new(*ip, port)),
            Address::Hostname(host) => net::lookup_first_addr(host, port)
                .await
                .map_err(map_net_error),
        }
    }

    #[tracing::instrument(
        name = "network",
        level = "debug",
        skip(self, address),
        fields(op = "check_host_reachable", address = ?address, port = port)
    )]
    async fn check_host_reachable(&self, address: &Address, port: u16) -> AppResult<bool> {
        reachability::check_host_reachable(address, port)
            .await
            .map_err(map_net_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_net_error_sets_invalid_argument_for_dns_errors() {
        let err = map_net_error(net::NetError::DnsNotFound("example.com".to_string()));
        assert_eq!(err.code(), codes::NETWORK_ERROR);
        assert_eq!(err.kind(), AppErrorKind::InvalidArgument);

        let err = map_net_error(net::NetError::NoAddrs("example.com".to_string()));
        assert_eq!(err.code(), codes::NETWORK_ERROR);
        assert_eq!(err.kind(), AppErrorKind::InvalidArgument);
    }

    #[test]
    fn map_net_error_sets_internal_for_resolve_errors() {
        let io_err = std::io::Error::new(std::io::ErrorKind::Other, "boom");
        let err = map_net_error(net::NetError::Resolve(io_err));
        assert_eq!(err.code(), codes::NETWORK_ERROR);
        assert_eq!(err.kind(), AppErrorKind::Internal);
    }
}
