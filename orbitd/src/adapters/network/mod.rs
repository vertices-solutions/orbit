// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use async_trait::async_trait;
use std::net::SocketAddr;

use crate::app::errors::{codes, AppError, AppErrorKind, AppResult};
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
    async fn resolve_host_addr(&self, address: &Address, port: u16) -> AppResult<SocketAddr> {
        match address {
            Address::Ip(ip) => Ok(SocketAddr::new(*ip, port)),
            Address::Hostname(host) => net::lookup_first_addr(host, port)
                .await
                .map_err(map_net_error),
        }
    }

    async fn check_host_reachable(&self, address: &Address, port: u16) -> AppResult<bool> {
        reachability::check_host_reachable(address, port)
            .await
            .map_err(map_net_error)
    }
}
