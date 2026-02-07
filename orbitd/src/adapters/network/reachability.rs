// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use super::net::{self, NetError};
use crate::app::types::Address;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;

const CONNECT_TIMEOUT_SECS: u64 = 3;

pub async fn check_host_reachable(address: &Address, port: u16) -> Result<bool, NetError> {
    let timeout = Duration::from_secs(CONNECT_TIMEOUT_SECS);
    match address {
        Address::Ip(ip) => Ok(is_socket_reachable(SocketAddr::new(*ip, port), timeout).await),
        Address::Hostname(host) => {
            let addrs = net::lookup_addrs(host, port).await?;
            for addr in addrs {
                if is_socket_reachable(addr, timeout).await {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

async fn is_socket_reachable(addr: SocketAddr, timeout_duration: Duration) -> bool {
    match timeout(timeout_duration, TcpStream::connect(addr)).await {
        Ok(Ok(_stream)) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::types::Address;
    use std::net::{IpAddr, Ipv4Addr};

    #[tokio::test]
    async fn is_socket_reachable_returns_false_when_connection_fails() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), 65535);
        let reachable = is_socket_reachable(addr, Duration::from_millis(250)).await;
        assert!(!reachable);
    }

    #[tokio::test]
    async fn check_host_reachable_returns_false_for_unreachable_ip() {
        let result =
            check_host_reachable(&Address::Ip(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1))), 65535)
                .await;
        assert_eq!(result.expect("lookup should succeed"), false);
    }

    #[tokio::test]
    async fn check_host_reachable_returns_false_for_hostname_with_unreachable_port() {
        let result = check_host_reachable(&Address::Hostname("localhost".to_string()), 0).await;
        assert_eq!(result.expect("hostname should resolve"), false);
    }

    #[tokio::test]
    async fn check_host_reachable_returns_error_for_unknown_hostname() {
        let result = check_host_reachable(
            &Address::Hostname("orbitd-should-not-exist.invalid".to_string()),
            22,
        )
        .await;
        assert!(matches!(
            result,
            Err(NetError::DnsNotFound(_)) | Err(NetError::Resolve(_))
        ));
    }
}
