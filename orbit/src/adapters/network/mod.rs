// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::app::errors::{AppError, AppResult};
use crate::app::ports::NetworkPort;

const CHECK_CONNECT_TIMEOUT_SECS: u64 = 3;

pub struct StdNetwork;

impl NetworkPort for StdNetwork {
    fn check_reachable(&self, host: &str, port: u16) -> AppResult<()> {
        let mut addrs = (host, port)
            .to_socket_addrs()
            .map_err(|_| AppError::network_error("destination host could not be resolved"))?;
        let mut resolved = false;
        let timeout = Duration::from_secs(CHECK_CONNECT_TIMEOUT_SECS);
        while let Some(addr) = addrs.next() {
            resolved = true;
            if TcpStream::connect_timeout(&addr, timeout).is_ok() {
                return Ok(());
            }
        }
        if !resolved {
            return Err(AppError::network_error("destination host could not be resolved"));
        }
        Err(AppError::network_error("destination host is unreachable"))
    }
}
