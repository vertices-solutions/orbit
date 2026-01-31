// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

pub mod commands;
pub mod dispatcher;
pub mod errors;
pub mod handlers;
pub mod ports;
pub mod services;

use std::sync::Arc;

use ports::{FilesystemPort, InteractionPort, NetworkPort, OrbitdPort, OutputPort};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UiMode {
    Interactive,
    NonInteractive,
}

impl UiMode {
    pub fn is_interactive(self) -> bool {
        matches!(self, UiMode::Interactive)
    }
}

#[derive(Clone)]
pub struct AppContext {
    pub ui_mode: UiMode,
    pub orbitd: Arc<dyn OrbitdPort>,
    pub interaction: Arc<dyn InteractionPort>,
    pub output: Arc<dyn OutputPort>,
    pub fs: Arc<dyn FilesystemPort>,
    pub network: Arc<dyn NetworkPort>,
}
