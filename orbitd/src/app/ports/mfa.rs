// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use proto::MfaAnswer;
use tokio::sync::mpsc;

/// MFA input boundary for the core.
/// Exposes a receiver for MFA answers independent of transport.
/// Note: this brings the MFA answers from the client back
pub trait MfaPort: Send {
    fn receiver(&mut self) -> &mut mpsc::Receiver<MfaAnswer>;
}
