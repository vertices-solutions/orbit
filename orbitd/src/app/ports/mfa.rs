// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use proto::MfaAnswer;
use tokio::sync::mpsc;

pub trait MfaPort: Send {
    fn receiver(&mut self) -> &mut mpsc::Receiver<MfaAnswer>;
}
