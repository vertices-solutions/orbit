// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::sync::atomic::{AtomicBool, Ordering};

static NON_INTERACTIVE: AtomicBool = AtomicBool::new(false);

pub fn set_non_interactive(value: bool) {
    NON_INTERACTIVE.store(value, Ordering::SeqCst);
}

pub fn is_non_interactive() -> bool {
    NON_INTERACTIVE.load(Ordering::SeqCst)
}
