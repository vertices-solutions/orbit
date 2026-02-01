// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use time::OffsetDateTime;

/// Time source boundary for UTC timestamps.
/// Makes time-dependent logic deterministic and testable.
pub trait ClockPort: Send + Sync {
    fn now_utc(&self) -> OffsetDateTime;
}
