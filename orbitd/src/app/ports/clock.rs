// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use time::OffsetDateTime;

pub trait ClockPort: Send + Sync {
    fn now_utc(&self) -> OffsetDateTime;
}
