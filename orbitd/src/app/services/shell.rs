// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

/// Very small, safe-ish shell escaper for paths.
pub fn sh_escape(p: &str) -> String {
    let mut out = String::from("'");
    out.push_str(&p.replace('\'', r"'\''"));
    out.push('\'');
    out
}
