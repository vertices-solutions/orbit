// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

/// Very small, safe-ish shell escaper for paths.
pub fn sh_escape(p: &str) -> String {
    let mut out = String::from("'");
    out.push_str(&p.replace('\'', r"'\''"));
    out.push('\'');
    out
}

#[cfg(test)]
mod tests {
    use super::sh_escape;

    #[test]
    fn sh_escape_wraps_plain_paths_in_single_quotes() {
        assert_eq!(sh_escape("/tmp/hello world"), "'/tmp/hello world'");
    }

    #[test]
    fn sh_escape_escapes_embedded_single_quotes() {
        assert_eq!(sh_escape("a'b"), "'a'\\''b'");
    }
}
