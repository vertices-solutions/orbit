// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use chrono::Local;
use rand::Rng;

pub fn generate_run_directory_name() -> String {
    // Get current local date in YYYY-MM-DD
    let date = Local::now().format("%Y-%m-%d").to_string();

    // Generate 10 random lowercase latin letters
    let mut rng = rand::rng();
    let rand_string: String = (0..10)
        .map(|_| {
            let idx = rng.random_range(0..26); // 0..=25
            (b'a' + idx) as char
        })
        .collect();

    // Combine as "YYYY-MM-DD-xxxxxxxxxx"
    format!("{}-{}", date, rand_string)
}

#[cfg(test)]
mod tests {
    use super::generate_run_directory_name;

    #[test]
    fn generate_run_directory_name_has_expected_shape() {
        let value = generate_run_directory_name();
        let bytes = value.as_bytes();

        assert_eq!(value.len(), 21);
        assert_eq!(bytes[4], b'-');
        assert_eq!(bytes[7], b'-');
        assert_eq!(bytes[10], b'-');

        for ch in value[0..4].chars() {
            assert!(ch.is_ascii_digit());
        }
        for ch in value[5..7].chars() {
            assert!(ch.is_ascii_digit());
        }
        for ch in value[8..10].chars() {
            assert!(ch.is_ascii_digit());
        }
        for ch in value[11..].chars() {
            assert!(ch.is_ascii_lowercase());
        }
    }
}
