// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

pub struct RemotePathFailure<'a> {
    pub remote_path: &'a str,
    pub reason: &'static str,
}

const REMOTE_PATH_IN_USE_REASON: &str = "in use by another job";
const REMOTE_PATH_IN_USE_INFIX: &str = " is still running in ";
const REMOTE_PATH_IN_USE_SUFFIX: &str = "; use --force to submit anyway";

pub fn parse_remote_path_failure(message: &str) -> Option<RemotePathFailure<'_>> {
    parse_remote_path_in_use(message).map(|remote_path| RemotePathFailure {
        remote_path,
        reason: REMOTE_PATH_IN_USE_REASON,
    })
}

fn parse_remote_path_in_use(message: &str) -> Option<&str> {
    if !message.starts_with("job ") {
        return None;
    }
    let (_, rest) = message.split_once(REMOTE_PATH_IN_USE_INFIX)?;
    let (remote_path, _) = rest.split_once(REMOTE_PATH_IN_USE_SUFFIX)?;
    let remote_path = remote_path.trim();
    if remote_path.is_empty() {
        None
    } else {
        Some(remote_path)
    }
}

#[cfg(test)]
mod tests {
    use super::{REMOTE_PATH_IN_USE_REASON, parse_remote_path_failure, parse_remote_path_in_use};

    #[test]
    fn parse_remote_path_in_use_extracts_path() {
        let message = "job 42 is still running in /scratch/run; use --force to submit anyway";
        assert_eq!(parse_remote_path_in_use(message), Some("/scratch/run"));
    }

    #[test]
    fn parse_remote_path_failure_maps_reason() {
        let message = "job 7 is still running in /data/run; use --force to submit anyway";
        let failure = parse_remote_path_failure(message).expect("expected failure");
        assert_eq!(failure.remote_path, "/data/run");
        assert_eq!(failure.reason, REMOTE_PATH_IN_USE_REASON);
    }

    #[test]
    fn parse_remote_path_in_use_rejects_non_matching_message() {
        let message = "conflict";
        assert!(parse_remote_path_in_use(message).is_none());
    }
}
