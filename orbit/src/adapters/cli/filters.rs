// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use clap::ArgMatches;
use proto::{RunPathFilterAction, RunPathFilterRule};

pub(super) fn run_filters_from_matches(matches: &ArgMatches) -> Vec<RunPathFilterRule> {
    let sub_matches = match matches.subcommand() {
        Some(("run", scope)) => scope,
        Some(("job", scope)) => match scope.subcommand() {
            Some(("run", run)) => run,
            _ => return Vec::new(),
        },
        Some(("blueprint", scope)) => match scope.subcommand() {
            Some(("run", run)) => run,
            _ => return Vec::new(),
        },
        _ => return Vec::new(),
    };

    let mut ordered: Vec<(usize, RunPathFilterAction, String)> = Vec::new();
    let mut push_rules = |arg: &str, action: RunPathFilterAction| {
        let values: Vec<String> = sub_matches
            .get_many::<String>(arg)
            .map(|vals| vals.map(|v| v.to_string()).collect())
            .unwrap_or_default();
        let indices: Vec<usize> = sub_matches
            .indices_of(arg)
            .map(|vals| vals.collect())
            .unwrap_or_default();
        for (idx, pattern) in indices.into_iter().zip(values.into_iter()) {
            ordered.push((idx, action, pattern));
        }
    };

    push_rules("include", RunPathFilterAction::Include);
    push_rules("exclude", RunPathFilterAction::Exclude);

    ordered.sort_by_key(|(idx, _, _)| *idx);
    ordered
        .into_iter()
        .map(|(_, action, pattern)| RunPathFilterRule {
            action: action as i32,
            pattern,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::cli::Cli;
    use clap::CommandFactory;

    #[test]
    fn run_filters_preserve_flag_order_for_job_run() {
        let matches = Cli::command().get_matches_from([
            "orbit",
            "job",
            "run",
            "--on",
            "cluster-a",
            "./project",
            "--include",
            "a",
            "--exclude",
            "b",
            "--include",
            "c",
        ]);
        let filters = run_filters_from_matches(&matches);
        let patterns: Vec<&str> = filters.iter().map(|f| f.pattern.as_str()).collect();
        let actions: Vec<i32> = filters.iter().map(|f| f.action).collect();

        assert_eq!(patterns, vec!["a", "b", "c"]);
        assert_eq!(
            actions,
            vec![
                RunPathFilterAction::Include as i32,
                RunPathFilterAction::Exclude as i32,
                RunPathFilterAction::Include as i32
            ]
        );
    }

    #[test]
    fn run_filters_preserve_flag_order_for_blueprint_run() {
        let matches = Cli::command().get_matches_from([
            "orbit",
            "blueprint",
            "run",
            "proj-a",
            "--on",
            "cluster-a",
            "--exclude",
            "tmp/**",
            "--include",
            "src/**",
        ]);
        let filters = run_filters_from_matches(&matches);
        let patterns: Vec<&str> = filters.iter().map(|f| f.pattern.as_str()).collect();
        let actions: Vec<i32> = filters.iter().map(|f| f.action).collect();

        assert_eq!(patterns, vec!["tmp/**", "src/**"]);
        assert_eq!(
            actions,
            vec![
                RunPathFilterAction::Exclude as i32,
                RunPathFilterAction::Include as i32
            ]
        );
    }

    #[test]
    fn run_filters_preserve_flag_order_for_top_level_run() {
        let matches = Cli::command().get_matches_from([
            "orbit",
            "run",
            "./project",
            "--on",
            "cluster-a",
            "--exclude",
            "target/**",
            "--include",
            "src/**",
        ]);
        let filters = run_filters_from_matches(&matches);
        let patterns: Vec<&str> = filters.iter().map(|f| f.pattern.as_str()).collect();
        assert_eq!(patterns, vec!["target/**", "src/**"]);
    }
}
