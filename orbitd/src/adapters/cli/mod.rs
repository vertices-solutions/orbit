// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use std::path::PathBuf;

use clap::{CommandFactory, FromArgMatches, Parser};

#[derive(Parser)]
#[command(
    name = "orbitd",
    version,
    about,
    long_about = None,
    after_help = "orbitd server\n\
\n\
Configuration precedence: defaults < config file < command-line flags.\n\
Config path precedence: defaults < ORBIT_CONFIG_PATH < command-line flags.\n\
If --config is omitted, orbitd tries ORBIT_CONFIG_PATH, then the default config file location; missing default config is OK.\n\
Paths in the config file are resolved relative to the config file directory; paths passed as flags are resolved relative to the current working directory."
)]
pub struct Opts {
    #[arg(
        short,
        long,
        value_name = "PATH",
        help = "Path to a TOML config file. When omitted, orbitd uses ORBIT_CONFIG_PATH if set, otherwise the default config file location if available."
    )]
    pub config: Option<PathBuf>,
    #[arg(
        long,
        value_name = "PATH",
        help = "Path to the SQLite database file. Overrides `database_path` from the config file."
    )]
    pub database_path: Option<PathBuf>,
    #[arg(
        long,
        value_name = "SECS",
        help = "How often to check remote job states. Overrides `job_check_interval_secs` from the config file."
    )]
    pub job_check_interval_secs: Option<u64>,
    #[arg(
        short,
        long,
        action = clap::ArgAction::SetTrue,
        help = "Enable debug logging and include logs from dependencies. Overrides `verbose` from the config file."
    )]
    pub verbose: bool,
    #[arg(
        long,
        value_name = "PORT",
        help = "Port to bind the daemon on. Overrides `port` from the config file."
    )]
    pub port: Option<u16>,
}

pub struct ParsedOpts {
    pub opts: Opts,
    pub verbose_override: Option<bool>,
}

const HELP_TEMPLATE: &str = r#"██████╗ ██████╗ ██████╗ ██╗ ████████╗
██╔══██╗██╔══██╗██╔══██╗██║ ╚══██╔══╝
██║  ██║██████╔╝██████╔╝██║    ██║
██║  ██║██╔══██╗██╔══██╗██║    ██║
╚█████╔╝██║  ██║██████╔╝██║    ██║
 ╚════╝ ╚═╝  ╚═╝╚═════╝ ╚═╝    ╚═╝

{before-help}{about-with-newline}{usage-heading} {usage}
{after-help}

{all-args}
"#;

fn apply_help_template_recursively(cmd: &mut clap::Command) {
    let mut owned = std::mem::take(cmd);
    owned = owned.help_template(HELP_TEMPLATE);
    for sub in owned.get_subcommands_mut() {
        apply_help_template_recursively(sub);
    }
    *cmd = owned;
}

pub fn cli_command() -> clap::Command {
    let mut cmd = Opts::command();
    apply_help_template_recursively(&mut cmd);
    cmd
}

pub fn parse_opts() -> ParsedOpts {
    let mut cmd = cli_command();
    let matches = cmd.get_matches();
    let verbose_override = if matches.get_flag("verbose") {
        Some(true)
    } else {
        None
    };
    let opts = Opts::from_arg_matches(&matches).unwrap_or_else(|err| err.exit());
    ParsedOpts {
        opts,
        verbose_override,
    }
}
