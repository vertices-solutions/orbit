// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use clap::{CommandFactory, FromArgMatches};
use clap_complete::generate;
use orbit::adapters;
use orbit::app::{AppContext, UiMode};
use orbit::app::dispatcher::Dispatcher;
use orbit::app::ports::{ConfigPort, FilesystemPort, InteractionPort, NetworkPort, OutputPort};
use orbit::args::{Cli, Cmd};
use std::sync::Arc;

const HELP_TEMPLATE: &str = r#"██████╗ ██████╗ ██████╗ ██╗ ████████╗
██╔══██╗██╔══██╗██╔══██╗██║ ╚══██╔══╝
██║  ██║██████╔╝██████╔╝██║    ██║
██║  ██║██╔══██╗██╔══██╗██║    ██║
╚█████╔╝██║  ██║██████╔╝██║    ██║
 ╚════╝ ╚═╝  ╚═╝╚═════╝ ╚═╝    ╚═╝

{before-help}{about-with-newline}{usage-heading} {usage}

{all-args}{after-help}
"#;

fn apply_help_template_recursively(cmd: &mut clap::Command) {
    let mut owned = std::mem::take(cmd); //take to avoid memory allocation
    owned = owned.help_template(HELP_TEMPLATE);
    for sub in owned.get_subcommands_mut() {
        apply_help_template_recursively(sub);
    }
    *cmd = owned;
}

fn generate_shell_completions(shell: clap_complete::Shell) {
    let mut cmd = Cli::command();
    apply_help_template_recursively(&mut cmd);
    let bin_name = cmd.get_name().to_owned();
    generate(shell, &mut cmd, bin_name, &mut std::io::stdout());
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut cmd = Cli::command();
    apply_help_template_recursively(&mut cmd);
    let matches = cmd.get_matches();
    let cli = Cli::from_arg_matches(&matches).unwrap_or_else(|err| err.exit());
    if let Cmd::Completions(args) = &cli.cmd {
        generate_shell_completions(args.shell);
        return Ok(());
    }

    let ui_mode = if cli.non_interactive {
        UiMode::NonInteractive
    } else {
        UiMode::Interactive
    };

    let config_path = cli.config.clone();
    let command = adapters::cli::command_from_cli(cli, &matches);

    let config_adapter = adapters::config::ConfigAdapter;
    let daemon_endpoint = config_adapter
        .daemon_endpoint(config_path)
        .map_err(|err| anyhow::anyhow!(err.message))?;

    let orbitd = Arc::new(adapters::grpc::GrpcOrbitdPort::new(daemon_endpoint));
    let output: Arc<dyn OutputPort> = match ui_mode {
        UiMode::Interactive => Arc::new(adapters::terminal::TerminalOutput::new()),
        UiMode::NonInteractive => Arc::new(adapters::json::JsonOutput::new()),
    };
    let interaction: Arc<dyn InteractionPort> = match ui_mode {
        UiMode::Interactive => Arc::new(adapters::terminal::TerminalInteraction::new()),
        UiMode::NonInteractive => Arc::new(adapters::json::NonInteractiveInteraction::new()),
    };
    let fs: Arc<dyn FilesystemPort> = Arc::new(adapters::fs::StdFilesystem);
    let network: Arc<dyn NetworkPort> = Arc::new(adapters::network::StdNetwork);

    let ctx = AppContext {
        ui_mode,
        orbitd,
        interaction,
        output,
        fs,
        network,
    };
    let dispatcher = Dispatcher::new(ctx);
    let exit_code = dispatcher
        .dispatch(command)
        .await
        .map_err(|err| anyhow::anyhow!(err.message))?;
    if exit_code != 0 {
        std::process::exit(exit_code);
    }
    Ok(())
}
