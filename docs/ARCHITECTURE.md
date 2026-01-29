# Overview
This workspace has three crates:
- `orbit` (CLI client) talks to `orbitd` (local daemon) through gRPC over local TCP.
- `proto` holds gRPC/protobuf definitions used by both client and server.
- `orbitd` handles the core functionality (SSH/SFTP, job lifecycle, and persistence).

# `orbit` architecture (current)
`orbit` is implemented as a clap-based CLI that dispatches into gRPC client calls and
prints either human-friendly output or JSON.

## Entry points and command flow
- `src/main.rs` is the entrypoint. It parses CLI args (`args.rs`), loads config, builds
  submit filters, and then branches into:
  - interactive flow (default): prints tables / prompts / MFA.
  - non-interactive flow (`--non-interactive`): JSON-only output, no prompts.
- `src/args.rs` defines the clap model (`Cmd`, `JobCmd`, `ClusterCmd`, etc).
- `src/config.rs` loads the TOML config and constructs the daemon endpoint.

## gRPC client layer
- `src/client.rs` builds gRPC requests and handles streaming responses.
- For most operations there are two variants:
  - `send_*` functions for interactive output.
  - `send_*_capture` functions for non-interactive output (`CapturedStream` / `SubmitCapture`).
- `validate_cluster_live` performs a local TCP reachability check and a lightweight
  streaming RPC (`ls`) to validate remote connectivity.

## Streaming + MFA
- `src/stream.rs` owns stream event handling, spinners, and output routing.
  It has "live" handlers (print as events arrive) and capture handlers (accumulate bytes).
- `src/mfa.rs` prompts for MFA responses in interactive mode; in non-interactive mode
  it returns a structured error (`NonInteractiveError`).

## Interactive prompting and UI
- `src/interactive.rs` provides TTY prompts and validation logic for cluster add,
  confirmation prompts, and default-path handling.
- `src/sbatch.rs` provides an interactive TUI picker for `.sbatch` scripts when more
  than one candidate exists; in non-interactive mode it requires `--sbatchscript`.
- `src/interaction.rs` stores a process-wide `AtomicBool` used to switch between
  interactive and non-interactive behavior.

## Formatting and error presentation
- `src/format.rs` renders tables and JSON for clusters and jobs.
- `src/non_interactive.rs` defines structured JSON envelopes and error codes.
- `src/errors.rs` converts gRPC status codes to user-friendly messages.
- `src/filters.rs` builds ordered include/exclude filters for submit.

## Current separations and duplication
- There are two execution paths in `main.rs` (interactive vs non-interactive) with
  duplicated command logic.
- Many gRPC client functions exist in both live and capture forms (`send_*` vs
  `send_*_capture`), which spreads mode-specific logic across multiple files.
- The `interaction` global flag is consulted by `interactive`, `sbatch`, `mfa`,
  and `stream` to decide whether to prompt or error.

# `orbitd` architecture (current)
`orbitd` is a tonic gRPC server with services under `orbitd/src/agent/`.
It stores cluster and job metadata in SQLite via `sqlx`, uses SSH/SFTP logic under
`orbitd/src/ssh/`, and streams MFA prompts and command outputs back to the CLI.
