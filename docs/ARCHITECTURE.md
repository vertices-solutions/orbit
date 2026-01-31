# Overview
This workspace has three crates:
- `orbit` (CLI client) talks to `orbitd` (local daemon) through gRPC over local TCP.
- `proto` holds gRPC/protobuf definitions used by both client and server.
- `orbitd` handles the core functionality (SSH/SFTP, job lifecycle, and persistence).

# Terminology
- Hexagonal architecture: a style where the application core depends only on ports
  (traits) and never on concrete external systems, keeping the domain isolated.
- Port: a trait that defines what the application needs from the outside world
  (e.g., `OrbitdPort`, `InteractionPort`, `FilesystemPort`, `OutputPort`).
- Adapter: a concrete implementation of a port that talks to a specific technology
  (e.g., gRPC, terminal UI, JSON output, filesystem).
- Command: a request object representing a single user intent, suffixed with `Command`
  (e.g., `SubmitJobCommand`, `AddClusterCommand`).
- Handler / Use case: the application logic that executes a command via ports,
  named with `handle_*` functions in `app/handlers`.
- Dispatcher: the router that maps a `Command` request to its handler.
- OutputPort: transforms command result data into user-facing output (tables, JSON).
- StreamOutput: a stream of output events (stdout/stderr/progress/exit) from long-running operations; 
  consumed via `StreamOutputPort`.
- Capture: in-memory storage of stream output (`StreamCapture` / `SubmitCapture`) that powers JSON responses and error handling.
- DTO: a data transfer object used between layers (command requests/results).
- AppError: the unified error type with an `ErrorType`, message, and exit code.
- UiMode: the UI mode (`Interactive` / `NonInteractive`) stored in `AppContext`.

# `orbit` architecture
`orbit` uses a Command + Hexagonal Architecture: a thin CLI adapter builds command
DTOs, a dispatcher routes them to handlers, and handlers call ports that are backed
by adapters.

## Entry points and command flow
- `src/main.rs` is the entrypoint. It parses CLI args, selects `UiMode`, resolves the
  daemon endpoint, constructs adapters, and builds an `AppContext`.
- `adapters/cli` maps clap arguments to `Command` and `*Command` DTOs.
- `app/dispatcher.rs` routes each `Command` to a handler and delegates output to the appropriate method of `OutputPort`.

## Application core
- core lives in `app/`
- `app/commands`: command DTOs and `CommandResult` variants returned by handlers. This is what carries the command-specific information through layers.
- `app/handlers`: use-case orchestration (validate inputs, call ports, shape results). These are dispatched by the dispatcher.
- `app/services`: data handling helpers such as `AddClusterResolver`,
  `PathResolver`, and `SbatchSelector`.
- `app/errors`: `AppError`, `ErrorType`, and helpers for mapping remote errors and exit codes.
- `app/ports`: trait definitions for external dependencies (gRPC, TTY, filesystem,
  config, network).

## Ports and adapters
Ports live in `app/ports`, adapters in `adapters/`:
- `OrbitdPort` -> `adapters/grpc` (`GrpcOrbitdPort`), the tonic gRPC client. `OrbitdPort` is transport-independent.
- `OutputPort` -> `adapters/terminal` (human output) and `adapters/json` (JSON).
- `StreamOutputPort` -> `TerminalStreamOutput` / `JsonStreamOutput`.
- `InteractionPort` -> `TerminalInteraction` (prompts/MFA) and
  `NonInteractiveInteraction` (structured errors).
- `FilesystemPort` -> `adapters/fs::StdFilesystem`.
- `ConfigPort` -> `adapters/config::ConfigAdapter` (reads `orbit.toml`).
- `NetworkPort` -> `adapters/network::StdNetwork` (reachability checks).

## Streaming + MFA
- Streaming RPCs (`submit`, `logs`, `ls`, `cleanup`, `retrieve`, etc.) are handled in
  `adapters/grpc`, which forwards stdout/stderr/status events to `StreamOutputPort`.
- MFA prompts are routed through `InteractionPort`, so the same handler works for
  interactive and non-interactive modes.
- `StreamCapture` and `SubmitCapture` accumulate output for JSON responses and
  error reporting.

## Output and UI mode
- `--non-interactive` sets `UiMode::NonInteractive`, switches output to JSON, and
  disables prompts/MFA via `NonInteractiveInteraction`.
- Interactive mode prints tables, prompts for confirmation/MFA, and streams output
  live to stdout/stderr.
- JSON mode uses a stable envelope:
  - Success: `{ "ok": true, "result": { ... } }`
  - Error: `{ "ok": false, "errorType": "...", "reason": "..." }`
- Exit codes are determined by `ErrorType` (usage, MFA-required, or other).

## Data handling and validation
- `AddClusterResolver` validates destination, discovers identity files, checks
  reachability, and prompts for missing fields in interactive mode.
- `PathResolver` canonicalizes local paths and surfaces validation errors.
- `SbatchSelector` finds `.sbatch` scripts under the submit root and prompts when
  multiple candidates exist (or errors in non-interactive mode).
- `validate_cluster_live` performs a reachability check and a lightweight `ls` RPC
  to confirm connectivity before submit.

## Module layout
```
orbit/src/
  main.rs
  lib.rs
  app/
    mod.rs
    dispatcher.rs
    commands/
    handlers/
    services/
    ports/
    errors.rs
  adapters/
    cli/
    grpc/
    terminal/
    json/
    fs/
    config/
    network/
```

# `orbitd` architecture
TODO: explain in more detail
`orbitd` is a tonic gRPC server with services under `orbitd/src/agent/`.
It stores cluster and job metadata in SQLite via `sqlx`, uses SSH/SFTP logic under
`orbitd/src/ssh/`, and streams MFA prompts and command outputs back to the CLI.
