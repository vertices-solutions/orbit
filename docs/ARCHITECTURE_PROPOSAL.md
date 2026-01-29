# Orbit Architecture Proposal (Command + Hexagonal)

## Scope
This proposal covers the `orbit` CLI crate only. `orbitd` and `proto` remain unchanged.

## Goals
- Use a clear Command pattern: one command request -> one handler/use case.
- Adopt Hexagonal Architecture: core use cases depend on ports, not adapters.
- Eliminate separate interactive vs non-interactive entrypoints.
- Keep data handling (validation, path resolution, selection) out of use cases.
- Make command logic unit-testable with fake ports.

## Non-goals
- Changing `orbitd` behavior or the protobuf contract.
- Redesigning the CLI UX or flag structure

## Terminology
- Hexagonal architecture: a style where the application core depends only on ports
  (interfaces) and never on concrete external systems, keeping the domain isolated.
- Port: a trait that defines what the application needs from the outside world
  (e.g., `OrbitdPort`, `InteractionPort`, `FilesystemPort`).
- Adapter: a concrete implementation of a port that talks to a specific technology
  (e.g., gRPC, terminal UI, JSON output).
- Command: a request object representing a single user intent, suffixed with `Command`
  (e.g., `SubmitJobCommand`).
- Handler / Use case: the application logic that executes a command via ports,
  named with `Handler` (e.g., `SubmitJobHandler`).
- Dispatcher: the router that maps a command request to its handler.
- OutputPort: transforms command result data into user-facing output (tables, JSON).
- StreamOutput: a stream of output events (stdout/stderr/progress/exit) from long-running
  operations; consumed via `StreamOutputPort`.
- DTO: a data transfer object used between layers (command requests/results).
- DomainError: errors from domain logic and validation rules.
- UseCaseError: orchestration errors in handlers (e.g., missing port data, failed calls).
- Golden tests: tests that run code, capture its output and compare it against a saved "golden" output. 

## Current pain points (summary)
- `main.rs` contains two execution paths with duplicated command logic.
- gRPC client functions are duplicated (`send_*` vs `send_*_capture`).
- Mode-specific behavior is scattered across modules via a global `interaction` flag.
- Output formatting and error shaping happen in the entrypoint, not in a presenter.

## Proposed Architecture
### High-level layering (Hexagonal)
```
[ CLI Adapter ]
      |
      v
[ Command Dispatcher ]  <-- application boundary
      |
      v
[ Use Cases / Command Handlers ]
      |
      v
[ Ports ]  <---->  [ Adapters (gRPC, FS, TTY, JSON, etc.) ]
```

### Command pattern
- Each CLI command maps to a `*Command` struct (e.g., `SubmitJobCommand`). Note the naming order: it should remain human readable, i.e. `AddClusterCommand` should be preferred over `ClusterAddCommand`.
- Each command is handled by one `*Handler` that returns a command-specific `*Result`.
- The dispatcher routes `*Command` -> handler.

### Ports (interfaces)
Define trait-based ports at the application boundary:
- `OrbitdPort`: submit, list, logs, cancel, cleanup, etc.
- `StreamOutputPort`: consume `StreamOutput` events.
- `InteractionPort`: prompt/confirm/choose/MFA (can error when non-interactive).
- `FilesystemPort`: path canonicalization, file existence checks.
- `ConfigPort`: daemon endpoint resolution.
- `ClockPort` (optional): timestamps for progress or timeouts.
- `NetworkPort` (optional): reachability checks (used by validate cluster).

### Adapters
- `adapters/grpc`: implements `OrbitdPort` using tonic + existing protobuf types.
- `adapters/cli`: clap parsing -> `*Command`.
- `adapters/terminal`: interactive prompts, spinners, sbatch picker.
- `adapters/json`: non-interactive output + error shaping.
- `adapters/fs`: std::fs / std::path helpers.

## Unifying interactive and non-interactive modes
Replace the global `interaction` flag with a `UiMode` stored in `AppContext`.

### InteractionPort behavior
- Interactive adapter: prompts user, returns values.
- Non-interactive adapter: returns structured errors:
  - `MissingInput`
  - `ConfirmationRequired`
  - `MfaRequired`

### Output handling
In the result of refactoring, adapters decide how to present, and command handlers stay mode-agnostic.

`--non-interactive` is the sole switch for JSON output and for disabling prompts.
Per-command `--json` flags should be removed.

Use a single `OutputPort` with two implementations:
- Human output: tables, prompts, spinners.
- JSON output: structured JSON envelope.

Streaming commands report events to `StreamOutputPort`. The port decides whether to:
- Print live output (interactive), or
- Capture and return serialized output (non-interactive).


#### JSON envelope
- Success: `{ "ok": true, "result": { ... } }`
- Error: `{ "ok": false, "errorType": "…", "reason": "…" }`
- `errorType` values are derived from `DomainError` / `UseCaseError` and should be
  stable and easy to handle programmatically.
- The process exit code is set by the CLI (based on the error) and is **not**
  included in the JSON payload.

Initial `errorType` set and purpose:
- `INVALID_ARGUMENT`: input validation failed or required flags are missing.
- `CONFIRMATION_REQUIRED`: operation requires confirmation (e.g., destructive action).
- `MFA_REQUIRED`: MFA is required but unavailable in non-interactive mode.
- `PERMISSION_DENIED`: authentication or authorization failure.
- `CLUSTER_NOT_FOUND`: referenced cluster name does not exist.
- `JOB_NOT_FOUND`: referenced job id does not exist.
- `CONFLICT`: operation conflicts with existing state (e.g., resource already exists).
- `DAEMON_UNAVAILABLE`: local `orbitd` is not reachable.
- `NETWORK_ERROR`: network resolution or connectivity failure.
- `REMOTE_ERROR`: remote execution or remote host failure.
- `LOCAL_ERROR`: local filesystem or environment failure.
- `INTERNAL_ERROR`: unexpected or unclassified error.

## Data handling isolation
Move data handling into dedicated services in the application layer:
- `SubmitPlanner`: resolve local path, choose sbatch script, build filters.
- `ClusterResolver`: look up cluster and validate connectivity.
- `PathResolver`: canonicalize and validate paths.
- `SbatchSelector`: collect scripts and pick one via `InteractionPort`.

Data validation lives in services; validation/prompt loops live in handlers.

Use cases then become thin orchestration units:
- Validate inputs via services.
- Call `OrbitdPort`.
- Return `*Result` (structured data, not formatted output).


## `orbit` module layout
```
orbit/src/
  main.rs                    // thin: parse -> dispatch
  app/
    mod.rs
    dispatcher.rs
    commands/                // *Command DTOs + *Result aliases
    handlers/                // *Handler implementations
    services/                // data handling helpers (pure or port-backed)
    ports/                   // trait definitions
    errors.rs                // DomainError + UseCaseError (see errorType above)
  adapters/
    cli/
    grpc/
    terminal/
    json/
    fs/
```

## Migration plan (strangler pattern)
1. **Introduce the new app boundary**
   - Add `app/ports`, `app/commands`, `app/handlers`, `app/errors`.
   - Create `AppContext` with `UiMode` and injected ports.

2. **Wrap the existing gRPC client**
   - Implement `OrbitdPort` by delegating to existing `client.rs` helpers.
   - Keep existing `send_*` functions for now to avoid a big bang change.

3. **Add presenters and stream outputs**
   - Implement `OutputPort` and `StreamOutputPort` for human and JSON output.
   - Introduce command-specific `*Result` aliases and DTOs for presenters to render.

4. **Migrate simple commands first**
   - Move `ping`, `cluster list`, `job list` into the new dispatcher.
   - Keep old path for other commands until migrated.

5. **Migrate streaming commands**
   - Replace `send_*` vs `send_*_capture` with a single stream path + sink.
   - Migrate `logs`, `ls`, `submit`, `retrieve`, `cleanup`.

6. **Migrate interactive prompts**
   - Move `interactive.rs` logic behind `InteractionPort` and `SbatchSelector`.
   - Remove `interaction::set_non_interactive()` and the global flag.

7. **Remove old entrypoints**
   - Delete `run_non_interactive` branch and duplicate command logic in `main.rs`.
   - Remove unused `*_capture` helpers and consolidate stream handling.
   - Remove per-command `--json` flags in favor of `--non-interactive`.

## Testing approach
- Unit-test handlers with fake ports (no gRPC, no TTY).
- Unit-test services and error mapping (`DomainError`/`UseCaseError` -> `errorType`).
- Add golden-ish tests for `OutputPort` (human and JSON) and `StreamOutputPort`.
- Keep adapter tests thin (CLI parsing, gRPC request construction).
- Continue end-to-end coverage using `tests/run_e2e.py`.

## Expected outcomes
- One entrypoint in `main.rs` with a mode-driven context.
- Command logic isolated in handlers, independent of CLI and output formatting.
- No duplication between interactive and non-interactive flows.
- Clear boundaries for future refactors (e.g., error handling or protocol changes).
