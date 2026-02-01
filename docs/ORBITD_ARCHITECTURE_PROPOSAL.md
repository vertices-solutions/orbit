# Orbitd Hexagonal Architecture Proposal

## Context
The `orbit` CLI has been refactored to a hexagonal architecture (`docs/ARCHITECTURE.md`). This proposal outlines how to port `orbitd` to the same architectural style while preserving external behavior (gRPC API, error codes, streaming semantics, and persistence schema).

## Goals
- Isolate application logic from gRPC, SSH, and SQLite details.
- Make use-cases unit-testable with mock ports.
- Standardize error handling and mapping to gRPC `Status`/error codes.
- Keep streaming and MFA flows intact, but decouple them from transport.
- Enable incremental migration without breaking the CLI.

## Non-goals
- No changes to `.proto` files or gRPC surface behavior.
- No schema changes in the SQLite database.
- No redesign of SSH or SFTP internals (only a boundary around them).

## Proposed Hexagonal Structure

### Layers
- **Application core** (pure logic): use-case service(s), ports, services, and domain types.
- **Inbound adapters**: gRPC server implementation translating proto requests into use-case calls and wiring stream/mfa IO.
- **Outbound adapters**: SQLite store, SSH/SFTP, filesystem, network reachability, and clock/time.

### Suggested Module Layout
```
orbitd/src/
  app/
    ports/
    services/
    errors.rs
    types.rs
    usecases.rs
  adapters/
    grpc/
    db/
    ssh/
    fs/
    network/
    time/
  config.rs
  main.rs
```

This mirrors the `orbit` layout and makes the seam between core and infrastructure explicit while keeping a single entrypoint approach.

## Application Core

### Use-case Service
Because `orbitd` exposes a single gRPC server entrypoint, a dispatcher/command layer is unnecessary overhead. Instead, a single `UseCases` struct with method-per-RPC will serve as the entrypoint:

```
impl UseCases {
    async fn add_cluster(&self, req: AddClusterInput) -> Result<AddClusterResult, AppError>;
    async fn submit_job(&self, req: SubmitJobInput) -> Result<SubmitJobResult, AppError>;
    async fn list_jobs(&self, req: ListJobsInput) -> Result<ListJobsResult, AppError>;
    ...
}
```

Use lightweight input DTOs only where they clarify mapping or decouple from proto types; otherwise pass primitive fields directly.

### Ports
Define ports traits in `app/ports` that describe what the core needs:
- `ClusterStorePort`: create/update/delete/list hosts and defaults
- `JobStorePort`: record submit, update states, list jobs
- `RemoteExecPort`: exec remote command, capture or stream output
- `FileSyncPort`: sync local project to remote
- `LocalFilesystemPort`: local path validation, read sbatch files
- `NetworkProbePort`: low-level reachability/connectivity checks
- `ClockPort`: timestamps for job records and age checks
- `StreamOutputPort`: emit stdout/stderr/progress/status events
- `MfaPort`: request MFA codes or passphrases when required

These ports ensure the core does not depend on `tonic`, `sqlx`, or SSH types.

### Errors
Introduce `AppError` in `orbitd/app/errors.rs` similar to `orbit`. It should carry:
- `error_type` or `error_code`
- a stable error message
- optional context for logging

The gRPC adapter maps `AppError` to `tonic::Status` and existing `error_codes` to preserve external behavior.

## Adapters

### Inbound Adapter: gRPC
- `adapters/grpc` implements `proto::agent_server::Agent`.
- It translates proto requests into `UseCases` calls, creates stream/MFA adapters, and returns proto responses.
- It maps results into proto responses and streams `StreamEvent`/`SubmitStreamEvent`.

### Outbound Adapters
- `adapters/db/sqlite_store`: wraps `state::db::HostStore` behind `ClusterStorePort` and `JobStorePort`.
- `adapters/ssh`: wraps `ssh::SessionManager`/`SessionCache` behind `RemoteExecPort`/`FileSyncPort`.
- `adapters/fs`: wraps `std::fs` and path normalization behind `LocalFilesystemPort`.
- `adapters/network`: wraps `util::reachability` behind `NetworkProbePort`.
- `adapters/time`: provides `ClockPort` for time and scheduling logic.

## Streaming and MFA 
These two exist separately for Transport Decoupling.

### StreamOutputPort
Use-cases push events through a `StreamOutputPort` implemented by the gRPC adapter. This preserves current streaming behavior without tying the core to `tonic` channels.

### MfaPort
Use-cases request MFA answers via a `MfaPort` abstraction. The gRPC adapter receives MFA responses from the client stream and feeds them to the core.

This separates what the handler needs (an MFA token) from how it is delivered (gRPC bidirectional stream).

## Background Job Checker
The current loop in `AgentSvc` should move into the core as a method on `UseCases`:
- `check_running_jobs` uses `JobStorePort`, `ClusterStorePort`, `RemoteExecPort`, and `ClockPort`.
- `main.rs` owns the scheduler and calls into the core on a timer.

This keeps scheduling logic at the edge while making the job-check logic testable.

## Mapping of Current Modules to Proposed Structure

### Current -> Proposed
- `orbitd/src/agent/rpc.rs` -> `adapters/grpc/agent_server.rs`
- `orbitd/src/agent/service.rs` -> `app/usecases.rs`
- `orbitd/src/agent/add_cluster.rs` -> `app/services/cluster_add_resolver.rs`
- `orbitd/src/agent/submit.rs` -> `app/usecases/submit.rs` or `app/services/submit_paths.rs`
- `orbitd/src/agent/sbatch.rs` -> `app/services/sbatch_selector.rs`
- `orbitd/src/agent/helpers.rs` -> `adapters/grpc/mapping.rs`
- `orbitd/src/agent/sessions.rs` -> `adapters/ssh/session_cache.rs`
- `orbitd/src/state/db.rs` -> `adapters/db/sqlite_store.rs`
- `orbitd/src/ssh/*` -> `adapters/ssh/*`
- `orbitd/src/util/*` -> `adapters/network`, `app/services`, or `adapters/fs`

## Migration Plan

1. **Introduce ports and adapters**
   - Define port traits and implement adapters that wrap current modules (SQLite, SSH, filesystem, network).
   - No behavior change; just indirection.

2. **Create `UseCases` and move one trivial RPC**
   - Start with `Ping` or `ListClusters`.
   - gRPC adapter calls `UseCases::ping` (or similar).

3. **Move streaming use-cases**
   - Migrate `submit`, `logs`, `ls`, `cleanup`, `retrieve` to use `StreamOutputPort` and `MfaPort`.
   - Keep existing stream event types.

4. **Move cluster add/update flow**
   - Carve out add/set/delete flows and extract validation logic into services.

5. **Move background job checker**
   - Extract to `UseCases::check_running_jobs` and call from `main.rs` timer.

6. **Delete legacy agent glue**
   - Remove or thin `agent/*` modules once all use-cases are in `app/`.

## Testing Strategy

- **Unit tests** for use-cases using mock ports (no SSH/SQLite).
- **Adapter tests** for SQLite and SSH behavior (existing tests can move under `adapters/`).
- **Integration tests** for gRPC streaming and MFA flows (ensure protocol compatibility).

## Risks and Mitigations
- **Streaming regressions**: keep proto event structs unchanged; add golden tests for stream sequences.
- **MFA flow mismatch**: enforce explicit `MfaPort` contract and simulate in unit tests.
- **Behavior drift**: migrate one RPC at a time, comparing responses to the old flow.

## Open Questions
- Should stream events be normalized in the core (single `StreamEvent` type) or keep RPC-specific variants (`SubmitStreamEvent`)?
- Do we want a single `JobStorePort` or split read/write concerns for background checks?
- Should remote path normalization be a pure service or part of a `RemotePathPort` that enforces host-specific rules?

## Summary
This proposal ports `orbitd` to the same hexagonal principles used by `orbit`, but avoids the dispatcher/command overhead by using a single `UseCases` entrypoint with method-per-RPC. It preserves the gRPC contract and behavior while enabling incremental migration, better testability, and clearer boundaries.
