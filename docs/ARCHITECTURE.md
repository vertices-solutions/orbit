# Overview
`orbit` is the CLI client. It interacts with `orbitd` through gRPC over local TCP.

`orbitd` is a local server. It does most of the heavy lifting.

# `orbit` architecture
This is mostly implemented in Command pattern, and since the CLI is kept fairly slim - it will probably remain that way.
Still, it should be refactored into command-specific files.
- `src/main.rs` is the entrypoint: it matches the command with clap and calls the appropriate "client" function.
- `src/client.rs` defines the high-level client-side functions.
- `src/config.rs` defines the primitives for loading the config file.
- `src/errors.rs` handles conversion of gRPC errors to human-readable errors. This is one of the things that I want to refactor later on: I want to have clear error handling, with errors defined at the protobuf level.
- `src/filters.rs`
- `src/format.rs` 
- `src/interaction.rs`
- `src/interactive.rs`
- `src/lib.rs`
- `src/mfa.rs`
- `src/non_interactive.rs`
- `src/sbatch.rs`
- `src/stream.rs` - handles streaming MFA events, streaming the responses back, and displaying 


# `orbitd` architecture
It's begging to be refactored into Hexagonal architecture.