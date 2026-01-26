## Orbit
<table align="center"><tr><td>

```text
██████╗ ██████╗ ██████╗ ██╗ ████████╗
██╔══██╗██╔══██╗██╔══██╗██║ ╚══██╔══╝
██║  ██║██████╔╝██████╔╝██║    ██║
██║  ██║██╔══██╗██╔══██╗██║    ██║
╚█████╔╝██║  ██║██████╔╝██║    ██║
 ╚════╝ ╚═╝  ╚═╝╚═════╝ ╚═╝    ╚═╝
 ```
</td> </tr> </table> 
<p align="center">
  <strong>Local first Slurm interface</strong>
</p>


<p align="center">
  <a href="https://www.rust-lang.org/">
    <img src="https://img.shields.io/github/languages/top/vertices-solutions/orbit" alt="Rust">
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/github/license/vertices-solutions/orbit" alt="License">
  </a>
  <a href="https://github.com/vertices-solutions/orbit/actions/workflows/ci.yml">
    <img src="https://github.com/vertices-solutions/orbit/actions/workflows/ci.yml/badge.svg?branch=main" alt="Tests">
  </a>
  <a href="https://codecov.io/gh/vertices-solutions/orbit">
    <img src="https://codecov.io/gh/vertices-solutions/orbit/graph/badge.svg" alt="Coverage">
  </a>
</p>




> [!CAUTION]
> `orbit` is in alpha stage. 
> It is quickly growing and maturating week after week: we have both unit test coverage and fairly extensive end-to-end tests providing strong guarantees of its performance. However, every once in a while we do find it misbehaving on some edge cases and/or rare Slurm deployment configurations. 
> If you also encounter any bugs, problems or undesired behaviors in `orbit` - please, go ahead and report them in issues.
> See the "Getting help" section for more details.


`orbit` provides a local-first interface to Slurm over SSH.


It aims to provide you with a single-command job submission over ssh:
[![asciicast](https://asciinema.org/a/UZn9Aj4FxCrdLe8h.svg)](https://asciinema.org/a/UZn9Aj4FxCrdLe8h)

## Introduction

`orbit` allows you to do supercomputing from the comfort of your local development environment. 

- **Add a cluster**: just tell `orbit` how to connect to a cluster over SSH, and it will handle the rest for you.
- **Submit a job**: `orbit` will handle for you where the submitted code and data will go and 
- **Retrieve the results**: `orbit` provides a simple and intuitive way to check the job's results and retrieve them once they're ready
- **Everything over SSH**: the best part is - `orbit` does everything over SSH!

## Quickstart

```sh
# install orbit through Homebrew (https://brew.sh)
brew install vertices-solutions/orbit/orbit

# interactively add your first cluster, give it a name you will use to refer to it later
orbit cluster add

# view your clusters 
orbit cluster list

# submit your analysis to your cluster!
# cd /my/super/project
orbit job submit <cluster name> .

# check the status of your job(s)
orbit job list

# check status of job with id 1
orbit job get 1

```

## Motivation

Submitting jobs to an HPC cluster usually means repeatedly doing the same glue work: copying a project to the cluster, picking the right `sbatch` script, dealing with interactive auth, and remembering where outputs landed, then rsync'ing them back.

`orbit` keeps that workflow local, repeatable, and scriptable: you talk to a local daemon, and it takes care of the remote side.
This allows you to:
- Automate your supercomputing workflows;
- Use supercomputers together with your favourite developer tools;
- Give AI agents access to supercomputing;
- Keep code in a local git repository serving as a single source of truth;
- And anything else that will come to your mind!



## Installation

### Homebrew (recommended)
First, make sure that you have Homebrew (https://brew.sh) installed.

```bash
brew install vertices-solutions/orbit/orbit
```

This installs both `orbit` (CLI) and `orbitd` (daemon). `brew services` runs the `orbitd` daemon for your user. 

### Build from source

- Install a Rust toolchain that supports Edition 2024.
- Build the workspace: `cargo build`
- Install binaries locally:
  - `cargo install --path orbit` (installs `orbit`)
  - `cargo install --path orbitd` (installs `orbitd`)

## Configuration

Configuration file used by both CLI and server components is located in:
- macOS: `~/Library/Application Support/orbit/orbit.toml`
- Linux: `~/.config/orbit/orbit.toml`


You can find the database file in: 
- macOS: `~/Library/Application Support/orbit/orbit.sqlite`
- Linux: `~/.config/orbit/orbit.sqlite`



## Development
```bash
# Make sure you have Rust and cargo version 1.92.0+
# Ensure you also have protobuf and sqlite installed

# run tests 
cargo test

# build everything
cargo build

# run the daemon and use a local config pointing to testing-specific database.
cargo run -p orbitd --release -- --config test.toml

# use the client
cargo run -p orbit -- --help
```
### Project Structure

- `orbit/` — command-line client (binary: `orbit`).
- `orbitd/` — daemon/server implementation (binary: `orbitd`).
- `proto/` — shared gRPC/protobuf contract and generated types.
  - `proto/protos/` — `.proto` sources.
  - `proto/build.rs` — code generation entrypoint.
- `scripts` - development scripts.
- `tests` - end-to-end tests for orbit.
- `docs` - where documentation and documenting assets for this project go

## Compatibility
`orbit` has been tested on clusters with Slurm versions newer than 22.05.8. 
User reports have shown that earlier versions of Slurm are also compatible. If you encounter any bugs on any Slurm version - report them through Issues and make sure to include Slurm version in your report.

## Getting help 
Do you have any questions or have you encountered any bugs? Please open a GitHub issue — happy to help.
When creating an issue, make sure to include the following:
- Are you building the code from source, or installing it from homebrew?
- What version of orbit are you running?
- What Slurm version is on your cluster?
- What OS, including version, are you using?

## Known issues
- Unreachable cluster sometimes causes timeouts of `cluster list` and `cluster get <cluster name>`. This is a known issue that's being worked on


## License

GNU Affero General Public License v3.0 (AGPL-3.0-only). See `LICENSE`.
