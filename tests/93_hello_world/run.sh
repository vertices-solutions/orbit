#!/usr/bin/env bash
set -euo pipefail

project_id="06_hello_world"
timestamp="$(date '+%Y-%m-%d %H:%M:%S')"
hostname="$(hostname)"

mkdir -p results

printf "Hello from orbit!\n"
printf "Project %s ran on %s at %s\n" "$project_id" "$hostname" "$timestamp"
printf "This is stdout.\n"
printf "To retrieve results: hpc job retrieve <job_id> results/hello.txt --output ./out/06_hello_world\n"
printf "This is stderr for --err logs.\n" 1>&2

cat <<RESULTS >results/hello.txt
Hello from orbit!
Project $project_id ran on $hostname at $timestamp
This is in results.
To retrieve stdout: hpc job logs <job_id>
RESULTS
