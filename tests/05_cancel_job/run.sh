#!/bin/bash
set -euo pipefail

mkdir -p results
printf 'started %s\n' "$(date -u +%FT%TZ)" > results/started.txt
sleep 600
