#!/bin/bash
#SBATCH --job-name=orbit-templated-hash
#SBATCH --time=00:10:00
#SBATCH --mem=256M
#SBATCH --cpus-per-task=1
#SBATCH --output=logs/%x-%u.out
#SBATCH --error=logs/%x-%u.err
#SBATCH --account={{ORBIT_SLURM_ACCOUNT}}

set -euo pipefail

bash run.sh
