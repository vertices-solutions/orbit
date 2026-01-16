# 02_python_stats

CSV stats with Python stdlib; exercises data sync and custom sbatch script paths.

Features covered:
- sbatch script in subdirectory via positional sbatchscript argument
- data file sync and relative-path access
- JSON/text outputs for retrieve and logs

Suggested commands:
- orbit job submit <cluster> tests/02_python_stats scripts/submit.sbatch
- orbit job ls <job_id> results
- orbit job retrieve <job_id> results --output ./out/02_python_stats
