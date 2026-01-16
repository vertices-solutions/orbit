# 01_smoke

Quick smoke test for basic submit/logs/retrieve paths.

Features covered:
- job submit, list, get
- stdout/stderr log parsing via SBATCH output/error (%x/%u templates)
- job logs (stdout and --err)
- job ls and retrieve for result files

Suggested commands:
- orbit job submit <cluster> tests/01_smoke
- orbit job logs <job_id>
- orbit job logs <job_id> --err
- orbit job ls <job_id> results
- orbit job retrieve <job_id> results --output ./out/01_smoke
