# 04_binary_output

Generates a small binary file and nested outputs to test retrieval.

Features covered:
- binary output files
- nested directories for job ls/retrieve
- stderr logging

Suggested commands:
- orbit job submit <cluster> tests/04_binary_output
- orbit job ls <job_id> results
- orbit job retrieve <job_id> results/raw/random.bin --output ./out/04_binary_output --overwrite
- orbit job retrieve <job_id> results/text --output ./out/04_binary_output --overwrite
