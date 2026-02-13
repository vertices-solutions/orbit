# 06_templated_hashes

Templated hashing job that generates random data on the remote host and hashes it.
Template values select the hashing method and number of files.

Workflow:
- orbit job submit --to <cluster> tests/06_templated_hashes
- orbit job submit --to <cluster> tests/06_templated_hashes --field hash_method=sha1 --field file_count=5
- orbit job submit --to <cluster> tests/06_templated_hashes --preset sha256_20
- orbit job submit --to <cluster> tests/06_templated_hashes --preset md5_100
- orbit job ls <job_id> results
- orbit job retrieve <job_id> results --output ./out/06_templated_hashes

Notes:
- Data is generated on the remote host inside the job (no local data sync).
- Hashing runs sequentially; set HASH_DELAY_SECS to tune runtime (default 1 second/file).
- FILE_MB controls file size in MiB (default 5) via the environment.
- This project is not part of the automated tests/run_e2e.py suite.
