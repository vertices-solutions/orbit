# hello-world

Minimal "hello world" project for verifying logs and result retrieval.

To run: 

```bash
# clone the project locally
git clone github.com/hpcd-dev/hello-world
cd hello-world

# submit job to your cluster
hpc job submit --to <cluster> .

# view job logs (stdout)
hpc job logs <job_id>

# view job logs (stderr)
hpc job logs <job_id> --err

# look at the project's run directory
hpc job ls <job_id>

# retrieve results
hpc job retrieve <job_id> results/hello.txt
```
