---
title: Introduction
description: Orbit is a local-first interface for working with Slurm clusters over SSH.
slug: /
---

`orbit` is a local-first interface for Slurm.

It is designed for a development workflow where code and data preparation stay on your machine, while compute runs remotely on an HPC cluster. Instead of switching into a remote-only workflow, you can manage submit, monitoring, logs, and result retrieval directly from a local CLI. Practically, it is designed to move all your HPC workflow to your local machine

## Learn more
- **Jump to [Quickstart](./quickstart.md)** to see what orbit feels like yourself
- **Continue reading** this document to get a deeper understanding of how Orbit works.
- **Learn about [Orbitfile](./orbitfile.md)** to make most use of Orbit.



## Main concepts
Orbit's primary goal is making remote clusters feel like they are just local resources on your machine. For Orbit, **Cluster** is any remote server with Slurm on it (Slurm is the most popular workload scheduler - software that enables scheduling computing workloads on a single server or on a large interconnected fleet of servers). Through Orbit, you can do computing on clusters in two primary ways:
- **Submitting jobs**: with [orbit job submit](./cli/commands/job/orbit-job-submit.md), you can write your code locally and then submit it to a remote cluster. Orbit will handle transferring files to a new directory on the cluster you point to, scheduling the job on the scheduling, and then will, on your request, [print the job's logs](./cli/commands/job/orbit-job-logs.md), [check job status](./cli/commands/job/orbit-job-get.md), [retrieve job results](./cli/commands/job/orbit-job-retrieve.md), or [list all your running jobs](./cli/commands/job/orbit-job-list.md).
- **Submitting projects**: projects is a distinct feature that allows you to reproduce your analysis with maybe varying parameters. You can [initialize a project](./cli/commands/project/orbit-project-init.md) (that will initialize the Orbit-specific file, Orbitfile, in a directory)

To enable all that, Orbit consists of two main components that are both installed together with Homebrew and run locally on your machine:

- `orbit`: CLI interface for everything described above.
- `orbitd`: the local daemon that performs cluster operations. It runs in the background and does all the heavy-lifting and maintenance such as persisting connections to servers so that you don't need to reconnect to them all the time.

The next few paragraphs will give you a detailed explanation of how 
## 1. Install Orbit

The most convenient and error-prone way to install Orbit is through [Homebrew](https://brew.sh):
```bash
brew install vertices-solutions/orbit/orbit
brew services start orbit
```

To check that orbit is installed and ready to run:
```bash
orbit ping
```

## 2. Add your first cluster

Now let's add your cluster to orbit's internal registry. 
It will stay local on your machine.
For example, if you're usually connecting to your cluster at user@example.com:

```bash
orbit cluster add user@example.com
```
You will be prompted for a short name for your cluster and other details.

## 3. Submit your first job

You want to submit a job when you just have some local code that you want to be run on your cluster. This is where you will be starting in most of cases: you just have a folder with scripts/pipelines/compiled code and you want to run it on a remote cluster.

Orbit assumes that you submit jobs from folders on your local machine.
Those folders can be nested to any degree and can contain any amount of code that is specific for your computing needs. Orbit handles file syncing for you.

Create a minimal local job folder:

```bash
mkdir -p ~/orbit-quickstart
cd ~/orbit-quickstart

cat > submit.sbatch <<'SBATCH'
#!/bin/bash
#SBATCH --job-name=orbit-quickstart
#SBATCH --output=results/hello.out

mkdir -p results
echo "hello from $(hostname)" > results/hello.out
SBATCH
```
The code above will create the smallest "hello world" project one can imagine: the sbatch script that you make with it writes its output to results/hello.out and prints "Hello from (work node hostname)" to stdout.

If your cluster requires extra Slurm directives (for example partition/account), add them to `submit.sbatch` script.

Now, you are ready to submit your job to cluster:

```bash
orbit job submit --to cluster-name . # note: record job-id from this step
orbit job list
orbit job get <job id>
orbit job retrieve <job id> results --output ./out
```

:::tip

Some Slurm clusters require partition and account to be specified; if that's the case - you will see errors in the log.
:::


## 4. Initialize your first project

In the same directory:

```bash
orbit project init . --name orbit_quickstart
```

This creates an `Orbitfile` and sets the project name.

## 5. Build and submit your first project
When you build a project, Orbit:
- checks that Orbitfile is correct and parses it
- saves the project directory in internal representation (a compressed tarball)
- Automatically versions your analysis code with date-derived version + a pseudotag "latest" so that you can always easily submit your latest version for a project:

```bash
orbit project build .
orbit project list # will show your project with a version
orbit project submit orbit_quickstart:latest --to <cluster-name>
```

After you make local changes to your project, and you're ready to make a new version - run `orbit project build <path>` again: that will create a new version for that project.
Project is identified by its name in Orbitfile: if you change the name with Orbitfile - new builds will be associated with a new project.
