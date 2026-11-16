#!/bin/bash
# A ready-made job script, used through `script_file` in perlmutter_script_job.yaml.
# It is uploaded exactly as written: no {placeholder} rendering. IRI starts it in the run
# directory (<remote.work_dir>/<run_id>) with the resources from the job file.
set -euo pipefail

echo "Hello from $(hostname) at $(date -u)"
echo "Slurm job ${SLURM_JOB_ID-?}: ${SLURM_NNODES-?} node(s), ${SLURM_NTASKS-?} task(s), cwd $(pwd)"

# srun uses the allocation from the job file's resources (n_node x n_process_per_node tasks)
srun bash -c 'echo "rank $SLURM_PROCID on $(hostname)"' | sort -n -k2 > ranks.txt
echo "Job completed" > result.txt
