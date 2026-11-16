#!/bin/bash
# A ready-made job script, used through `script_file` in perlmutter_script_job.yaml.
# It is uploaded exactly as written: no {placeholder} rendering. IRI starts it in the run
# directory (<remote.work_dir>/<run_id>) with the resources from the job file.
# NERSC's IRI starts it under srun, once per rank (n_node x n_process_per_node copies), so
# do not call srun here: a nested srun waits for CPUs the ranks already hold.
set -euo pipefail

rank=${SLURM_PROCID-0}
echo "rank $rank of ${SLURM_NTASKS-1} on $(hostname) at $(date -u), cwd $(pwd)"

if [ "$rank" = 0 ]; then
    echo "Job completed" > result.txt   # one writer for shared files
fi
