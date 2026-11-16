#!/bin/bash
# A ready-made job script, used through `script_file` in crux_script_job.yaml.
# It is uploaded exactly as written: no {placeholder} rendering and no GLOBUSIRI_* variables.
# On Crux, IRI (PBS) runs it ONCE on the first node, in the run directory; the script starts
# the ranks itself with Cray PALS mpiexec. Crux's IRI ignores the job file's
# n_process_per_node, so the rank layout is set here.
set -euo pipefail

PPN=${PPN-4}       # processes per node
DEPTH=${DEPTH-32}  # cores per process (128 physical cores / 4)

# mpiexec comes from the cray-pals module; load it if the batch environment lacks it.
if ! command -v mpiexec >/dev/null 2>&1; then
    set +eu
    . /etc/profile
    [ -f /opt/cray/pe/lmod/lmod/init/profile ] && . /opt/cray/pe/lmod/lmod/init/profile
    module load cray-pals
    set -eu
fi

NNODES=$(sort -u "${PBS_NODEFILE:?not running under PBS}" | wc -l)
NRANKS=$((NNODES * PPN))
echo "PBS job ${PBS_JOBID-?} on $(hostname) at $(date -u): $NNODES node(s), $NRANKS rank(s), cwd $(pwd)"

mpiexec -n "$NRANKS" --ppn "$PPN" --depth "$DEPTH" --cpu-bind depth \
    bash -c 'echo "rank $PALS_RANKID (local $PALS_LOCAL_RANKID) on $(hostname)"' | sort -n -k2 > ranks.txt
cat ranks.txt
test "$(wc -l < ranks.txt)" -eq "$NRANKS"
echo "Job completed" > result.txt
