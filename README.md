# GlobusIRI

GlobusIRI submits and monitors jobs on DOE HPC facilities through the **IRI API** and moves
files with **Globus**:

- **HTTPS** carries run scripts up to the HPC and brings logs back.
- **E2E Transfer** moves bulk input and output data.

Supported sites: NERSC Perlmutter and ALCF Crux.

```
iri login (once) → iri/https/e2e refresh (cron)
→ [e2e stagein] → https upload → iri submit → iri monitor → https download logs → [e2e stageout]
```

## Install

```bash
pip install "globusiri[panda]"
export GLOBUSIRI_WORK_DIR=/path/to/globusiri_work   # optional; default: per-user data dir
globusiri init && globusiri paths
```

Config, tokens, logs, and run state all live under `GLOBUSIRI_WORK_DIR`. If the variable is
not set, they go in the per-user globusiri data directory.

## Documentation

- [docs/design.md](docs/design.md): packaging and local layout, architecture, token model, workflow, and open questions
- [docs/configuration.md](docs/configuration.md): WORK_DIR and the `sites.yaml` schema
- [docs/cli.md](docs/cli.md): `globusiri` commands and cron setup
- [src/globusiri/data/sites.example.yaml](src/globusiri/data/sites.example.yaml): example site config for `NERSC_Perlmutter` and `ALCF_CRUX` (fixed per site)
- [src/globusiri/data/jobs/](src/globusiri/data/jobs/): example job files (`perlmutter_example_job.yaml`, `crux_example_job.yaml`), one per kind of submission

Status: the design is written. Implementation has not started yet.
