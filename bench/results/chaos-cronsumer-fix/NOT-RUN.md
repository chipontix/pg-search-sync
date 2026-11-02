# Failure matrix on `cronsumer-commit-after-handled`: not run

Result on 2026-11-02: BUILD FAILED, zero scenarios run, zero reconcile results.
This directory holds no evidence about the fix, either way.

- 21:20–21:52 +03, branch at 8ca6a98: `docker compose build indexer gaos` under
  the shared Docker lock. `go mod download` for the indexer had not finished
  after 31 minutes (a sample download from the VM ran at about 75–130 KB/s). The
  build was stopped by hand before the 40-minute lock limit; it did not fail by itself.
- 22:30–22:37 +03: pulling the Elasticsearch and Redpanda images (725 MB
  compressed) kept failing on DNS timeouts inside the VM. Neither pull finished.
  The app binaries did compile on the host, and the indexer binary links the fork
  (`github.com/chipontix/kafka-cronsumer v0.0.0-20261101065122-5897aa618d87`).
- 22:37 +03: free disk on the host had dropped from 38 GB to 16 GB (other
  projects on the machine), and the owner called the run off.

`deploy/docker-compose.small-vm.yml`, `deploy/Dockerfile.prebuilt` and the
`bench/chaos.sh` changes from that day have therefore never run end to end.

To run it, from the branch:

    make bin-linux
    COMPOSE_EXTRA=deploy/docker-compose.small-vm.yml CHAOS_RESULTS=bench/results/chaos-cronsumer-fix make chaos
