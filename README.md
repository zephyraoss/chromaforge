# Chromaforge

Chromaforge is a Go CLI that reconstructs the AcoustID fingerprint SQLite database used by Chromakopia. It is designed for the one-time initial build on an Azure `L16s_v3` VM with local NVMe scratch space and a managed disk for the finished database.

Incremental updates are handled by Chromakopia, not this repository.

## Commands

`chromaforge build`

- Replays the AcoustID daily JSON update archive from `https://data.acoustid.org/`
- Builds a fresh SQLite database with the libSQL driver
- Uses a local cache directory beside `--db` unless `--cache-dir` is set
- Prefetches upcoming AcoustID archive files in background download workers while replay/index work is running
- `--download-workers` controls that background download concurrency
- `--gomaxprocs`, `--decode-workers`, and `--workers` let you tune CPU/core usage explicitly
- `--cache-size` and `--mmap-size` let you override the SQLite page cache and mmap window in bytes
- On first `Ctrl+C`, finishes the current day, saves resume progress beside `--db`, and exits cleanly; a second `Ctrl+C` aborts immediately
- Supports `--soft-heap-limit` to cap SQLite heap usage for the process
- Uses unsafe bulk-load mode with journaling disabled during replay/index builds, then finalizes the database back to WAL
- Defers the final `acoustid` unique index and `idx_hash` until bulk inserts complete
- Runs validation
- Optionally `rsync`s the final `.db` to the configured output path
- Optionally triggers Azure VM self-deallocation

`chromaforge validate`

- Verifies row counts
- Performs random acoustid and hash spot checks
- Runs `PRAGMA integrity_check`

`chromaforge version`

- Prints version metadata injected at build time

## Requirements

- Go 1.24+
- Network access to `https://data.acoustid.org/`
- CGO-enabled builds

`rsync` is only required when using `--output`.

## Build

```bash
go build ./cmd/chromaforge
```

Example:

```bash
chromaforge build \
  --db /mnt/nvme/chromakopia.db \
  --gomaxprocs 12 \
  --download-workers 12 \
  --cache-size 4294967296 \
  --mmap-size 4294967296 \
  --workers 16 \
  --decode-workers 16 \
  --batch-size 500 \
  --soft-heap-limit 2147483648
```

Azure VM example with copy + self-deallocate:

```bash
chromaforge build \
  --db /mnt/nvme/chromakopia.db \
  --output /mnt/disk/chromakopia.db \
  --gomaxprocs 12 \
  --download-workers 12 \
  --cache-size 4294967296 \
  --mmap-size 4294967296 \
  --workers 16 \
  --decode-workers 16 \
  --batch-size 500 \
  --soft-heap-limit 2147483648 \
  --self-deallocate
```

## Validation

```bash
chromaforge validate --db /mnt/disk/chromakopia.db
```

## Azure Build VM

Deploy only the build path from this repo:

1. Create the resource group.
2. Create the managed disk that will persist `chromakopia.db`.
3. Create a user-assigned managed identity for the build VM.
4. Grant that identity `Virtual Machine Contributor` scoped to the VM or an appropriate parent scope.
5. Create the `L16s_v3` VM.
6. Attach the managed disk.
7. Paste [`deploy/cloud-init.yaml`](./deploy/cloud-init.yaml) into the VM Custom data field.

The build VM downloads the latest `chromaforge` binary, mounts the managed disk and local NVMe, runs the build, copies the resulting database with `rsync`, and then asks Azure to deallocate the VM.

## Docker

The included `Dockerfile` provides a reproducible build image:

```bash
docker build -t chromaforge:latest .
```

## Notes

- The final database contains only `fingerprints` and `sub_fingerprints`, plus `idx_hash`.
- Build-time replay state is held outside the final schema.
- The implementation accepts `track_meta-update` files in the public archive because those records carry joinable track metadata for `title` and `artist`.

## License

Apache License 2.0. See [`LICENSE`](./LICENSE).
