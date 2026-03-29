# Chromaforge

Chromaforge is a Go CLI that reconstructs the AcoustID fingerprint SQLite database used by Chromakopia. It is designed for the one-time initial build on an Azure `L16s_v3` VM with local NVMe scratch space and a managed disk for the finished database.

Incremental updates are handled by Chromakopia, not this repository.

## Commands

`chromaforge build`

- Replays the AcoustID daily JSON update archive from `https://data.acoustid.org/`
- Builds a fresh SQLite database with the libSQL driver
- Uses a local cache directory beside `--db` unless `--cache-dir` is set
- Places SQLite temp files under the cache path by default; `--temp-dir` overrides it
- Prefetches upcoming AcoustID archive files in background download workers while replay/index work is running
- `--download-workers` controls that background download concurrency
- `--gomaxprocs`, `--decode-workers`, and `--workers` let you tune CPU/core usage explicitly
- `--cache-size` and `--mmap-size` tune replay/write memory, while `--index-cache-size` and `--index-mmap-size` tune the later index-build phase
- On first `Ctrl+C`, finishes the current day, saves resume progress beside `--db`, and exits cleanly; a second `Ctrl+C` aborts immediately
- Supports `--soft-heap-limit` to cap SQLite heap usage for the process
- Uses unsafe bulk-load mode with journaling disabled during replay/index builds, then finalizes the database back to WAL
- Defers the final `acoustid` unique index and `idx_hash` until bulk inserts complete
- Supports `--skip-validate` so build completion is not blocked on validation
- Optionally `rsync`s the final `.db` to the configured output path
- Optionally triggers Azure VM self-deallocation

`chromaforge validate`

- Verifies the final tables and indexes exist
- Performs sampled acoustid and hash spot checks without `ORDER BY RANDOM()`
- Skips `PRAGMA quick_check` by default for speed
- Supports `--quick-check` when you want the slower SQLite consistency pass
- Supports `--full-integrity-check` when you want the slowest full `PRAGMA integrity_check`
- Supports `--count-rows` when you want exact `COUNT(*)` scans instead of the fast default

`chromaforge match`

- Accepts a raw Chromaprint fingerprint with `--fingerprint` or `--fingerprint-file`
- Accepts `fpcalc -raw` output directly, including `DURATION=...`
- Uses the same sampled sub-fingerprint hashing the builder stored in SQLite
- Returns the top local candidate matches ranked by aligned hash hits

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
  --temp-dir /mnt/nvme/.chromaforge-tmp \
  --cache-size 4294967296 \
  --mmap-size 4294967296 \
  --index-cache-size 2147483648 \
  --index-mmap-size 2147483648 \
  --workers 16 \
  --decode-workers 16 \
  --batch-size 500 \
  --skip-validate \
  --soft-heap-limit 2147483648
```

Azure VM example with copy + self-deallocate:

```bash
chromaforge build \
  --db /mnt/nvme/chromakopia.db \
  --output /mnt/disk/chromakopia.db \
  --gomaxprocs 12 \
  --download-workers 12 \
  --temp-dir /mnt/nvme/.chromaforge-tmp \
  --cache-size 4294967296 \
  --mmap-size 4294967296 \
  --index-cache-size 2147483648 \
  --index-mmap-size 2147483648 \
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

Quick check example:

```bash
chromaforge validate \
  --db /mnt/disk/chromakopia.db \
  --quick-check
```

Full validation example:

```bash
chromaforge validate \
  --db /mnt/disk/chromakopia.db \
  --full-integrity-check \
  --count-rows \
  --timeout 0
```

## Matching

Raw fingerprint example:

```bash
chromaforge match \
  --db /mnt/disk/chromakopia.db \
  --fingerprint '123,456,789,101112'
```

`fpcalc -raw` example:

```bash
fpcalc -raw song.mp3 | chromaforge match \
  --db /mnt/disk/chromakopia.db \
  --fingerprint-file -
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
