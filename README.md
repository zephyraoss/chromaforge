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

`chromaforge build-ckaf`

- Replays the AcoustID archive straight into a CKAF dataset via [libchroma](https://github.com/zephyraoss/libchroma), skipping the SQLite intermediate
- Emits three files sharing one dataset id: `<prefix>.ckd` (every fingerprint, PFOR-compressed), `<prefix>.cki` (sampled posting index, stride 8 / 2-bit quantization / skip interval 64, MBID-mapped fingerprints only), and `<prefix>.ckm` (fingerprint → track/MBID map for every fingerprint, nil MBID when unmapped)
- Unmapped fingerprints stay in the `.ckd`/`.ckm` so they can be promoted into the posting index when new MBID mappings arrive
- Deduplicates submissions per acoustid like `build`: the first fingerprint wins and later duplicates merge a missing duration by the recorded fingerprint id
- MBID membership is decided after the full replay, so mappings that arrive days after their fingerprints still land in the `.cki`/`.ckm`
- Spools decoded fingerprints beside `--output` (roughly the final `.ckd` size on disk) so first-`Ctrl+C` graceful stops can resume with the same command; the spool and progress file are removed on success
- RAM: replay state (track gid, track→mbid, fingerprint→track, acoustid dedup) is held in memory — order of 10–20 GB at the full 92.6M-fingerprint corpus. Final assembly additionally buffers the whole dataset in the libchroma builders until they finish, approaching the output size (~340 GB of `.ckd` payload at full scale), so full-corpus builds need a very-high-memory machine; `--end-date`-bounded builds scale roughly linearly
- Reference: a 1.6M-fingerprint `.cki` built from 2011-08 archive data measured 6.14 B/posting with recall@1 87.6% on held-out duplicate submissions

`chromaforge sync`

- Applies daily AcoustID archive deltas (~60 MB/day) to an existing CKAF dataset built by `build-ckaf`, in place
- Tracks progress in `<prefix>.sync-progress.json` (`last_synced_day` plus the artifact `generation`, see refresh below); the first run starts from the source date `build-ckaf` stamped into the `.ckd` header
- New fingerprints are appended to the `.ckd`/`.ckm` overflow regions, with `.cki` postings (same stride-8 sampling as the build) when their track has an enabled MBID
- When a `track_mbid` delta maps a previously-unmapped track, the track's existing fingerprints are promoted: they gain `.ckm` mappings and `.cki` postings from their values already stored in the `.ckd`
- Duplicate submissions merge a missing duration into the recorded fingerprint (via an overflow record that shadows the main one), mirroring the build's dedup rule
- CKAF files carry a single overflow region, so sync keeps an append-only journal (`<prefix>.sync-journal`) of everything added since the last compaction and rewrites the region from it each run; the journal and progress file also make re-running a day a no-op
- Warns when the overflow region exceeds 10% of the main records; `--compact` folds the overflow into fresh main files instead (rebuilding the `.cki` so it keeps covering only MBID-mapped fingerprints) and resets the journal
- **Serving-node mode** (`--serving`, auto-detected when `<prefix>.ckd` is absent): updates a `.cki`/`.ckm`-only copy. New MBID-mapped fingerprints get postings and mappings appended using values straight from the day files; promotions of fingerprints whose values live only in the canonical `.ckd` — and duration backfills of `.ckd` main records — are skipped and logged as deferred counts (`sync deferred to next refresh: promotions=… duration_backfills=…`) so drift stays visible. Requires the `.sync-progress.json` deployed alongside the artifacts; `--compact` is unavailable
- RAM: rebuilds fingerprint→track / track→MBID state by scanning the `.ckm` each run — order of 10 GB at the full 92.6M-fingerprint corpus
- The dataset files must be local paths; serving-node mode is how a node avoids holding the `.ckd`

`chromaforge refresh`

- Re-baselines a dataset on a machine that holds the canonical `.ckd`: applies every archive day since the dataset's `last_synced_day` like a full sync — including all promotions the serving nodes deferred, with fingerprint values read from the local `.ckd` — then always compacts into fresh main files and rebuilds the mapped-only `.cki`
- Writes `<prefix>.sync-progress.json` with a **new artifact generation** (a UUID). The sync journal records the generation it was created under; when a serving node's next sync sees a different generation in the progress file, it knows a refresh replaced the files underneath its journal and discards the journal instead of replaying stale overflow onto the fresh artifacts
- Uploading/deploying the artifacts is the operator's job (`aws s3 cp`, `rclone`, `rsync` — see the runbook below); chromaforge deliberately contains no object-storage client
- RAM: the `.ckm` state scan (~10 GB at full corpus) plus the pending overflow content (every fingerprint added since the last refresh, with decoded values) held in memory during the rewrite — size the throwaway VM for roughly the interval's delta volume on top of the scan
- Interruptible like sync: first `Ctrl+C` stops after the current day and saves progress; re-running continues

### Two-tier operation without a permanent full-dataset machine

No permanent machine holds the `.ckd` (~340 GB at full corpus). The serving
node keeps only `.cki` + `.ckm` (~41 GB); the canonical `.ckd` lives in
S3-compatible storage and is only materialized on a throwaway VM for the
monthly refresh.

Daily, on the serving node (cron):

```bash
chromaforge sync --serving --dataset /data/chromakopia
```

Monthly, on a throwaway VM with ~400 GB scratch disk:

```bash
# 1. Materialize the canonical dataset (progress file included)
rclone copy s3:chromakopia/dataset/ /mnt/scratch/ \
  --include "chromakopia.ckd" --include "chromakopia.cki" \
  --include "chromakopia.ckm" --include "chromakopia.sync-progress.json"

# 2. Refresh: catch up all days, fold in deferred promotions, compact,
#    rotate the artifact generation
chromaforge refresh --dataset /mnt/scratch/chromakopia

# 3. Canonical copy back to object storage
rclone copy /mnt/scratch/ s3:chromakopia/dataset/ \
  --include "chromakopia.ckd" --include "chromakopia.cki" \
  --include "chromakopia.ckm" --include "chromakopia.sync-progress.json"

# 4. Deploy the serving artifact set — the progress file MUST travel with
#    the .cki/.ckm (its generation reconciles the serving node's state).
#    Stop the serving node's sync cron/timer for the swap.
rsync -av /mnt/scratch/chromakopia.cki /mnt/scratch/chromakopia.ckm \
  /mnt/scratch/chromakopia.sync-progress.json node-a:/data/

# 5. Discard the VM
```

Notes:

- The serving node's stale `<prefix>.sync-journal` does not need manual
  cleanup: the next `sync --serving` sees the new generation in the deployed
  progress file and discards the journal itself. Deleting it during the swap
  is also fine.
- The first deployment works the same way: run `refresh` on the build
  machine right after `build-ckaf` (it writes the initial progress file even
  when no archive days are pending), then ship the artifact set.
- Days that land in the archive between step 2 and the serving node's next
  sync are not lost: the deployed progress file says which day the artifacts
  cover, and the node re-syncs everything after it.

`chromaforge validate`

- Verifies the final tables and indexes exist
- Performs sampled acoustid and hash spot checks without `ORDER BY RANDOM()`
- Skips `PRAGMA quick_check` by default for speed
- Supports `--quick-check` when you want the slower SQLite consistency pass
- Supports `--full-integrity-check` when you want the slowest full `PRAGMA integrity_check`
- Supports `--count-rows` when you want exact `COUNT(*)` scans instead of the fast default

`chromaforge backfill-metadata`

- Replays archive metadata into an existing database without rebuilding `sub_fingerprints`
- Fills missing `mb_id` and `duration` values in place
- Uses `--decode-workers` to parallelize fingerprint JSON decode/filter work while keeping SQLite writes sequential
- Uses a separate resume file beside `--db` so interrupted backfills can continue later
- Leaves existing fingerprint hashes and indexes intact

`chromaforge match`

- Accepts a raw Chromaprint fingerprint with `--fingerprint` or `--fingerprint-file`
- Accepts `fpcalc -raw` output directly, including `DURATION=...`
- Uses the same sampled sub-fingerprint hashing the builder stored in SQLite
- Applies a small duration filter by default when query duration is known
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

## Metadata Backfill

```bash
chromaforge backfill-metadata \
  --db /mnt/disk/chromakopia.db \
  --gomaxprocs 32 \
  --decode-workers 32 \
  --download-workers 16
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

Disable duration filtering:

```bash
fpcalc -raw song.mp3 | chromaforge match \
  --db /mnt/disk/chromakopia.db \
  --fingerprint-file - \
  --duration-window 0
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
- `track_meta-update` files are ignored because `title` and `artist` are no longer stored in the database.
- Metadata backfill and duplicate-acoustid ingest only fill missing metadata fields; they do not overwrite existing non-empty values.

## License

Apache License 2.0. See [`LICENSE`](./LICENSE).
