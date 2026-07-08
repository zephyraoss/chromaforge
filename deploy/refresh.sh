#!/usr/bin/env bash
set -euo pipefail

REMOTE=${REMOTE:-s3:chromakopia/dataset}
SCRATCH=${SCRATCH:-/mnt/scratch}
PREFIX=${PREFIX:-chromakopia}
SERVING_HOST=${SERVING_HOST:-node-a}
SERVING_DIR=${SERVING_DIR:-/data}

canonical_files=("$PREFIX.ckd" "$PREFIX.cki" "$PREFIX.ckm" "$PREFIX.sync-progress.json")
serving_files=("$PREFIX.cki" "$PREFIX.ckm" "$PREFIX.sync-progress.json")

echo ">> downloading canonical dataset"
for f in "${canonical_files[@]}"; do
  rclone copyto "$REMOTE/$f" "$SCRATCH/$f"
done

echo ">> refreshing"
chromaforge refresh --dataset "$SCRATCH/$PREFIX"

echo ">> uploading canonical dataset"
for f in "${canonical_files[@]}"; do
  rclone copyto "$SCRATCH/$f" "$REMOTE/$f"
done

echo ">> deploying serving artifacts to $SERVING_HOST:$SERVING_DIR"
ssh "$SERVING_HOST" "systemctl stop chromaforge-sync.timer || true"
for f in "${serving_files[@]}"; do
  rsync -av "$SCRATCH/$f" "$SERVING_HOST:$SERVING_DIR/"
done
ssh "$SERVING_HOST" "systemctl start chromaforge-sync.timer || true"

echo ">> refresh complete; this VM can be discarded"
