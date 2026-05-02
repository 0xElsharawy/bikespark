#!/usr/bin/env bash

set -euo pipefail

# Check dependencies
for cmd in wget unzip; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "❌ $cmd is not installed. Please install it first."
    exit 1
  fi
done

YEAR="${1:-}"

if [ -z "$YEAR" ]; then
  echo "Usage: $0 <year>"
  exit 1
fi

ZIP_FILE="${YEAR}-citibike-tripdata.zip"
OUT_DIR="citibike_${YEAR}"

wget -q "https://s3.amazonaws.com/tripdata/${ZIP_FILE}" -O "$ZIP_FILE"
unzip -q "$ZIP_FILE" -d "$OUT_DIR"
rm "$ZIP_FILE"

echo "✅ Done: $OUT_DIR/"
