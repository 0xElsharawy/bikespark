#!/usr/bin/env bash

set -euo pipefail

# Check dependencies
for cmd in wget unzip; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "❌ $cmd is not installed. Please install it first."
    exit 1
  fi
done

YEAR=2014

ZIP_FILE="${YEAR}-citibike-tripdata.zip"
OUT_DIR="citibike_${YEAR}"

JAR_DIR="jars"

mkdir -p "$JAR_DIR"

wget -q \
  "https://github.com/ClickHouse/clickhouse-java/releases/download/v0.6.0/clickhouse-jdbc-0.6.0-all.jar" \
  -O "${JAR_DIR}/clickhouse-jdbc-0.6.0-all.jar"

wget -q "https://s3.amazonaws.com/tripdata/${ZIP_FILE}" -O "$ZIP_FILE"

unzip -q "$ZIP_FILE" -d "$OUT_DIR"
rm "$ZIP_FILE"

echo "✅ Done:"
echo "  Year  → $YEAR"
echo "  Data  → $OUT_DIR/"
echo "  JAR   → ${JAR_DIR}/clickhouse-jdbc-0.6.0-all.jar"
