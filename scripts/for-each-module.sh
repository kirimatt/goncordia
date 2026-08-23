#!/usr/bin/env bash
set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "usage: $0 command [args...]" >&2
  exit 2
fi

module_dirs=(
  .
  driver/stdlib
  driver/pgxv5
  driver/mongodb
  driver/redis
  driver/cassandra
  driver/clickhouse
  driver/dynamodb
  driver/firestore
  driver/gorm
  driver/bun
  otel
  gontest
  bench
)

for module_dir in "${module_dirs[@]}"; do
  echo "module: $module_dir"
  (cd "$module_dir" && GOWORK=off "$@")
done
