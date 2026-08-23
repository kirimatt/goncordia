#!/usr/bin/env bash
set -euo pipefail

version=${1:-}
group=${2:-base}
if [[ ! "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "invalid module version: ${version:-<missing>}" >&2
  exit 1
fi

case "$group" in
  base)
    module_dirs=(
      driver/stdlib driver/pgxv5 driver/mongodb driver/redis
      driver/cassandra driver/clickhouse driver/dynamodb driver/firestore
      otel gontest
    )
    ;;
  integrations)
    module_dirs=(driver/gorm driver/bun bench)
    ;;
  *)
    echo "unknown module group: $group" >&2
    exit 1
    ;;
esac

for module_dir in "${module_dirs[@]}"; do
  expected="github.com/kirimatt/goncordia/$module_dir"
  actual=$(sed -n 's/^module //p' "$module_dir/go.mod")
  if [[ "$actual" != "$expected" ]]; then
    echo "$module_dir/go.mod declares $actual, want $expected" >&2
    exit 1
  fi
  root_version=$(cd "$module_dir" && GOWORK=off go list -m -f '{{if eq .Path "github.com/kirimatt/goncordia"}}{{.Version}}{{end}}' all)
  if [[ ! "$root_version" =~ ^v1\.[0-9]+\.[0-9]+$ ]]; then
    echo "$module_dir/go.mod does not require a root v1 release" >&2
    exit 1
  fi
  if grep -Eq '^replace[[:space:]]|^replace[[:space:]]*\(' "$module_dir/go.mod"; then
    echo "$module_dir/go.mod contains a replace directive" >&2
    exit 1
  fi
  if [[ ! -f "$module_dir/go.sum" ]]; then
    echo "$module_dir/go.sum is missing" >&2
    exit 1
  fi
  tag="$module_dir/$version"
  if git rev-parse --verify --quiet "refs/tags/$tag" >/dev/null; then
    if [[ "${ALLOW_EXISTING_TAGS:-false}" != "true" ]]; then
      echo "tag already exists: $tag" >&2
      exit 1
    fi
    if [[ "$(git rev-list -n 1 "$tag")" != "$(git rev-parse HEAD)" ]]; then
      echo "tag $tag does not point to HEAD" >&2
      exit 1
    fi
  fi
done
