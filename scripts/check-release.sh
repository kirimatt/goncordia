#!/usr/bin/env bash
set -euo pipefail

latest=$(sed -nE 's/^## \[(v[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.-]+)?)\] — [0-9]{4}-[0-9]{2}-[0-9]{2}$/\1/p' CHANGELOG.md | head -n 1)
tag=${1:-$latest}

if [[ ! "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.-]+)?$ ]]; then
  echo "invalid semantic-version tag: $tag" >&2
  exit 1
fi
if [[ -z "$latest" || "$tag" != "$latest" ]]; then
  echo "release tag $tag must match latest CHANGELOG version ${latest:-<missing>}" >&2
  exit 1
fi
if ! grep -Eq "^## \[$tag\] — [0-9]{4}-[0-9]{2}-[0-9]{2}$" CHANGELOG.md; then
  echo "CHANGELOG.md has no dated section for $tag" >&2
  exit 1
fi
if ! grep -Fq "github.com/kirimatt/goncordia@$tag" README.md; then
  echo "README.md installation version does not match $tag" >&2
  exit 1
fi
