#!/usr/bin/env bash
# Requires a conventional commit subject, such as "feat: add failover".
set -euo pipefail

subject=$(head -n 1 "$1")
pattern='^(feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert)(\([a-z0-9-]+\))?!?: .+'
if [[ ! $subject =~ $pattern ]]; then
  echo "Commit subject must look like 'feat: add failover', got: $subject" >&2
  exit 1
fi
