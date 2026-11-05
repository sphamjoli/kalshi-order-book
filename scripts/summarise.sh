#!/usr/bin/env bash
# Markets placed is summed across feeds. Prints markets placed, losses by cause and page times for each run in results/.
set -uo pipefail

for log in results/*.log; do
  clean=$(sed -E 's/\x1b\[[0-9;]*m//g' "$log")
  count() { grep -c "$1" <<<"$clean"; }
  placed=$(grep 'Placing markets' <<<"$clean" | grep -o 'markets=[0-9]*' | cut -d= -f2 | paste -sd+ - | bc)
  echo "== $(basename "$log" .log)"
  echo "markets placed: ${placed:-0}"
  echo "losses: $(count 'Connection lost')"
  grep 'Connection lost' <<<"$clean" | sed -E 's/.*error=//; s/ attempt=.*//' | cut -c1-60 | sort | uniq -c
  echo "retries: $(count 'retrying')"
  grep -o 'elapsed_ms=[0-9]*' <<<"$clean" | cut -d= -f2 | awk '
    NR <= 20 {a += $1; x++}
    NR > 20 && NR <= 42 {b += $1; y++}
    NR > 42 {c += $1; z++}
    END {
      if (x) printf "page ms, 1-20: %d\n", a / x
      if (y) printf "page ms, 21-42: %d\n", b / y
      if (z) printf "page ms, 43+: %d\n", c / z
    }'
done
