#!/usr/bin/env bash
# Runs one timed experiment. Usage: run_experiment.sh NAME SECONDS [collect options]
# The name "baseline" runs a scan with no sockets. SAMPLE=1 records throughput
# and socket backlog. LOAD_TEST_AFTER=N runs a speed test after N seconds.
# KEEP_OUTPUT=1 keeps the recording.
set -euo pipefail

name=$1
seconds=$2
shift 2
bin=./target/release/engineering-challenge
mkdir -p results
export RUST_LOG=info,engineering_challenge::kalshi::rest=debug NO_COLOR=1

if [[ $name == baseline ]]; then
  "$bin" discover 2>"results/$name.log" &
else
  "$bin" collect --output "results/$name.sbe" "$@" 2>"results/$name.log" &
fi
pid=$!

sample() {
  while kill -0 "$pid" 2>/dev/null; do
    sleep 10
    bytes=$(nettop -P -L 1 -p "$pid" -J bytes_in -x 2>/dev/null | tail -1 | awk -F, '{print $(NF-1)}')
    backlog=$(netstat -anv -p tcp | awk -v p=":$pid" 'index($0, p) && /ESTABLISHED/ {n++; r+=$2; s+=$3} END {print n+0, r+0, s+0}')
    echo "$(date +%T) bytes_in=$bytes connections,recv_q,send_q=$backlog cpu=$(ps -o %cpu= -p "$pid")"
  done >"results/$name.samples"
}

[[ -n ${SAMPLE:-} ]] && sample &
if [[ -n ${LOAD_TEST_AFTER:-} ]]; then
  sleep "$LOAD_TEST_AFTER"
  date -u +%T >"results/$name-loaded.txt"
  networkQuality -s >>"results/$name-loaded.txt"
  sleep $((seconds - LOAD_TEST_AFTER))
else
  sleep "$seconds"
fi
kill -INT "$pid" 2>/dev/null || true
status=0
wait "$pid" || status=$?
echo "$status" >"results/$name.exit"
wait
[[ -n ${KEEP_OUTPUT:-} ]] || rm -f "results/$name.sbe"
