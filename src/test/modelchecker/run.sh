#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../../.." && pwd)
source_file=$root/src/test/modelchecker/queuetest.c

if [ -n "${GENMC:-}" ]; then
  genmc=$GENMC
else
  genmc=$(command -v genmc || true)
fi

if [ -z "$genmc" ] || [ ! -x "$genmc" ]; then
  echo "GenMC executable not found; set GENMC or add genmc to PATH" >&2
  exit 1
fi

run_queue_case()
{
  number=$1
  name=$2
  echo "queue model: $name"
  "$genmc" \
    --rc11 \
    --disable-estimation \
    --disable-spin-assume \
    --check-liveness \
    --print-error-trace \
    --disable-warn-on-unfreed-memory \
    -- \
    -std=gnu11 \
    -DQUEUE_MODEL_CASE="$number" \
    -I"$root/src/include" \
    "$source_file"
}

run_queue_case 1 two-producers
run_queue_case 2 two-consumers
run_queue_case 3 partition-growth
run_queue_case 4 payload-publication
run_queue_case 5 claim-versus-seal
