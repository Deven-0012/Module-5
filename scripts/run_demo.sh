#!/usr/bin/env bash
# Runs all four demo scenarios end-to-end and writes logs to demo-output/ (override with OUT=dir).
# Prereq: RabbitMQ running (docker compose up -d) and deps installed.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=${PY:-.venv/bin/python}
export PYTHONUNBUFFERED=1
OUT=${OUT:-demo-output}
mkdir -p "$OUT"
rm -f processed_events.txt

stop_workers() { kill "$@" 2>/dev/null || true; wait "$@" 2>/dev/null || true; }

echo "=== Scenario 1: success + competing consumers ==="
$PY worker.py --name worker-1 > $OUT/1_worker-1.log 2>&1 & W1=$!
$PY worker.py --name worker-2 > $OUT/1_worker-2.log 2>&1 & W2=$!
sleep 2
$PY producer.py --count 10 --start 1001 | tee $OUT/1_producer.log
sleep 8
stop_workers $W1 $W2

echo "=== Scenario 2: failure before ACK + redelivery ==="
$PY worker.py --name worker-1 > $OUT/2_worker-1.log 2>&1 & W1=$!
$PY worker.py --name worker-2 --crash-after 2 > $OUT/2_worker-2.log 2>&1 & W2=$!
sleep 2
$PY producer.py --count 6 --start 2001 | tee $OUT/2_producer.log
sleep 10
echo "worker-2 exit status: $(wait $W2; echo $?)" | tee -a $OUT/2_worker-2.log || true
echo "worker-1 still running? $(kill -0 $W1 2>/dev/null && echo yes || echo no)" | tee -a $OUT/2_worker-1.log
stop_workers $W1

echo "=== Scenario 3: duplicate event_id ==="
DUP_ID=$(head -n1 processed_events.txt)
$PY worker.py --name worker-1 > $OUT/3_worker-1.log 2>&1 & W1=$!
sleep 2
$PY producer.py --duplicate-of "$DUP_ID" | tee $OUT/3_producer.log
$PY producer.py --count 1 --start 3001 | tee -a $OUT/3_producer.log
sleep 4
stop_workers $W1
echo "=== Scenario 4: crash AFTER processing, before ACK -> redelivered duplicate ==="
$PY worker.py --name worker-1 > $OUT/4_worker-1.log 2>&1 & W1=$!
sleep 1
$PY worker.py --name worker-2 --crash-after 1 --crash-after-processing > $OUT/4_worker-2.log 2>&1 & W2=$!
sleep 2
$PY producer.py --count 2 --start 4001 | tee $OUT/4_producer.log
sleep 6
stop_workers $W1 $W2

cp processed_events.txt $OUT/processed_events.txt
echo "done - see $OUT/"
