#!/usr/bin/env bash
set -eu
source /home/ubuntu/ck.env
source /run/ck-worker.conf
[ -n "$QNAME" ] || { echo "user.ck.qname is not set" >&2; exit 1; }

cd /home/ubuntu/c553-hw6
mkdir -p /home/ubuntu/logs
ARGS=(-s "$QNAME" -t "${THREADS:-8}" -p "${PREFETCH:-1}" -x "${WHISPER:-1}")
if [ -n "${IDLE:-}" ] && [ "$IDLE" != 0 ]; then ARGS+=(-i "$IDLE"); fi

exec venv/bin/python3 src/worker.py "${ARGS[@]}" >> "/home/ubuntu/logs/worker-$QNAME.log" 2>&1
