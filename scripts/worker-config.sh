#!/usr/bin/env bash
set -eu

# Wait for the LXD agent socket (up to 60 s after boot).
for _ in $(seq 1 60); do
  curl -sf --unix-socket /dev/lxd/sock http://lxd/1.0 >/dev/null && break
  sleep 1
done

get() { curl -sf --unix-socket /dev/lxd/sock "http://lxd/1.0/config/user.ck.$1" || true; }

{
  echo "QNAME=$(get qname)"
  echo "THREADS=$(get threads)"
  echo "PREFETCH=$(get prefetch)"
  echo "IDLE=$(get idle)"
  echo "WHISPER=$(get whisper)"
} > /run/ck-worker.conf
chmod 644 /run/ck-worker.conf
