#!/usr/bin/env bash
# Helpers for the Part 7 experiments, sourced by scripts/exp-*.sh (run on the node).

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RES=$REPO/results
PY=$REPO/venv/bin/python3
source ~/ck.env
PW=$(cat ~/ck_password)

log()  { echo "[$(date -u +%H:%M:%S)] $*"; }
hdr()  { echo "# vm=$1 node=$(hostname) utc=$(date -u +%Y-%m-%dT%H:%M:%SZ) commit=$(git -C "$REPO" rev-parse --short HEAD)"; }
cql()  { lxc exec svc -- docker exec cassandra cqlsh -u ck -p "$PW" -e "$1" 2>/dev/null; }
rmq()  { lxc exec svc -- docker exec rabbitmq rabbitmqctl -q "$@"; }
worker_vms() { lxc list '^ckw' -c n --format csv; }

on_tiny() {
  lxc exec tiny --user 1000 --group 1000 --cwd /home/ubuntu/c553-hw6 \
    --env HOME=/home/ubuntu -- bash -c "source /home/ubuntu/ck.env && $*"
}

prov_field() { grep -v '^#' "$DIR/prov-$Q.log" | tail -1 | cut -d, -f"$1"; }

clocks() {
  hdr node
  for vm in node tiny svc $(worker_vms); do
    echo "== $vm"
    c='chronyc tracking 2>/dev/null || timedatectl timesync-status 2>/dev/null || timedatectl'
    if [ "$vm" = node ]; then bash -c "$c"; else lxc exec "$vm" -- bash -c "$c"; fi
  done
}

session_start() {   # Q W P DIR
  Q=$1; W=$2; P=$3; DIR=$4
  mkdir -p "$DIR"
  [ -z "$(worker_vms)" ] || { echo "ERROR: ckw VMs already exist"; exit 1; }
  on_tiny "${SETUP_ENV:-} venv/bin/python3 src/setup.py $Q" > "$DIR/setup-$Q.log" 2>&1
  "$PY" "$REPO/src/provisioner.py" -s "$Q" --min "$W" --max "$W" -p "$P" \
    > "$DIR/prov-$Q.log" 2>&1 &
  PROV=$!
  log "session $Q: launching $W workers (prefetch $P)"
  until [ "$(prov_field 5)" = "$W" ]; do
    kill -0 "$PROV" 2>/dev/null || { echo "ERROR: provisioner died, see $DIR/prov-$Q.log"; exit 1; }
    sleep 2
  done
  log "session $Q: $W workers connected"
  clocks > "$DIR/clocks-$Q.txt" 2>&1
  local pids=()
  for vm in $(worker_vms); do
    lxc exec "$vm" -- bash -c 'command -v mpstat >/dev/null || \
      (apt-get update -qq && apt-get install -y -qq sysstat)' >/dev/null 2>&1 &
    pids+=($!)
  done
  wait "${pids[@]}"
}

session_end() {
  for vm in $(worker_vms); do
    lxc exec "$vm" -- cat "/home/ubuntu/logs/worker-$Q.log" > "$DIR/worker-$Q-$vm.log" 2>/dev/null
  done
  kill -TERM "$PROV"
  wait "$PROV"
  log "session $Q: workers stopped and deleted"
}

reset_run() {
  rmq purge_queue "$Q.tasks" >/dev/null
  rmq purge_queue "$Q.results" >/dev/null
  cql "TRUNCATE ck.tasks; TRUNCATE ck.claims; TRUNCATE ck.counters;" >/dev/null
}

mon_start() {   # NAME
  MON=()
  { hdr svc; echo "# utc name,type,ready,unacked,consumers"
    while :; do
      echo "$(date -u +%H:%M:%S) $(rmq list_queues name type messages_ready \
        messages_unacknowledged consumers | grep "^$Q\." | tr '\t\n' ', ')"
      sleep 5
    done; } > "$DIR/$1.queues" 2>&1 &
  MON+=($!)
  if [ "${MPSTAT:-0}" = 1 ]; then
    { hdr svc; lxc exec svc -- mpstat -P ALL 1; } > "$DIR/$1.mpstat-svc" 2>&1 &
    MON+=($!)
    for vm in $(worker_vms); do
      { hdr "$vm"; lxc exec "$vm" -- mpstat -P ALL 1; } > "$DIR/$1.mpstat-$vm" 2>&1 &
      MON+=($!)
    done
  fi
}

mon_stop() {
  kill "${MON[@]}" 2>/dev/null
  lxc exec svc -- pkill -x mpstat 2>/dev/null
  for vm in $(worker_vms); do lxc exec "$vm" -- pkill -x mpstat 2>/dev/null; done
  wait "${MON[@]}" 2>/dev/null
}

one_run() {
  local name=$1; shift
  reset_run
  mon_start "$name"
  if [ -n "${KILL_AFTER:-}" ]; then
    ( sleep "$KILL_AFTER"
      { hdr node; echo "kill -9 worker on ckw8 at $(date +%s.%N)"; } > "$DIR/$name.kill"
      lxc exec ckw8 -- pkill -9 -f '[s]rc/worker.py' ) &
  fi
  on_tiny "mkdir -p ~/runs && venv/bin/python3 src/client.py -s $Q $* -o ~/runs/$name.csv" \
    > "$DIR/$name.out" 2>&1
  mon_stop
  on_tiny "cat ~/runs/$name.csv" > "$DIR/$name.csv"
  { hdr svc; cql "SELECT worker, name, value FROM ck.counters WHERE run='$Q';"; } \
    > "$DIR/$name.counters"
  log "$name: $(grep -E '^(successes|time|throughput)' "$DIR/$name.out" | tr -s ' ' | tr '\n' ' ')"
}

runs3() {   # NAME CLIENT_ARGS...
  local name=$1; shift
  for r in 1 2 3; do
    one_run "$name-r$r" "$@"
    local t
    t=$(awk '/^time/ {print $4}' "$DIR/$name-r$r.out")
    if [ "$r" = 1 ] && [ "${t%.*}" -ge 600 ] 2>/dev/null; then
      log "$name: first run took ${t}s (> 10 min): run once"
      break
    fi
  done
}
