#!/usr/bin/env bash

set -u
source "$(dirname "${BASH_SOURCE[0]}")/exp-lib.sh"
DIR=$RES/elastic; mkdir -p "$DIR"

declare -A CFG=(
  [i]="--min 0 --max 8 --idle 30"
  [ii]="--min 0 --max 8 --idle 120"
  [iii]="--min 2 --max 8 --idle 30"
  [iv]="--min 8 --max 8"
)

for run in ${*:-i ii iii iv}; do
  args=${CFG[$run]:?unknown run $run}
  Q=el-$run
  min=$(echo "$args" | awk '{print $2}')
  [ -z "$(worker_vms)" ] || { echo "ERROR: ckw VMs exist, the system is not at rest"; exit 1; }

  on_tiny "venv/bin/python3 src/setup.py $Q" > "$DIR/setup-$Q.log" 2>&1
  reset_run
  clocks > "$DIR/clocks-$Q.txt" 2>&1

  # shellcheck disable=SC2086
  "$PY" "$REPO/src/provisioner.py" -s "$Q" $args > "$DIR/prov-$Q.log" 2>&1 &
  PROV=$!
  log "7e-$run: provisioner started ($args), running elastic.txt (~15 min)"

  mon_start "7e-$run"
  on_tiny "mkdir -p ~/runs && venv/bin/python3 src/client.py -s $Q -S workloads/elastic.txt \
    -o ~/runs/7e-$run.csv" > "$DIR/7e-$run.out" 2>&1
  log "7e-$run: client done: $(grep -E '^(successes|time)' "$DIR/7e-$run.out" | tr -s ' ' | tr '\n' ' ')"

  if [ "$run" != iv ]; then
    until [ "$(prov_field 8)" = "$min" ]; do sleep 5; done
    log "7e-$run: back to $min workers"
  fi
  mon_stop
  on_tiny "cat ~/runs/7e-$run.csv" > "$DIR/7e-$run.csv"
  { hdr svc; cql "SELECT worker, name, value FROM ck.counters WHERE run='$Q';"; } > "$DIR/7e-$run.counters"
  for vm in $(worker_vms); do
    lxc exec "$vm" -- cat "/home/ubuntu/logs/worker-$Q.log" > "$DIR/worker-$Q-$vm.log" 2>/dev/null
  done
  kill -TERM "$PROV"; wait "$PROV"
  log "7e-$run: provisioner stopped, all VMs deleted"
done
log "all done"
