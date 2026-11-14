#!/usr/bin/env bash
set -eu
SRC=small1
REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
APP=/home/ubuntu/c553-hw6

echo "[1/5] checks"
lxc exec $SRC -- test -x $APP/venv/bin/python3
lxc exec $SRC -- test -f /home/ubuntu/ck.env
lxc exec $SRC -- pkill -TERM -f "[s]rc/worker.py" || true      # no worker from workers.sh

echo "[2/5] systemd unit (enabled, NOT started here: small1 has no user.ck.qname)"
lxc file push "$REPO/config/ck-worker.service" $SRC/etc/systemd/system/ck-worker.service
lxc exec $SRC -- chmod +x $APP/scripts/worker-config.sh $APP/scripts/worker-launch.sh
lxc exec $SRC -- systemctl daemon-reload
lxc exec $SRC -- systemctl enable ck-worker

echo "[3/5] clean identity and old logs"
# Empty machine-id: otherwise every copy gets the same DHCP identity, hence the same IP.
lxc exec $SRC -- bash -c 'rm -rf /home/ubuntu/logs/*; cloud-init clean --logs; truncate -s 0 /etc/machine-id'

echo "[4/5] stop and publish (a few minutes)"
lxc stop $SRC
lxc image delete ck-worker 2>/dev/null || true
lxc publish $SRC --alias ck-worker --compression none

echo "[5/5] done"
lxc image list ck-worker
echo "Now delete the small VMs (logs saved?): lxc delete -f small1 small2 ... small8"
