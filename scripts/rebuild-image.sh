#!/usr/bin/env bash
# Run on the node: rebuild ck-worker with the current code, from the image itself.
set -eu
B=ckwbuild
REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
APP=/home/ubuntu/c553-hw6

echo "[1/5] builder VM from the current image"
lxc launch ck-worker $B --vm -c limits.cpu=8 -c limits.memory=16GiB -d root,size=32GiB
until lxc exec $B -- true 2>/dev/null; do sleep 2; done
lxc exec $B -- systemctl stop ck-worker

echo "[2/5] copy the current code (not the venv, results or workloads)"
tar czf - -C "$(dirname "$REPO")" --exclude=venv --exclude=results --exclude=workloads \
    "$(basename "$REPO")" | lxc exec $B -- sudo -u ubuntu tar xzf - -C /home/ubuntu
lxc file push ~/ck.env $B/home/ubuntu/ck.env --uid 1000 --gid 1000 --mode 600
lxc exec $B -- chmod +x $APP/scripts/worker-config.sh $APP/scripts/worker-launch.sh
lxc exec $B -- sudo -u ubuntu $APP/venv/bin/python3 -m py_compile $APP/src/worker.py $APP/src/mgmt.py

echo "[3/5] sysstat (mpstat on the workers)"
lxc exec $B -- bash -c 'command -v mpstat >/dev/null || (apt-get update -qq && apt-get install -y -qq sysstat)'

echo "[4/5] clear identity, stop, publish"
lxc exec $B -- bash -c 'rm -rf /home/ubuntu/logs/*; cloud-init clean --logs; truncate -s 0 /etc/machine-id'
lxc stop $B
lxc image delete ck-worker 2>/dev/null || true
lxc publish $B --alias ck-worker --compression none

echo "[5/5] clean up"
lxc delete $B
lxc image list ck-worker
