#!/usr/bin/env bash
set -eu
REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
VMS=${*:-tiny small1 small2 small3 small4 small5 small6 small7 small8}

for vm in $VMS; do
  tar czf - -C "$(dirname "$REPO")" --exclude=venv "$(basename "$REPO")" \
    | lxc exec "$vm" -- sudo -u ubuntu tar xzf - -C /home/ubuntu
  lxc file push ~/ck.env "$vm/home/ubuntu/ck.env" --uid 1000 --gid 1000 --mode 600
  echo "$vm synced"
done
