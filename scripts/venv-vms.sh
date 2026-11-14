#!/usr/bin/env bash
set -u
VMS=${*:-tiny small1 small2 small3 small4 small5 small6 small7 small8}
mkdir -p ~/logs
 
declare -A PID
for vm in $VMS; do
  lxc exec "$vm" -- sudo -iu ubuntu bash c553-hw6/scripts/build-venv.sh \
    > ~/logs/venv-"$vm".log 2>&1 &
  PID[$vm]=$!
done
 
fail=0
for vm in $VMS; do
  if wait "${PID[$vm]}"; then echo "$vm: venv OK"
  else echo "$vm: FAILED (see ~/logs/venv-$vm.log)"; fail=1; fi
done
exit $fail
 
