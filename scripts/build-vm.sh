#!/usr/bin/env bash
set -eu
IMAGE=ubuntu:24.04

create() {   # create NAME CPUS MEMORY DISK
  if lxc info "$1" >/dev/null 2>&1; then echo "$1 already exists"; return; fi
  lxc launch "$IMAGE" "$1" --vm -c limits.cpu="$2" -c limits.memory="$3" -d root,size="$4"
}

create tiny 4 8GiB 40GiB
create svc 8 32GiB 64GiB
for i in $(seq 1 8); do create small$i 8 16GiB 32GiB; done

for vm in tiny svc small{1..8}; do
  until lxc exec $vm -- ip -4 -o addr show enp5s0 2>/dev/null | grep -q inet; do sleep 3; done
  echo "$vm ready: $(lxc exec $vm -- ip -4 -o addr show enp5s0 | awk '{print $4}')"
done
lxc list
