#!/usr/bin/env bash
set -eu
KEY=/home/ubuntu/.ssh/id_ed25519

lxc exec tiny -- sudo -H -u ubuntu bash -c "[ -f $KEY ] || ssh-keygen -t ed25519 -N '' -q -f $KEY"
PUB=$(lxc exec tiny -- cat $KEY.pub)

CONFIG="Host *
  StrictHostKeyChecking accept-new
"
for vm in svc small{1..8}; do
  echo "$PUB" | lxc exec $vm -- sudo -H -u ubuntu bash -c 'mkdir -p ~/.ssh && cat >> ~/.ssh/authorized_keys'
  IP=$(lxc exec $vm -- ip -4 -o addr show enp5s0 | awk '{print $4}' | cut -d/ -f1)
  CONFIG+="Host $vm
  HostName $IP
"
done

echo "$CONFIG" | lxc exec tiny -- sudo -H -u ubuntu bash -c 'cat > ~/.ssh/config'
echo "ok"
