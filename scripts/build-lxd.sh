#!/usr/bin/env bash

set -eu
command -v lxd >/dev/null || sudo snap install lxd


sudo lxc network show lxdbr0 >/dev/null 2>&1 || sudo lxd init --auto --storage-backend dir

if systemctl is-active --quiet firewalld; then
  sudo firewall-cmd --zone=trusted --change-interface=lxdbr0 --permanent
  sudo firewall-cmd --reload
fi

if ! id -nG | grep -qw lxd; then
  sudo usermod -aG lxd "$USER"
  echo "Added $USER to group lxd: log out and back in before running 02_vms.sh"
fi
echo "LXD ready."
