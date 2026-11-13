#!/usr/bin/env bash
set -eu
cd "$(dirname "${BASH_SOURCE[0]}")"
./build-lxd.sh
id -nG | grep -qw lxd || { echo "Log out/in, then re-run bootstrap.sh"; exit 0; }
./build-vm.sh
./build-services.sh
./setup-ssh.sh
./sync.sh
