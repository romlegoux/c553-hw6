#!/bin/bash

OUTPUT_FILE="$HOME/c553-hw6/sysinfo/node.txt"

> "$OUTPUT_FILE"


{
    echo "=== DATE -U ==="
    date -u
    echo -e "\n=== HOSTNAME ==="
    hostname
    echo -e "\n=== UNAME -A ==="
    uname -a
    echo -e "\n=== LSCPU ==="
    lscpu
    echo -e "\n=== FREE -G ==="
    free -g
    echo -e "\n=== LSBLK ==="
    lsblk
    echo -e "\n=== LXC --VERSION ==="
    lxc --version 2>/dev/null || echo "lxc not found"
    echo -e "\n=== INSTANCE METADATA ==="
    curl -s http://169.254.169.254/openstack/latest/meta_data.json
    echo ""
} >> "$OUTPUT_FILE"
