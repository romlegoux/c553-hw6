#!/bin/bash
set -eu

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT"

sudo apt update
sudo apt install -y python3-venv

python3 -m venv venv

venv/bin/pip install --upgrade pip
venv/bin/pip install -r requirements.txt

