#!/usr/bin/env bash
# Run on the node (9a): download test-clean and the models into ~/data, upload them to SeaweedFS.
set -eu
REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
DATA=${DATA:-$HOME/data}
mkdir -p "$DATA" && cd "$DATA"
[ -d LibriSpeech/test-clean ] || { wget -q https://openslr.elda.org/resources/12/test-clean.tar.gz \
                                   && tar xzf test-clean.tar.gz; }
M=https://huggingface.co/ggerganov/whisper.cpp/resolve/main
for m in base.en tiny.en; do [ -f "ggml-$m.bin" ] || wget -q "$M/ggml-$m.bin"; done
source ~/ck.env
"$REPO/venv/bin/python3" "$REPO/src/stage-data.py" "$DATA"
