#!/usr/bin/env python3
"""Stage LibriSpeech test-clean and the whisper models in SeaweedFS (9a), on the node.

  python3 src/stage_data.py DATA_DIR

DATA_DIR holds LibriSpeech/test-clean/ and ggml-base.en.bin, ggml-tiny.en.bin.
Buckets: audio (test-clean/SPK/CHAP/*, same layout), models (model + .sha256),
transcripts and results (empty). Writes workloads/jobs-test-clean.txt.
"""

import os
import sys
from concurrent.futures import ThreadPoolExecutor

import s3util

MODELS = ("ggml-base.en.bin", "ggml-tiny.en.bin")


def upload(args):
    path, key = args
    s3 = s3util.client()
    try:
        if s3.head_object(Bucket="audio", Key=key)["ContentLength"] == os.path.getsize(path):
            return 0
    except Exception:
        pass
    s3.upload_file(path, "audio", key)
    return 1


def main():
    data = sys.argv[1]
    s3 = s3util.client()
    for b in ("audio", "models", "transcripts", "results"):
        s3util.ensure_bucket(s3, b)

    for m in MODELS:
        path = os.path.join(data, m)
        sha = s3util.sha256_file(path)
        s3.upload_file(path, "models", m)
        s3.put_object(Bucket="models", Key=m + ".sha256", Body=sha.encode())
        print(f"models/{m}: {os.path.getsize(path)} bytes, sha256 {sha}")

    root = os.path.join(data, "LibriSpeech")
    files, chapters = [], set()
    for d, _, names in os.walk(os.path.join(root, "test-clean")):
        for n in names:
            if n.endswith((".flac", ".trans.txt")):
                key = os.path.relpath(os.path.join(d, n), root)
                files.append((os.path.join(d, n), key))
                chapters.add(os.path.dirname(key))
    with ThreadPoolExecutor(16) as pool:
        sent = sum(pool.map(upload, files))
    print(f"audio: {len(files)} files ({sent} uploaded, {len(files) - sent} already there), "
          f"{len(chapters)} chapters")

    out = os.path.join(os.path.dirname(__file__), "..", "workloads", "jobs-test-clean.txt")
    with open(out, "w") as f:
        f.write("\n".join(sorted(chapters)) + "\n")
    print(f"jobs file: {os.path.normpath(out)}")


if __name__ == "__main__":
    main()
