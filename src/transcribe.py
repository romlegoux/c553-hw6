#!/usr/bin/env python3
"""Part 9 helpers: verified model cache, one transcription, word error rate.

  python3 src/transcribe.py CHAPTER [MODEL] [THREADS]
      benchmark one chapter, e.g. test-clean/1089/134686 base.en 8
"""

import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time

import s3util

WHISPER = os.environ.get("CK_WHISPER", "/opt/whisper.cpp/build/bin/whisper-cli")
CACHE = os.path.expanduser("~/models")
_lock = threading.Lock()
_verified = {}


def model_path(s3, model):
    """Local copy of ggml-MODEL.bin: fetched once from SeaweedFS, SHA-256 checked."""
    name = f"ggml-{model}.bin"
    path = os.path.join(CACHE, name)
    with _lock:
        if _verified.get(name):
            return path
        want = s3.get_object(Bucket="models", Key=name + ".sha256")["Body"].read().decode().strip()
        if not (os.path.exists(path) and s3util.sha256_file(path) == want):
            os.makedirs(CACHE, exist_ok=True)
            s3.download_file("models", name, path + ".part")
            if s3util.sha256_file(path + ".part") != want:
                os.remove(path + ".part")
                raise RuntimeError(f"SHA-256 mismatch for {name}")
            os.replace(path + ".part", path)
        _verified[name] = True
        return path


def transcribe(s3, key, model_file, threads):
    """FLAC from the audio bucket -> (text, audio seconds, bytes read). Temp files removed."""
    tmp = tempfile.mkdtemp(prefix="ck-")
    try:
        flac, wav = os.path.join(tmp, "in.flac"), os.path.join(tmp, "in.wav")
        s3.download_file("audio", key, flac)
        subprocess.run(["ffmpeg", "-v", "error", "-y", "-i", flac, "-ar", "16000", "-ac", "1",
                        "-c:a", "pcm_s16le", wav], check=True)
        seconds = (os.path.getsize(wav) - 44) / 32000
        subprocess.run([WHISPER, "-m", model_file, "-t", str(threads), "-nt", "-otxt",
                        "-of", os.path.join(tmp, "out"), "-f", wav],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        with open(os.path.join(tmp, "out.txt")) as f:
            text = " ".join(f.read().split())
        return text, seconds, os.path.getsize(flac)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def words(text):
    """Normalization of the subject: lowercase, keep a-z 0-9 and ', split on spaces."""
    return re.sub(r"[^a-z0-9']", " ", text.lower()).split()


def edit_distance(hyp, ref):
    """Minimum number of word substitutions + deletions + insertions."""
    prev = list(range(len(ref) + 1))
    for i, h in enumerate(hyp, 1):
        cur = [i] + [0] * len(ref)
        for j, r in enumerate(ref, 1):
            cur[j] = min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (h != r))
        prev = cur
    return prev[-1]


def references(s3, chapter):
    """{utterance id: reference text} from SPK-CHAP.trans.txt."""
    spk, chap = chapter.rstrip("/").split("/")[-2:]
    body = s3.get_object(Bucket="audio", Key=f"{chapter}/{spk}-{chap}.trans.txt")["Body"].read()
    return dict(line.split(" ", 1) for line in body.decode().splitlines() if " " in line)


def main():
    chapter = sys.argv[1].rstrip("/")
    model = sys.argv[2] if len(sys.argv) > 2 else "base.en"
    threads = int(sys.argv[3]) if len(sys.argv) > 3 else 8
    s3 = s3util.client()
    t0 = time.time()
    mfile = model_path(s3, model)
    print(f"model {mfile} ready in {time.time() - t0:.1f} s")
    refs = references(s3, chapter)
    keys = [k for k in s3util.list_keys(s3, "audio", chapter + "/") if k.endswith(".flac")]
    audio = errors = nref = 0
    t0 = time.time()
    for k in keys:
        text, sec, _ = transcribe(s3, k, mfile, threads)
        utt = os.path.basename(k)[:-5]
        r = words(refs.get(utt, ""))
        errors += edit_distance(words(text), r)
        nref += len(r)
        audio += sec
    wall = time.time() - t0
    print(f"{chapter}: {len(keys)} utterances, {audio:.1f} s of audio in {wall:.1f} s "
          f"-> {audio / wall:.1f}x real time, WER {errors / max(nref, 1):.3%} ({model}, {threads} threads)")


if __name__ == "__main__":
    main()
