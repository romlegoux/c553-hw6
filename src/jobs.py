#!/usr/bin/env python3
"""Part 9 tasks run by the worker: transcription, fan-in, merge.

Task commands:
  transcribe JOB NTASKS MODEL KEY            one utterance (KEY in the audio bucket)
  transcribe-chapter JOB NTASKS MODEL CHAP   every utterance of a chapter
  merge JOB MODEL CHAPTER                    concatenate, WER, summary.json
"""

import json
import os
import threading
import time

import common
import s3util
import transcribe

KINDS = ("transcribe", "transcribe-chapter", "merge")
_lock = threading.Lock()
_st = {}


def is_job(cmd):
    return cmd.split(" ", 1)[0] in KINDS


def statements(session):
    with _lock:
        if not _st:
            p = session.prepare
            _st.update(
                done=p("INSERT INTO job_tasks (run, job, task, audio_s, bytes_read, bytes_written) "
                       "VALUES (?, ?, ?, ?, ?, ?)"),
                count=p("SELECT COUNT(*) FROM job_tasks WHERE run = ? AND job = ?"),
                rows=p("SELECT audio_s, bytes_read, bytes_written FROM job_tasks "
                       "WHERE run = ? AND job = ?"),
                job=p("SELECT submitted FROM jobs WHERE run = ? AND job = ?"))
    return _st


def upload_text(s3, job, key, text):
    body = (text + "\n").encode()
    utt = os.path.basename(key)[:-len(".flac")]
    s3.put_object(Bucket="transcripts", Key=f"{job}/{utt}.txt", Body=body)
    return len(body)


def run(cmd, run_name, tid, ctx):
    """Returns (status, start, end, extra result fields, follow-up tasks [(id, cmd)])."""
    if ctx.get("s3") is None:
        ctx["s3"] = s3util.client()
    s3, session, x = ctx["s3"], ctx["session"], ctx["x"]
    st = statements(session)
    kind, rest = cmd.split(" ", 1)
    start = time.time()
    if kind == "merge":
        return merge(rest, run_name, s3, session, st, start)

    job, ntasks, model, target = rest.split(" ", 3)
    mfile = transcribe.model_path(s3, model)
    if kind == "transcribe":
        keys, chapter = [target], os.path.dirname(target)
    else:
        chapter = target
        keys = [k for k in s3util.list_keys(s3, "audio", chapter + "/") if k.endswith(".flac")]
    audio = nread = nwritten = 0
    for k in keys:
        text, sec, r = transcribe.transcribe(s3, k, mfile, x)
        nwritten += upload_text(s3, job, k, text)
        audio, nread = audio + sec, nread + r
    end = time.time()

    # Fan-in: one row per task (a repeated completion rewrites it), then count.
    session.execute(st["done"], (run_name, job, tid, audio, nread, nwritten))
    n = session.execute(st["count"], (run_name, job)).one()[0]
    follow = [(f"merge-{job}", f"merge {job} {model} {chapter}")] if n >= int(ntasks) else []
    return 0, start, end, {"audio_s": audio}, follow


def merge(rest, run_name, s3, session, st, start):
    job, model, chapter = rest.split(" ")
    rows = list(session.execute(st["rows"], (run_name, job)))
    keys = s3util.list_keys(s3, "transcripts", job + "/")
    refs = transcribe.references(s3, chapter)
    texts, errors, nref, nread = [], 0, 0, 0
    for k in keys:
        body = s3.get_object(Bucket="transcripts", Key=k)["Body"].read()
        nread += len(body)
        text = body.decode().strip()
        ref = transcribe.words(refs.get(os.path.basename(k)[:-len(".txt")], ""))
        errors += transcribe.edit_distance(transcribe.words(text), ref)
        nref += len(ref)
        texts.append(text)
    transcript = ("\n".join(texts) + "\n").encode()
    submitted = session.execute(st["job"], (run_name, job)).one().submitted
    summary = {
        "header": common.provenance_header(),
        "job": job, "chapter": chapter, "model": model,
        "utterances": len(keys), "missing": len(refs) - len(keys),
        "audio_seconds": round(sum(r.audio_s for r in rows), 3),
        "reference_words": nref, "word_errors": errors,
        "wer": round(errors / max(nref, 1), 5),
        "processing_time": round(time.time() - submitted, 3),
        "bytes_read": sum(r.bytes_read for r in rows) + nread,
        "bytes_written": sum(r.bytes_written for r in rows) + len(transcript),
    }
    body = json.dumps(summary, indent=1).encode()
    summary["bytes_written"] += len(body)
    body = json.dumps(summary, indent=1).encode()
    s3.put_object(Bucket="results", Key=f"{job}/transcript.txt", Body=transcript)
    s3.put_object(Bucket="results", Key=f"{job}/summary.json", Body=body)
    extra = {"job": job, "transcript": f"results/{job}/transcript.txt", "summary": summary}
    return 0, start, time.time(), extra, []
