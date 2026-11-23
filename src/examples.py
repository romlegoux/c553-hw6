#!/usr/bin/env python3
import json
import sys
import s3util
import transcribe

model, jobs = sys.argv[1], sys.argv[2:]
s3 = s3util.client()
for job in jobs:
    summ = json.loads(s3.get_object(Bucket="results", Key=f"{job}/summary.json")["Body"].read())
    if summ["model"] != model:
        sys.exit(f"ERROR: transcripts of {job} are {summ['model']}, not {model}")
    refs = transcribe.references(s3, summ["chapter"])
    bad = []
    for key in s3util.list_keys(s3, "transcripts", job + "/"):
        utt = key.split("/")[-1][:-4]
        hyp = transcribe.words(s3.get_object(Bucket="transcripts", Key=key)["Body"].read().decode())
        ref = transcribe.words(refs[utt])
        if hyp != ref:
            bad.append((len(ref), utt, " ".join(ref), " ".join(hyp)))
    _, utt, ref, hyp = min(bad)
    print(f"{job} ({utt})\n  ref: {ref}\n  hyp: {hyp}")
