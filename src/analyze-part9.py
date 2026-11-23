#!/usr/bin/env python3
import glob
import json
import re
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

D = "results/part9/"
out = []


def stats(run):
    text = open(D + run + ".out").read()
    time = float(re.search(r"^time \(s\)\s*:\s*([\d.]+)", text, re.M).group(1))
    speed = float(re.search(r"^speed \(audio s/s\)\s*:\s*([\d.]+)", text, re.M).group(1))
    wer = float(re.search(r"^overall WER\s*:\s*([\d.]+)", text, re.M).group(1))
    lat = [float(x) for x in re.findall(r"^job \S+ done in ([\d.]+) s", text, re.M)]
    return time, speed, wer, sum(lat) / len(lat)


def busy(run, threads):
    df = pd.read_csv(D + run + ".csv", comment="#")
    vm = df.worker.str.split("/").str[0]
    per_vm = (df.end - df.start).groupby(vm).sum() / threads
    return per_vm.min(), per_vm.max()


def jobs(run):
    return [json.loads(l)["summary"] for l in open(D + run + ".jobs.jsonl") if l.startswith("{")]


out.append("E2 (8 VMs, base.en)\nrun | time (s) | speed | min busy VM (s) | max busy VM (s)")
for run, t in [("e2/e2-utt-1x8", 1), ("e2/e2-utt-2x4", 2), ("e2/e2-utt-4x2", 4),
               ("e2/e2-utt-8x1", 8), ("e2/e2-chapter-8x1", 8)]:
    time, speed, _, _ = stats(run)
    lo, hi = busy(run, t)
    out.append(f"{run[3:]} | {time:.0f} | {speed:.1f} | {lo:.0f} | {hi:.0f}")

out.append("\nE1 (8x1, base.en)\nVMs | time (s) | speed | speedup | efficiency")
runs = {1: "e1/e1-w1-8x1", 2: "e1/e1-w2-8x1", 4: "e1/e1-w4-8x1", 8: "e2/e2-utt-8x1"}
times = {n: stats(r)[0] for n, r in runs.items()}
for n, r in runs.items():
    s = times[1] / times[n]
    out.append(f"{n} | {times[n]:.0f} | {stats(r)[1]:.1f} | {s:.2f} | {100 * s / n:.0f}%")

plt.plot(list(times), [times[1] / t for t in times.values()], "o-", label="measured")
plt.plot([1, 8], [1, 8], "--", color="gray", label="ideal")
plt.xlabel("worker VMs"); plt.ylabel("speedup"); plt.title("E1 speedup (8x1, base.en)")
plt.legend(); plt.grid(alpha=.3); plt.savefig(D + "9e1-speedup.png", dpi=150)

out.append("\nE3 (8 VMs, 8x1)\nmodel | time (s) | speed | WER | mean job latency (s) | read/job (MB) | written/job (kB)")
for model, run in [("base.en", "e2/e2-utt-8x1"), ("tiny.en", "e3/e3-tiny-8x1")]:
    time, speed, wer, lat = stats(run)
    js = jobs(run)
    rd = sum(j["bytes_read"] for j in js) / len(js) / 1e6
    wr = sum(j["bytes_written"] for j in js) / len(js) / 1e3
    out.append(f"{model} | {time:.0f} | {speed:.1f} | {wer:.2f}% | {lat:.0f} | {rd:.2f} | {wr:.1f}")
for model, run in [("base.en", "e2/e2-utt-8x1"), ("tiny.en", "e3/e3-tiny-8x1")]:
    worst = sorted(jobs(run), key=lambda j: -j["wer"])[:5]
    out.append(f"5 worst {model}: " + ", ".join(f"{j['job']} {100 * j['wer']:.1f}%" for j in worst))

out.append("\n9d (kill -9 on ckw8)")
js = jobs("kill/9d-kill-8x1")
df = pd.read_csv(D + "kill/9d-kill-8x1.csv", comment="#")
logs = pd.concat(pd.read_csv(f, comment="#") for f in glob.glob(D + "kill/worker-*.log"))
merges = logs[logs.id.str.startswith("merge") & (logs.outcome == "done")].id.value_counts()
out.append(f"{len(df)} tasks ok, {sum(j['missing'] == 0 for j in js)}/{len(js)} jobs complete, "
           f"merges per job: {merges.min()}-{merges.max()}, time {stats('kill/9d-kill-8x1')[0]:.0f} s")

open(D + "tables.md", "w").write("\n".join(out) + "\n")
print("\n".join(out))
