#!/usr/bin/env python3
"""Turn the raw results of Part 7 into tables and plots.

  python3 src/analyze.py [RESULTS_DIR]      (default: results)

Writes RESULTS_DIR/analysis/tables.md and RESULTS_DIR/analysis/*.png.
"""

import glob
import math
import os
import re
import sys
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

RES = sys.argv[1] if len(sys.argv) > 1 else "results"
OUT = os.path.join(RES, "analysis")
ST = os.path.join(RES, "static")
EL = os.path.join(RES, "elastic")
MD = []


def emit(line=""):
    MD.append(line)


def table(headers, rows):
    emit("| " + " | ".join(headers) + " |")
    emit("|" + "|".join("---" for _ in headers) + "|")
    for r in rows:
        emit("| " + " | ".join(str(x) for x in r) + " |")
    emit()


def one(pattern):
    """The single file matching pattern, plain or gzipped."""
    found = sorted(glob.glob(pattern) + glob.glob(pattern + ".gz"))
    return found[0] if found else None


def reps(directory, name):
    """Sorted list of the run names NAME-r1, NAME-r2, ... in directory."""
    outs = sorted(glob.glob(os.path.join(directory, f"{name}-r[0-9].out")))
    return [o[:-4] for o in outs]


def read_out(base):
    s = {}
    for line in open(base + ".out"):
        m = re.match(r"^(tasks|successes|failures|duplicates ignored|time \(s\)|"
                     r"throughput \(t/s\)|messages published)\s*:\s*([\d.]+)", line)
        if m:
            s[m.group(1).split(" ")[0]] = float(m.group(2))
    return s


def read_csv(base):
    return pd.read_csv(one(base + ".csv"), comment="#")


def read_counters(base):
    """{name: total} and {(vm, name): total} from a cqlsh counters dump."""
    tot, per_vm = {}, {}
    for line in open(base + ".counters"):
        parts = [p.strip() for p in line.split("|")]
        if len(parts) == 3 and parts[2].isdigit():
            vm = parts[0].split("/")[0]
            tot[parts[1]] = tot.get(parts[1], 0) + int(parts[2])
            per_vm[(vm, parts[1])] = per_vm.get((vm, parts[1]), 0) + int(parts[2])
    return tot, per_vm


def read_mpstat(path):
    """Mean busy % (100 - idle) and mean iowait % of the 'all' lines."""
    busy, iow = [], []
    for line in open(path):
        f = line.split()
        if len(f) == 12 and f[1] == "all":
            busy.append(100 - float(f[11]))
            iow.append(float(f[5]))
    return (np.mean(busy), np.mean(iow)) if busy else (float("nan"), float("nan"))


def read_prov(path):
    """(per-second DataFrame with an epoch column, list of events)."""
    rows, events = [], []
    for line in open(path):
        if line.startswith("# event"):
            f = line.split()
            extra = dict(kv.split("=") for kv in f[5:] if "=" in kv)
            events.append({"t": float(f[2]), "kind": f[3], "vm": f[4], **extra})
        elif re.match(r"^\d{4}-\d\d-\d\dT", line):
            f = line.rstrip("\n").split(",")
            t = datetime.strptime(f[0], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            rows.append([t.timestamp()] + [int(x) for x in f[1:8]])
    df = pd.DataFrame(rows, columns=["t", "ready", "unacked", "consumers", "active",
                                     "booting", "leaving", "total"])
    return df, events


def read_queues(path):
    """Median ready and unacked messages of QNAME.tasks over the run's snapshots."""
    ready, unacked = [], []
    for line in open(path):
        if line.startswith("#"):
            continue
        for q in line.split()[1:]:
            f = q.split(",")
            if f[0].endswith(".tasks") and len(f) >= 5:
                ready.append(int(f[2]))
                unacked.append(int(f[3]))
    inner = slice(1, -1) if len(ready) > 2 else slice(None)
    return float(np.median(ready[inner])), float(np.median(unacked[inner]))


def ms(values, fmt="{:.1f}"):
    """'mean ± std' over the repetitions (std omitted for a single run)."""
    v = [x for x in values if x is not None and not math.isnan(x)]
    if not v:
        return "-"
    if len(v) == 1:
        return fmt.format(v[0]) + " (1 run)"
    return fmt.format(np.mean(v)) + " ± " + fmt.format(np.std(v, ddof=1))


def summary_stats(runs):
    outs = [read_out(r) for r in runs]
    return [o.get("time") for o in outs], [o.get("throughput") for o in outs], outs


# --------------------------------------------------------------------------- 7a

def part_7a():
    emit("## 7a. Throughput (100,000 × sleep 0)")
    emit()
    rows, plot = [], {"Local": [], "Remote": []}
    for t in (1, 2, 4, 8):
        times, thr, _ = summary_stats(reps(f"{ST}/local", f"7a-local-t{t}"))
        rows.append([f"Local, {t} thread{'s' * (t > 1)}", t, 100000, ms(times, "{:.2f}"), ms(thr)])
        plot["Local"].append((t, np.mean(thr), np.std(thr, ddof=1) if len(thr) > 1 else 0))
    for w in (1, 2, 4, 8):
        times, thr, _ = summary_stats(reps(f"{ST}/w{w}", f"7a-w{w}"))
        rows.append([f"Remote, {w} worker VM{'s' * (w > 1)}", 8 * w, 100000, ms(times), ms(thr)])
        plot["Remote"].append((8 * w, np.mean(thr), np.std(thr, ddof=1) if len(thr) > 1 else 0))
    table(["Mode", "Threads", "Tasks", "Time (s)", "Throughput (tasks/s)"], rows)

    emit("Evidence for Q1 (means over the repetitions): CPU of svc and of the workers "
         "(mpstat, 'all' line); backlog of QNAME.tasks (median of the 5 s list_queues "
         "snapshots); the client's Cassandra insert rate (the 'submitted' column: inserts are "
         "issued at this rate, tasks are published only once their insert is acknowledged).")
    emit()
    rows = []
    for w in (1, 2, 4, 8):
        svc_b, svc_w, wk_b, ins, rdy, una, retries = [], [], [], [], [], [], []
        for r in reps(f"{ST}/w{w}", f"7a-w{w}"):
            b, i = read_mpstat(r + ".mpstat-svc")
            svc_b.append(b)
            svc_w.append(i)
            wk = [read_mpstat(p)[0] for p in glob.glob(r + ".mpstat-ckw*")]
            wk_b.append(np.mean(wk) if wk else float("nan"))
            df = read_csv(r)
            ins.append(len(df) / (df.submitted.max() - df.submitted.min()))
            q = read_queues(r + ".queues")
            rdy.append(q[0])
            una.append(q[1])
            retries.append(sum(1 for line in open(r + ".out") if line.startswith("# insert of")))
        rows.append([w, ms(svc_b), ms(svc_w), ms(wk_b), ms(rdy, "{:.0f}"),
                     f"{ms(una, '{:.0f}')} of {8 * w}", ms(ins, "{:.0f}"),
                     " / ".join(str(x) for x in retries)])
    table(["Worker VMs", "svc CPU busy (%)", "svc iowait (%)", "worker CPU busy (%)",
           "ready messages (median)", "unacked (median) of threads",
           "client insert rate (tasks/s)", "retried inserts per run"], rows)
    emit("Retried inserts: once in some runs the Cassandra driver marked svc down "
         "('Host ... has been marked down'); the 512 writes in flight failed together, failed "
         "again after 1 s and succeeded after 2 more s: a ~3 s pause of the submission.")
    emit()

    fig, ax = plt.subplots(figsize=(6, 4))
    for label, pts in plot.items():
        x, y, e = zip(*pts)
        ax.errorbar(x, y, yerr=e, marker="o", capsize=4, label=label)
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xticks([1, 2, 4, 8, 16, 32, 64])
    ax.set_xticklabels([1, 2, 4, 8, 16, 32, 64])
    ax.set_xlabel("threads")
    ax.set_ylabel("throughput (tasks/s, log)")
    ax.set_title("7a: throughput of sleep 0 tasks")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(f"{OUT}/7a-throughput.png", dpi=150)
    plt.close(fig)


# --------------------------------------------------------------------------- 7b

LENGTHS = [("10ms", 10, 1000), ("1s", 100, 100), ("10s", 100, 10)]   # name, ideal s, tasks/thread


def part_7b():
    emit("## 7b. Efficiency (ideal time / measured time)")
    emit()
    rows, eff, ovh = [], {n: [] for n, _, _ in LENGTHS}, []
    for w in (1, 2, 4, 8):
        row, orow = [w], [w]
        for name, ideal, per_thread in LENGTHS:
            times, _, _ = summary_stats(reps(f"{ST}/w{w}", f"7b-{name}-w{w}"))
            e = [ideal / t for t in times]
            row += [ms(times), ms(e, "{:.2f}")]
            eff[name].append((w, np.mean(e), np.std(e, ddof=1) if len(e) > 1 else 0))
            orow.append(ms([(t - ideal) / per_thread * 1000 for t in times]))
        rows.append(row)
        ovh.append(orow)
    table(["Worker VMs", "10ms: time (s)", "10ms: eff.", "1s: time (s)", "1s: eff.",
           "10s: time (s)", "10s: eff."], rows)
    emit("Evidence for Q2: overhead per task = (measured − ideal) / tasks per thread, in ms.")
    emit()
    table(["Worker VMs", "10ms", "1s", "10s"], ovh)

    fig, ax = plt.subplots(figsize=(6, 4))
    for name, pts in eff.items():
        x, y, e = zip(*pts)
        ax.errorbar(x, y, yerr=e, marker="o", capsize=4, label=f"{name} tasks")
    ax.set_xscale("log", base=2)
    ax.set_xticks([1, 2, 4, 8])
    ax.set_xticklabels([1, 2, 4, 8])
    ax.set_ylim(0, 1.05)
    ax.set_xlabel("worker VMs (8 threads each)")
    ax.set_ylabel("efficiency")
    ax.set_title("7b: efficiency against the number of workers")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(f"{OUT}/7b-efficiency.png", dpi=150)
    plt.close(fig)


# --------------------------------------------------------------------------- 7c

def per_vm(df):
    """Tasks and busy time (sum of run times) per worker VM."""
    d = df.assign(vm=df.worker.str.split("/").str[0], run=df.end - df.start)
    g = d.groupby("vm")
    return g.size(), g.run.sum()


def part_7c():
    emit("## 7c. Prefetch and load balancing (8 worker VMs)")
    emit()
    cases = [("sleep 0, 100,000", 1, f"{ST}/w8", "7a-w8"),
             ("sleep 0, 100,000", 10, f"{ST}/p10", "7c-sleep0-p10"),
             ("sleep 0, 100,000", 100, f"{ST}/p100", "7c-sleep0-p100"),
             ("Mixed, 4,000", 1, f"{ST}/w8", "7c-mixed-p1"),
             ("Mixed, 4,000", 100, f"{ST}/p100", "7c-mixed-p100")]
    rows, busy_plot = [], {}
    for label, p, d, name in cases:
        runs = reps(d, name)
        times, thr, _ = summary_stats(runs)
        tmin, tmax, bmin, bmax = [], [], [], []
        for r in runs:
            n, b = per_vm(read_csv(r))
            tmin.append(n.min()); tmax.append(n.max())
            bmin.append(b.min()); bmax.append(b.max())
            if label.startswith("Mixed") and r.endswith("-r1"):
                busy_plot[p] = b
        if label.startswith("Mixed"):
            score = ms([37.1875 / t for t in times], "{:.2f}") + " (eff.)"
        else:
            score = ms(thr) + " (tasks/s)"
        rows.append([label, p, ms(times), score,
                     f"{ms(tmin, '{:.0f}')} / {ms(tmax, '{:.0f}')}",
                     f"{ms(bmin)} / {ms(bmax)}"])
    table(["Workload", "Prefetch", "Time (s)", "Tasks/s or eff.",
           "Tasks per worker (min / max)", "Busy time per worker (min / max, s)"], rows)
    emit("The sleep 0 run with prefetch 1 is the 7a run on 8 workers. Ideal makespan "
         "of the mixed workload: 2,380 thread-seconds / 64 threads = 37.2 s.")
    emit()

    if busy_plot:
        fig, ax = plt.subplots(figsize=(7, 4))
        vms = sorted(set().union(*[b.index for b in busy_plot.values()]),
                     key=lambda v: int(re.sub(r"\D", "", v) or 0))
        x = np.arange(len(vms))
        for i, (p, b) in enumerate(sorted(busy_plot.items())):
            ax.bar(x + (i - 0.5) * 0.4, [b.get(v, 0) for v in vms], 0.4, label=f"prefetch {p}")
        ax.axhline(2380 / 8, ls="--", c="gray", label="perfect balance (297.5 s)")
        ax.set_xticks(x)
        ax.set_xticklabels(vms)
        ax.set_ylabel("busy time (s)")
        ax.set_title("7c: busy time per worker VM, mixed workload (run 1)")
        ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.1), ncol=3)
        fig.tight_layout()
        fig.savefig(f"{OUT}/7c-busy-time.png", dpi=150)
        plt.close(fig)


# --------------------------------------------------------------------------- 7d

def part_7d():
    emit("## 7d. Duplicates and failures (8 worker VMs)")
    emit()
    rows, plot = [], {}
    for label, name in [("(i) -d 0.1, 10,000 tasks", "7d-i-dup"),
                        ("(ii) 1,600 tasks, no failure", "7d-ii-nofail"),
                        ("(ii) 1,600 tasks, kill -9", "7d-ii-kill")]:
        runs = reps(f"{ST}/w8", name)
        times, _, outs = summary_stats(runs)
        cnt = [read_counters(r)[0] for r in runs]
        dfs = [read_csv(r) for r in runs]
        g = lambda key: ms([c.get(key, 0) for c in cnt], "{:.0f}")
        redeliv = [c.get("deliveries", 0) - o.get("messages", 0) for c, o in zip(cnt, outs)]
        rows.append([label, ms(times), ms([o.get("messages") for o in outs], "{:.0f}"),
                     g("deliveries"), g("duplicates"),
                     f"{g('executions')} ({g('reexecutions')})",
                     ms([o.get("duplicates") for o in outs], "{:.0f}"),
                     ms([df.id.nunique() for df in dfs], "{:.0f}"),
                     ms(redeliv, "{:.0f}")])
        if name.startswith("7d-ii"):
            df = dfs[0]
            kill = None
            if os.path.exists(runs[0] + ".kill"):
                kill = float(open(runs[0] + ".kill").read().split()[-1])
            plot[label] = (df, kill)
    table(["Run", "Time (s)", "Published", "Deliveries", "Duplicates discarded",
           "Executions (re-runs)", "Duplicate results", "Distinct results",
           "Redeliveries (deliveries − published)"], rows)
    emit("Counters are summed over every worker process of the run, including the killed one.")
    emit()

    fig, ax = plt.subplots(figsize=(7, 4))
    for label, (df, kill) in plot.items():
        t0 = df.submitted.min()
        sec = np.floor(df.arrival - t0).astype(int)
        c = sec.value_counts().sort_index()
        c = c.reindex(range(0, sec.max() + 1), fill_value=0)
        ax.plot(c.index, c.values, label=label.replace("(ii) 1,600 tasks, ", ""))
        if kill:
            ax.axvline(kill - t0, c="red", ls="--", label=f"kill -9 ckw8 (t = {kill - t0:.1f} s)")
    ax.set_xlabel("seconds since the first submission")
    ax.set_ylabel("tasks completed per second")
    ax.set_title("7d (ii): 1,600 × sleep 1000 on 8 workers (run 1)")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(f"{OUT}/7d-completions.png", dpi=150)
    plt.close(fig)


# --------------------------------------------------------------------------- 7e

PHASES = [(60, 120, 12), (180, 120, 40), (300, 120, 16), (540, 20, 150), (720, 120, 24)]
RUNS_7E = [("i", "(i) min 0, idle 30s"), ("ii", "(ii) min 0, idle 120s"),
           ("iii", "(iii) min 2, idle 30s"), ("iv", "(iv) static, min = max = 8")]


def needed(t):
    for start, dur, rate in PHASES:
        if start <= t < start + dur:
            return min(8, math.ceil(rate * 1.0 / 8))
    return 0


def part_7e():
    emit("## 7e. Elasticity (elastic.txt, 14,040 × sleep 1000)")
    emit()
    rows = []
    for run, label in RUNS_7E:
        base = f"{EL}/7e-{run}"
        if not os.path.exists(base + ".out"):
            continue
        s = read_out(base)
        df = read_csv(base)
        lat = df.arrival - df.submitted
        prov, ev = read_prov(f"{EL}/prov-el-{run}.log")
        starts = [e for e in ev if e["kind"] == "launch"]
        vmsec = sum(float(e["vm_seconds"]) for e in ev if e["kind"] == "deleted")
        cold = [float(e["cold_start"]) for e in ev if e["kind"] == "connected"]
        cs = f"{min(cold):.1f} / {np.mean(cold):.1f} / {max(cold):.1f}" if cold else "-"
        rows.append([label, f"{s['time']:.1f}", f"{lat.mean():.2f}", f"{lat.quantile(0.95):.2f}",
                     len(starts), f"{vmsec:.0f}", cs])
        if run != "iv":
            plot_7e(run, label, df, prov, ev)
    table(["Run", "Time (s)", "Mean latency (s)", "p95 latency (s)", "VM starts",
           "VM-seconds", "Cold start (s) (min / mean / max)"], rows)
    emit("Latency = arrival of the result − submission (two different clocks: tiny for both). "
         "VM-seconds = Σ over worker VMs of (deletion − launch), from the provisioner's events. "
         "Cold start = provisioner's lxc launch → first RabbitMQ connection of the VM's worker.")
    emit()


def plot_7e(run, label, df, prov, ev):
    t0 = df.submitted.min() - PHASES[0][0]           # client start: first task at +60 s
    end = max(df.arrival.max(), prov.t.max()) - t0
    x = np.arange(0, int(end) + 1)
    fig, axs = plt.subplots(3, 1, figsize=(10, 9), sharex=True)

    p = prov.assign(x=prov.t - t0)
    axs[0].step(p.x, p.active, where="post", label="running (connected)")
    axs[0].step(p.x, p.booting, where="post", label="booting")
    axs[0].plot(x, [needed(t) for t in x], "k--", lw=1, label="needed by the load")
    axs[0].set_ylabel("worker VMs")
    axs[0].set_ylim(-0.2, 8.8)

    bins = np.arange(0, end + 5, 5)
    done, _ = np.histogram(df.arrival - t0, bins)
    offered, _ = np.histogram(df.submitted - t0, bins)
    axs[1].step(bins[:-1], done / 5, where="post", label="throughput (completed)")
    axs[1].step(bins[:-1], offered / 5, where="post", ls=":", label="offered load (submitted)")
    axs[1].set_ylabel("tasks/s (5 s bins)")

    axs[2].plot(p.x, p.ready, label="ready messages")
    axs[2].set_ylabel("backlog")
    axs[2].set_xlabel("seconds since the client started")

    for ax in axs:
        for start, _, rate in PHASES:
            ax.axvline(start, c="gray", ls=":", lw=0.8)
        ax.grid(True, alpha=0.3)
        ax.legend(loc="upper right", fontsize=8)
    for start, _, rate in PHASES:
        axs[0].text(start + 2, 8.3, f"{rate}/s", fontsize=8)
    axs[0].set_title(f"7e {label}")
    fig.tight_layout()
    fig.savefig(f"{OUT}/7e-{run}.png", dpi=150)
    plt.close(fig)


# --------------------------------------------------------------------------- Q5, Q6, clocks

def part_q5_q6():
    emit("## Evidence for Q5 (local vs one remote VM, 8 threads) and Q6 (durability)")
    emit()
    rows = []
    for label, d, name in [("Local, 8 threads, sleep 0", f"{ST}/local", "7a-local-t8"),
                           ("Remote, 1 VM, sleep 0", f"{ST}/w1", "7a-w1"),
                           ("Local, 8 threads, 10ms", f"{ST}/local", "7b-local-10ms-t8"),
                           ("Remote, 1 VM, 10ms", f"{ST}/w1", "7b-10ms-w1")]:
        runs = reps(d, name)
        if not runs:
            rows.append([label, "not measured", "-", "-"])
            continue
        times, thr, outs = summary_stats(runs)
        per_task = [t / o["tasks"] * 8 * 1000 for t, o in zip(times, outs)]
        rows.append([label, ms(times, "{:.2f}"), ms(thr), ms(per_task, "{:.2f}")])
    table(["Run", "Time (s)", "Throughput (tasks/s)", "Time per task per thread (ms)"], rows)

    rows = []
    for label, d, name in [("Durable: quorum, persistent, confirms", f"{ST}/w8", "7a-w8"),
                           ("Fast: classic, transient, no confirms", f"{ST}/q6", "q6-fast-w8")]:
        runs = reps(d, name)
        if not runs:
            continue
        times, thr, _ = summary_stats(runs)
        svc = [read_mpstat(r + ".mpstat-svc") for r in runs if os.path.exists(r + ".mpstat-svc")]
        rows.append([label, ms(times), ms(thr), ms([b for b, _ in svc]), ms([i for _, i in svc])])
    table(["Q6: 100,000 × sleep 0 on 8 workers", "Time (s)", "Throughput (tasks/s)",
           "svc CPU busy (%)", "svc iowait (%)"], rows)


def part_clocks():
    emit("## Clocks (chronyc tracking / timedatectl, at the start of each session)")
    emit()
    rows = []
    for path in sorted(glob.glob(f"{ST}/*/clocks-*.txt") + glob.glob(f"{EL}/clocks-*.txt")):
        offs, unsynced, vm = {}, [], None
        for line in open(path):
            if line.startswith("== "):
                vm = line[3:].strip()
            m = re.search(r"System time\s*:\s*([\d.]+) seconds (fast|slow)", line)
            if m and vm:
                offs[vm] = float(m.group(1)) * 1000 * (1 if m.group(2) == "fast" else -1)
            m = re.search(r"Offset:\s*([+-]?[\d.]+)(us|ms|s)\b", line)
            if m and vm:
                offs[vm] = float(m.group(1)) * {"us": 0.001, "ms": 1, "s": 1000}[m.group(2)]
            if re.search(r"Packet count:\s*0\b", line) and vm:
                unsynced.append(vm)
        stable = {k: v for k, v in offs.items() if not k.startswith("ckw")}
        workers = {k: v for k, v in offs.items() if k.startswith("ckw")}
        fmt = lambda d: (lambda k: f"{k}: {d[k]:+.1f} ms")(max(d, key=lambda k: abs(d[k]))) if d else "-"
        rows.append([os.path.basename(path)[7:-4], fmt(stable), fmt(workers),
                     ", ".join(unsynced) or "-"])
    table(["Session", "Largest offset, node/tiny/svc", "Largest offset, workers (just booted)",
           "Not synced yet (0 NTP packets)"], rows)
    emit("The worker VMs had just booted when their clocks were read, and systemd-timesyncd "
         "had made at most one measurement: their offsets (up to ~0.9 s) are the error before "
         "the first correction. Hence no metric above compares a worker's time with tiny's: "
         "latency, throughput and completions use tiny's clock only (submitted, arrival), busy "
         "time uses the worker's clock only (start, end).")
    emit()


def main():
    os.makedirs(OUT, exist_ok=True)
    emit("# CS553 HW6 — Part 7 results")
    emit()
    emit("Generated by src/analyze.py from the raw files in results/. "
         "Values are mean ± standard deviation over the repetitions.")
    emit()
    for part in (part_7a, part_7b, part_7c, part_7d, part_7e, part_q5_q6, part_clocks):
        part()
    with open(f"{OUT}/tables.md", "w") as f:
        f.write("\n".join(MD) + "\n")
    print(f"{OUT}/tables.md and {len(glob.glob(OUT + '/*.png'))} plots written")


if __name__ == "__main__":
    main()
