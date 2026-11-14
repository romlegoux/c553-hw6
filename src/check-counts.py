#!/usr/bin/env python3
import argparse
import subprocess
from collections import Counter
import common


def read_counters(qname):
    cluster, session = common.cassandra_connect()
    totals = Counter()
    for row in session.execute(
            "SELECT name, value FROM counters WHERE run = %s", (qname,)):
        totals[row.name] += row.value
    cluster.shutdown()
    return totals


def read_logs(qname, k):
    """Count the outcomes of every task line in the workers' logs."""
    outcomes = Counter()
    for i in range(1, k + 1):
        try:
            text = subprocess.check_output(["ssh", f"small{i}", f"cat ~/logs/worker-{qname}.log"],
                text=True, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError:
            continue                              # no log on this VM
        for line in text.splitlines():
            if not line or line.startswith("#") or line.startswith("id,"):
                continue
            outcomes[line.rsplit(",", 1)[-1]] += 1
    return outcomes


def main():
    p = argparse.ArgumentParser()
    p.add_argument("qname")
    p.add_argument("-k", type=int, default=8, help="number of small VMs to read")
    args = p.parse_args()

    c = read_counters(args.qname)
    o = read_logs(args.qname, args.k)
    executed = o["done"] + o["lost-lease"]        # tasks that ran to completion

    rows = [
        ("deliveries",   c["deliveries"],   executed + o["dup"] + o["requeue"]),
        ("executions",   c["executions"],   executed),
        ("duplicates",   c["duplicates"],   o["dup"]),
        ("requeues",     c["requeues"],     o["requeue"]),
        ("reexecutions", c["reexecutions"], None),
    ]
    print(common.provenance_header())
    print(f"# run={args.qname} logs=small1..small{args.k}")
    print(f"{'name':<13} {'counter':>8} {'logs':>8} {'diff':>6}")
    for name, cnt, logs in rows:
        if logs is None:
            print(f"{name:<13} {cnt:>8} {'-':>8} {'-':>6}")
        else:
            print(f"{name:<13} {cnt:>8} {logs:>8} {cnt - logs:>6}")
    if o["lost-lease"]:
        print(f"# lost-lease lines: {o['lost-lease']}")


if __name__ == "__main__":
    main()
