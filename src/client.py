#!/usr/bin/env python3
import argparse
import queue
import sys
import threading
import time

import common
# sentinel: a worker thread that removes it exits
STOP = None   



# Part 3 — local mode

def local_worker(name, tasks, results):
    """Thread body: remove a task, run it, insert the result, until STOP."""
    while True:
        task = tasks.get()                       # task = remove()  (blocks)
        if task is STOP:
            return
        tid, cmd = task
        status, start, end = common.run_task(cmd)
        results.put({"id": tid, "status": status, "worker": name,
                     "start": start, "end": end, "attempts": 1})


def run_local(args, cmds):
    """Submit every task to the in-memory pool and collect one result per task.
    Returns (records, first_publish, last_result, duplicates)."""
    tasks, results = queue.Queue(), queue.Queue()
    host = common.hostname()
    threads = [threading.Thread(target=local_worker,
                                args=(f"{host}/local-t{i}", tasks, results),
                                daemon=True)
               for i in range(args.threads)]
    for t in threads:
        t.start()

    cid = common.client_id()
    records = {}

    # Submission.
    first_publish = time.time()
    for seq, cmd in enumerate(cmds):
        tid = common.task_id(cid, seq)
        records[tid] = common.new_record(tid, cmd, time.time())
        tasks.put((tid, cmd))
    for _ in threads:
        tasks.put(STOP)

    # Collection: exactly one result per task
    last_result = first_publish
    for _ in range(len(cmds)):
        res = results.get()
        last_result = time.time()
        records[res["id"]].update(start=res["start"], end=res["end"],
                                  arrival=last_result, worker=res["worker"],
                                  attempts=res["attempts"], status=res["status"])

    for t in threads:
        t.join()
    return records, first_publish, last_result, 0


# ---------------------------------------------------------------------------
# Part 2 — remote mode (to do)
# ---------------------------------------------------------------------------

def run_remote(args, cmds):
    sys.exit("remote mode not implemented yet (Part 2)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="CS553 HW6 client")
    p.add_argument("-s", dest="qname", required=True,
                   help="LOCAL for the in-process pool, otherwise the run name QNAME")
    p.add_argument("-w", dest="workload", required=True, help="workload file (sleep MS per line)")
    p.add_argument("-t", dest="threads", type=int, default=1, help="LOCAL only: number of threads")
    p.add_argument("-d", dest="dup", type=float, default=0.0,
                   help="remote only: probability of publishing a second copy")
    p.add_argument("-o", dest="output", help="per-task CSV output file")
    args = p.parse_args()
    if args.threads < 1:
        p.error("-t must be >= 1")
    if not 0.0 <= args.dup <= 1.0:
        p.error("-d must be between 0 and 1")
    return args


def main():
    args = parse_args()
    cmds = common.read_workload(args.workload)
    if not cmds:
        sys.exit("ERROR: empty workload")

    if args.qname == "LOCAL":
        records, first, last, dups = run_local(args, cmds)
    else:
        records, first, last, dups = run_remote(args, cmds)

    common.write_summary(common.summary(records, first, last, dups))
    if args.output:
        common.write_csv(records, args.output)
    sys.exit(common.exit_code(records))


if __name__ == "__main__":
    main()
