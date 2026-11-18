#!/usr/bin/env python3
"""CS553 HW6 client, run on tiny.

  client -s LOCAL -t N -w WORKLOAD [-o CSV]
  client -s QNAME (-w WORKLOAD | -S SCHEDULE) [-d P] [-o CSV]
"""

import argparse
import logging
import os
import queue
import random
import sys
import threading
import time

import common
import mgmt

STOP = None

MAX_INFLIGHT = 512     # max concurrent Cassandra writes
NUM_PUBLISHERS = 4
RESULTS_PREFETCH = 1000
DUP_SEED = 553         # fixed seed for -d
INSERT_TRIES = 5


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
    """Submit every task to the in-memory pool and collect one result per task."""
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

    first_publish = time.time()
    for seq, cmd in enumerate(cmds):
        tid = common.task_id(cid, seq)
        records[tid] = common.new_record(tid, cmd, time.time())
        tasks.put((tid, cmd))                    # insert(task)
    for _ in threads:                            # one STOP per thread, after the tasks
        tasks.put(STOP)

    last_result = first_publish
    for _ in range(len(cmds)):
        res = results.get()
        last_result = time.time()
        records[res["id"]].update(start=res["start"], end=res["end"],
                                  arrival=last_result, worker=res["worker"],
                                  attempts=res["attempts"], status=res["status"])

    for t in threads:
        t.join()
    return records, first_publish, last_result, 0, len(cmds)


def fatal(msg):
    """Abort the whole client from any thread (sys.exit only ends one thread)."""
    print(f"ERROR: {msg}", file=sys.stderr, flush=True)
    os._exit(1)


def check_queues(qname):
    """Passive declare: never creates a queue."""
    import pika
    conn = common.rabbit_connect()
    for suffix in ("tasks", "results"):
        q = f"{qname}.{suffix}"
        try:
            conn.channel().queue_declare(queue=q, passive=True)
        except pika.exceptions.ChannelClosedByBroker:
            conn.close()
            print(f"ERROR: queue '{q}' does not exist (run setup first)", file=sys.stderr)
            sys.exit(2)
    conn.close()


def collector(qname, records, state, started):
    """Consume QNAME.results until every task has one result."""
    conn = common.rabbit_connect()
    ch = conn.channel()
    ch.basic_qos(prefetch_count=RESULTS_PREFETCH)
    total = len(records)

    def on_result(ch, method, props, body):
        now = time.time()
        res = common.decode(body)
        rec = records.get(res.get("id")) if res.get("run") == qname else None
        if rec is None:
            state["stray"] += 1                  # not one of our tasks
        elif rec["arrival"] is not None:
            state["dups"] += 1
        else:
            rec.update(start=res["start"], end=res["end"], arrival=now,
                       worker=res["worker"], attempts=res["attempts"],
                       status=res["status"])
            state["done"] += 1
            state["last"] = now
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if state["done"] == total:
            ch.stop_consuming()

    ch.basic_consume(queue=f"{qname}.results", on_message_callback=on_result)
    started.set()                                # consuming before the first publish
    ch.start_consuming()
    conn.close()


def publisher(qname, ready, dup_ids, state, lock, durable):
    """Publish each ready task."""
    import pika
    conn = common.rabbit_connect()
    ch = conn.channel()
    if durable:
        ch.confirm_delivery()
    props = pika.BasicProperties(delivery_mode=2 if durable else 1,   # 2 = persistent
                                 content_type="application/json")
    while True:
        try:
            item = ready.get(timeout=1)
        except queue.Empty:
            conn.process_data_events(0)          # keep heartbeats alive while idle
            continue
        if item is STOP:
            break
        tid, cmd = item
        body = common.encode_task(qname, tid, cmd)
        for _ in range(2 if tid in dup_ids else 1):   # -d: second copy, same ID
            with lock:
                if state["first"] is None:
                    state["first"] = time.time()      # timing starts here
            try:
                ch.basic_publish(exchange="", routing_key=f"{qname}.tasks",
                                 body=body, properties=props, mandatory=durable)
            except Exception as e:                    # nack or unroutable
                fatal(f"publish of {tid} failed: {e!r}")
            with lock:
                state["published"] += 1
    conn.close()


def read_schedule(path):
    """Schedule file (Part 6): lines 'START DURATION RATE COMMAND', in seconds after the client starts and tasks per second."""
    plan = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            start, duration, rate, cmd = line.split(maxsplit=3)
            start, duration, rate = float(start), int(duration), int(rate)
            for s in range(duration):
                for j in range(rate):
                    plan.append((start + s + j / rate, cmd))
    plan.sort(key=lambda x: x[0])
    return plan


def run_remote(args, cmds, offsets=None):
    """Returns (records, first_publish, last_result, duplicates, published)."""
    qname = args.qname
    check_queues(qname)
    durable = mgmt.durable_mode(qname)
    print(f"# mode: {'durable (quorum, persistent, confirms)' if durable else 'fast (classic, transient, no confirms)'}",
          flush=True)

    cluster, session = common.cassandra_connect()
    insert = session.prepare(
        "INSERT INTO tasks (run, id, cmd, state, submitted, attempts) "
        "VALUES (?, ?, ?, 'QUEUED', ?, 0)")

    cid = common.client_id()
    tasks = [(common.task_id(cid, seq), cmd) for seq, cmd in enumerate(cmds)]
    records = {tid: common.new_record(tid, cmd, None) for tid, cmd in tasks}
    rng = random.Random(DUP_SEED)
    dup_ids = {tid for tid, _ in tasks if rng.random() < args.dup}

    state = {"first": None, "published": 0,                  # publishers (lock)
             "done": 0, "dups": 0, "stray": 0, "last": None}  # collector only
    lock = threading.Lock()

    started = threading.Event()
    col = threading.Thread(target=collector, args=(qname, records, state, started),
                           daemon=True)
    col.start()
    started.wait()

    ready = queue.Queue()
    pubs = [threading.Thread(target=publisher, args=(qname, ready, dup_ids, state, lock, durable),
                             daemon=True)
            for _ in range(NUM_PUBLISHERS)]
    for p in pubs:
        p.start()

    inflight = threading.BoundedSemaphore(MAX_INFLIGHT)

    def on_written(_result, item):
        inflight.release()
        ready.put(item)

    def submit(item, tries):
        tid, cmd = item
        fut = session.execute_async(insert, (qname, tid, cmd, records[tid]["submitted"]))
        fut.add_callbacks(on_written, on_failed,
                          callback_args=(item,), errback_args=(item, tries))

    def on_failed(exc, item, tries):
        if tries >= INSERT_TRIES:
            fatal(f"Cassandra insert of {item[0]} failed {tries} times: {exc!r}")
        delay = 2 ** (tries - 1)                 # 1, 2, 4, 8 s
        print(f"# insert of {item[0]} failed ({exc!r}), retry {tries} in {delay} s",
              file=sys.stderr, flush=True)
        threading.Timer(delay, submit, args=(item, tries + 1)).start()

    t_start = time.time()
    for i, (tid, cmd) in enumerate(tasks):
        if offsets is not None:
            delay = t_start + offsets[i] - time.time()
            if delay > 0:
                time.sleep(delay)
        inflight.acquire()                       # wait if MAX_INFLIGHT writes pending
        records[tid]["submitted"] = time.time()
        submit((tid, cmd), 1)

    for _ in range(MAX_INFLIGHT):                # all slots back = all writes done
        inflight.acquire()
    for _ in pubs:                               # STOPs land after every task in `ready`
        ready.put(STOP)
    for p in pubs:
        p.join()
    print(f"all {len(tasks)} tasks submitted ({state['published']} messages), "
          f"waiting for results...", flush=True)

    col.join()
    cluster.shutdown()
    if state["stray"]:
        print(f"note: {state['stray']} results from other runs/clients ignored")
    return records, state["first"], state["last"], state["dups"], state["published"]


def parse_args():
    p = argparse.ArgumentParser(description="CS553 HW6 client")
    p.add_argument("-s", dest="qname", required=True,
                   help="LOCAL for the in-process pool, otherwise the run name QNAME")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("-w", dest="workload", help="workload file (sleep MS per line)")
    src.add_argument("-S", dest="schedule",
                     help="remote only: schedule file (START DURATION RATE COMMAND)")
    p.add_argument("-t", dest="threads", type=int, default=1, help="LOCAL only: number of threads")
    p.add_argument("-d", dest="dup", type=float, default=0.0,
                   help="remote only: probability of publishing a second copy")
    p.add_argument("-o", dest="output", help="per-task CSV output file")
    args = p.parse_args()
    if args.threads < 1:
        p.error("-t must be >= 1")
    if not 0.0 <= args.dup <= 1.0:
        p.error("-d must be between 0 and 1")
    if args.schedule and args.qname == "LOCAL":
        p.error("-S is for the remote mode only")
    return args


def main():
    logging.basicConfig(level=logging.WARNING, stream=sys.stderr,
                        format="# %(asctime)s %(name)s %(levelname)s %(message)s")
    args = parse_args()
    offsets = None
    if args.schedule:
        plan = read_schedule(args.schedule)
        offsets, cmds = [o for o, _ in plan], [c for _, c in plan]
    else:
        cmds = common.read_workload(args.workload)
    if not cmds:
        sys.exit("ERROR: empty workload")

    if args.qname == "LOCAL":
        records, first, last, dups, published = run_local(args, cmds)
    else:
        records, first, last, dups, published = run_remote(args, cmds, offsets)

    text = common.summary(records, first, last, dups)
    common.write_summary(text + f"\nmessages published : {published}")
    if args.output:
        common.write_csv(records, args.output)
    sys.exit(common.exit_code(records))


if __name__ == "__main__":
    main()
