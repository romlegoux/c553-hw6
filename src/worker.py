#!/usr/bin/env python3

import argparse
import logging
import os
import signal
import sys
import threading
import time

import common

CLAIM_TTL = 30
RENEW_EVERY = 10
REQUEUE_DELAY = 2.0

stop = threading.Event()   # set by SIGTERM/SIGINT, or when idle too long (-i)
activity_lock = threading.Lock()
last_activity = time.time()
busy = 0


def activity(delta):
    global last_activity, busy
    with activity_lock:
        busy += delta
        last_activity = time.time()



# Cassandra statements

def prepare(session):
    p = session.prepare
    return {
        "claim":   p("INSERT INTO claims (run, id, worker) VALUES (?, ?, ?) "
                     f"IF NOT EXISTS USING TTL {CLAIM_TTL}"),
        "renew":   p(f"UPDATE claims USING TTL {CLAIM_TTL} SET worker = ? "
                     "WHERE run = ? AND id = ? IF worker = ?"),
        "done":    p("UPDATE claims USING TTL 0 SET worker = 'DONE' "
                     "WHERE run = ? AND id = ? IF worker = ?"),
        "release": p("DELETE FROM claims WHERE run = ? AND id = ? IF worker = ?"),
        "get_att": p("SELECT attempts FROM tasks WHERE run = ? AND id = ?"),
        "set_att": p("UPDATE tasks SET attempts = ? WHERE run = ? AND id = ?"),
        "state":   p("UPDATE tasks SET state = 'DONE' WHERE run = ? AND id = ?"),
        "count":   p("UPDATE counters SET value = value + 1 "
                     "WHERE run = ? AND worker = ? AND name = ?"),
    }


def count(session, st, run, me, name):
    """Fire-and-forget counter increment: never retried (a retried counter
    update after a timeout could count twice). Errors are only logged."""
    fut = session.execute_async(st["count"], (run, me, name))
    fut.add_errback(lambda e: print(f"# counter {name} failed: {e!r}", flush=True))



# Lease renewal during execution

def renewer(session, st, run, tid, me, finished, lost):
    """Extend the claim every RENEW_EVERY s until `finished` is set.
    If a renewal is not applied, someone else owns the task now: set `lost`."""
    while not finished.wait(RENEW_EVERY):
        rs = session.execute(st["renew"], (me, run, tid, me))
        if not rs.was_applied:
            lost.set()
            return



# One worker thread

def worker_thread(idx, qname, prefetch, session, st):
    import pika
    me = f"{common.hostname()}/{os.getpid()}/t{idx}"
    conn = common.rabbit_connect(name=common.hostname())
    ch = conn.channel()                          
    ch.basic_qos(prefetch_count=prefetch)
    pub = conn.channel()
    pub.confirm_delivery()
    props = pika.BasicProperties(delivery_mode=2, content_type="application/json")

    def log(tid, start, end, status, redelivered, outcome):
        s = f"{start:.6f}" if start else ""
        e = f"{end:.6f}" if end else ""
        print(f"{tid},{s},{e},{'' if status is None else status},"
              f"{int(redelivered)},{outcome}", flush=True)

    def on_task(ch, method, properties, body):
        task = common.decode(body)
        run, tid, cmd = task["run"], task["id"], task["cmd"]
        redelivered = method.redelivered

        if stop.is_set():
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return

        count(session, st, run, me, "deliveries")

        # 1. Claim (LWT). On failure the response tells who holds it.
        rs = session.execute(st["claim"], (run, tid, me))
        if not rs.was_applied:
            holder = rs.one().worker
            if holder == "DONE":
                count(session, st, run, me, "duplicates")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                log(tid, None, None, None, redelivered, "dup")
            else:
                # Holder may be dead with its lease still running: never drop it.
                count(session, st, run, me, "requeues")
                time.sleep(REQUEUE_DELAY)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                log(tid, None, None, None, redelivered, "requeue")
            return

        # 2. Attempts: we are the only writer while we hold the lease.
        row = session.execute(st["get_att"], (run, tid)).one()
        attempts = ((row.attempts or 0) if row else 0) + 1
        session.execute(st["set_att"], (attempts, run, tid))
        count(session, st, run, me, "executions")
        if attempts > 1:
            count(session, st, run, me, "reexecutions")

        # 3. Execute, renewing the lease in the background.
        finished, lost = threading.Event(), threading.Event()
        ren = threading.Thread(target=renewer,
                               args=(session, st, run, tid, me, finished, lost),
                               daemon=True)
        ren.start()
        status, start, end = common.run_task(cmd)
        finished.set()
        ren.join()

        # 4. Publish the result, confirmed
        pub.basic_publish(exchange="", routing_key=f"{run}.results",
                          body=common.encode_result(run, tid, status, me,
                                                    start, end, attempts),
                          properties=props, mandatory=True)

        # 5. DONE, only if we still own the claim.
        rs = session.execute(st["done"], (run, tid, me))
        outcome = "done"
        if rs.was_applied:
            session.execute(st["state"], (run, tid))
        else:
            outcome = "lost-lease"

        # 6. Ack, only after the result is published.
        ch.basic_ack(delivery_tag=method.delivery_tag)
        log(tid, start, end, status, redelivered, outcome)

    def on_task_safe(ch, method, properties, body):
        """Any error (e.g. Cassandra briefly unavailable) must not kill the
        thread: log it and give the message back. If we held the claim, it
        expires after CLAIM_TTL and the task is re-run (at-least-once)."""
        activity(+1)
        try:
            on_task(ch, method, properties, body)
        except Exception as e:
            print(f"# error on delivery {method.delivery_tag}: {e!r}", flush=True)
            try:
                time.sleep(REQUEUE_DELAY)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            except Exception:
                pass
        finally:
            activity(-1)

    tag =ch.basic_consume(queue=f"{qname}.tasks", on_message_callback=on_task_safe)

    while not stop.is_set():
        conn.process_data_events(time_limit=1)

    ch.basic_cancel(tag)
    conn.close()



# Main

def check_queue(qname):
    """Passive declare of QNAME.tasks: exit non-zero if it does not exist."""
    import pika
    conn = common.rabbit_connect()
    try:
        conn.channel().queue_declare(queue=f"{qname}.tasks", passive=True)
    except pika.exceptions.ChannelClosedByBroker:
        sys.exit(f"ERROR: queue '{qname}.tasks' does not exist")
    conn.close()


def main():
    p = argparse.ArgumentParser(description="CS553 HW6 worker")
    p.add_argument("-s", dest="qname", required=True, help="run name QNAME")
    p.add_argument("-t", dest="threads", type=int, default=8, help="worker threads")
    p.add_argument("-p", dest="prefetch", type=int, default=1, help="prefetch per thread")
    p.add_argument("-i", dest="idle", type=float, default=0,
                   help="leave after IDLE s without any task (0 = never)")
    args = p.parse_args()

    # Show the Cassandra driver's warnings (hidden by default) in the log.
    logging.basicConfig(level=logging.WARNING, stream=sys.stdout,
                        format="# %(asctime)s %(name)s %(levelname)s %(message)s")
    check_queue(args.qname)
    cluster, session = common.cassandra_connect()
    st = prepare(session)

    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    print(common.provenance_header(), flush=True)
    print(f"# worker qname={args.qname} threads={args.threads} prefetch={args.prefetch} "
          f"idle={args.idle}", flush=True)
    print("id,start,end,status,redelivered,outcome", flush=True)

    threads = [threading.Thread(target=worker_thread,
                                args=(i, args.qname, args.prefetch, session, st))
               for i in range(args.threads)]
    for t in threads:
        t.start()

    while any(t.is_alive() for t in threads):
        for t in threads:
            t.join(timeout=0.5)
        if args.idle and not stop.is_set():
            with activity_lock:
                idle_for, running = time.time() - last_activity, busy
            if running == 0 and idle_for >= args.idle:
                print(f"# idle for {idle_for:.0f} s: leaving", flush=True)
                stop.set()

    cluster.shutdown()
    print("# worker stopped", flush=True)


if __name__ == "__main__":
    main()
