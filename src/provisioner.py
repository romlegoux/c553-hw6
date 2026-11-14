#!/usr/bin/env python3

import argparse
import base64
import json
import math
import signal
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import common

PREFIX = "ckw"
IMAGE = "ck-worker"
stop = False


def on_signal(*_):
    global stop
    stop = True


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def event(kind, name, extra=""):
    print(f"# event {time.time():.3f} {kind} {name} {extra}".rstrip(), flush=True)



# RabbitMQ: passive declare + management API

def api_get(path):
    """GET on the RabbitMQ management API (port 15672 on the same host as AMQP)."""
    u = urllib.parse.urlparse(common.env("CK_AMQP_URL"))
    user, pw = urllib.parse.unquote(u.username), urllib.parse.unquote(u.password)
    req = urllib.request.Request(f"http://{u.hostname}:15672/api{path}")
    req.add_header("Authorization",
                   "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode())
    with urllib.request.urlopen(req, timeout=2) as r:
        return json.load(r)


def connected_vms():
    """Names of our VMs that have at least one open RabbitMQ connection."""
    names = set()
    for c in api_get("/connections?columns=client_properties"):
        name = (c.get("client_properties") or {}).get("connection_name") or ""
        if name.startswith(PREFIX):
            names.add(name)
    return names



# LXD

def lxc_states():
    """{name: 'RUNNING' | 'STOPPED' | ...} for our VMs."""
    out = subprocess.run(["lxc", "list", f"^{PREFIX}", "--format", "csv", "-c", "ns"],
                         capture_output=True, text=True, timeout=20).stdout
    return dict(line.split(",")[:2] for line in out.splitlines() if "," in line)


def launch(name, args, idle):
    cmd = ["lxc", "launch", IMAGE, name, "--vm",
           "-c", "limits.cpu=8", "-c", "limits.memory=16GiB", "-d", "root,size=32GiB",
           "-c", f"user.ck.qname={args.qname}",
           "-c", f"user.ck.threads={args.threads}",
           "-c", f"user.ck.prefetch={args.prefetch}",
           "-c", f"user.ck.idle={idle}"]
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def delete(name):
    return subprocess.Popen(["lxc", "delete", "--force", name],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)



# Main 

def main():
    p = argparse.ArgumentParser(description="CS553 HW6 provisioner (run on the node)")
    p.add_argument("-s", dest="qname", required=True)
    p.add_argument("--min", dest="vmin", type=int, required=True)
    p.add_argument("--max", dest="vmax", type=int, required=True)
    p.add_argument("--idle", type=int, default=30, help="idle time of workers above MIN (s)")
    p.add_argument("-t", dest="threads", type=int, default=8)
    p.add_argument("-p", dest="prefetch", type=int, default=1)
    args = p.parse_args()
    if not 0 <= args.vmin <= args.vmax:
        p.error("need 0 <= MIN <= MAX")
    static = args.vmin == args.vmax                  # idle ignored

    if lxc_states():
        sys.exit(f"ERROR: VMs named {PREFIX}* already exist; delete them first "
                 f"(lxc delete -f $(lxc list ^{PREFIX} -c n --format csv))")

    import pika
    conn = common.rabbit_connect(name="provisioner")
    ch = conn.channel()
    try:
        ch.queue_declare(queue=f"{args.qname}.tasks", passive=True)
    except pika.exceptions.ChannelClosedByBroker:
        sys.exit(f"ERROR: queue '{args.qname}.tasks' does not exist")

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    print(common.provenance_header(), flush=True)
    print(f"# provisioner qname={args.qname} min={args.vmin} max={args.vmax} "
          f"idle={'static' if static else args.idle} threads={args.threads} "
          f"prefetch={args.prefetch}", flush=True)
    print("utc,ready,unacked,consumers,active,booting,leaving,total,started", flush=True)

    vms = {}
    counter = 0
    per_vm = args.threads * args.prefetch
    conns, unacked = set(), 0

    while not stop:
        tick = time.time()

        # 1. queue and connections
        q = ch.queue_declare(queue=f"{args.qname}.tasks", passive=True).method
        ready, consumers = q.message_count, q.consumer_count
        try:
            unacked = api_get(f"/queues/%2F/{args.qname}.tasks").get("messages_unacknowledged", 0)
            conns = connected_vms()
        except Exception as e:                       # keep the previous values
            print(f"# management API error: {e!r}", flush=True)

        # 2-3. VM states
        states = lxc_states()
        for name, vm in list(vms.items()):
            if vm["launch"] is not None and vm["launch"].poll() is not None:
                if vm["launch"].returncode != 0:
                    event("launch-failed", name)
                    vm["del"] = delete(name)
                vm["launch"] = None
            if vm["del"] is not None:
                if vm["del"].poll() is not None:
                    event("deleted", name, f"vm_seconds={time.time() - vm['t0']:.1f}")
                    del vms[name]
                continue
            if name in conns and vm["conn"] is None:
                vm["conn"] = time.time()
                event("connected", name, f"cold_start={vm['conn'] - vm['t0']:.1f}")
            if vm["conn"] is not None and name not in conns:
                vm["left"] = True
            if vm["launch"] is None and states.get(name) == "STOPPED":
                vm["del"] = delete(name)             # worker left, VM powered off

        alive = {n: v for n, v in vms.items() if v["del"] is None}
        active = sum(1 for v in alive.values() if v["conn"] is not None and not v["left"])
        booting = sum(1 for v in alive.values() if v["conn"] is None)
        leaving = sum(1 for v in alive.values() if v["left"])
        no_idle = sum(1 for v in alive.values() if v["idle"] == 0 and not v["left"])

        # 4. scale up
        capacity = active + booting
        want = max(0, args.vmin - capacity)
        if ready > capacity * per_vm:
            want = max(want, math.ceil((ready - capacity * per_vm) / per_vm))
        want = min(want, args.vmax - len(alive))
        started = []
        for _ in range(want):
            counter += 1
            name = f"{PREFIX}{counter}"
            idle = 0 if (static or no_idle < args.vmin) else args.idle
            if idle == 0:
                no_idle += 1
            vms[name] = {"launch": launch(name, args, idle), "t0": time.time(),
                         "conn": None, "left": False, "idle": idle, "del": None}
            event("launch", name, f"idle={idle}")
            started.append(name)

        # 5. log
        print(f"{utc_now()},{ready},{unacked},{consumers},{active},{booting},{leaving},"
              f"{len(alive) + len(started)},{';'.join(started)}", flush=True)

        conn.process_data_events(0)                  # heartbeats
        time.sleep(max(0.0, 1.0 - (time.time() - tick)))

    # SIGTERM: clean stop of every VM we launched, then delete them.
    print(f"# stopping {len(vms)} VMs", flush=True)
    procs = []
    for name, vm in vms.items():
        if vm["launch"] is not None:
            vm["launch"].wait()
        procs.append((name, vm, subprocess.Popen(
            ["bash", "-c", f"lxc stop --timeout 120 {name}; lxc delete --force {name}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)))
    for name, vm, proc in procs:
        proc.wait()
        event("deleted", name, f"vm_seconds={time.time() - vm['t0']:.1f}")
    conn.close()
    print("# provisioner stopped", flush=True)


if __name__ == "__main__":
    main()
