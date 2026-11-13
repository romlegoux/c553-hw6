#!/usr/bin/env python3
import csv
import json
import os
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone


KEYSPACE = "ck"

def env(name, default=None):
    """Read a CK_* variable; exit with a clear message if it is required and missing."""
    value = os.environ.get(name, default)
    if value is None:
        sys.exit(f"ERROR: environment variable {name} is not set (source ck.env?)")
    return value


def hostname():
    """Short hostname of this VM (tiny, small3, ...)."""
    return socket.gethostname().split(".")[0]

def rabbit_connect():
    """Open a blocking RabbitMQ connection from CK_AMQP_URL.
    One connection/channel per thread: pika objects are NOT thread-safe."""
    import pika
    params = pika.URLParameters(env("CK_AMQP_URL"))
    return pika.BlockingConnection(params)


def cassandra_connect():
    """Return (cluster, session) connected to the ck keyspace."""
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster
    hosts = [h.strip() for h in env("CK_CASSANDRA_HOSTS").split(",") if h.strip()]
    auth = PlainTextAuthProvider(username=env("CK_CASSANDRA_USER"),
                                 password=env("CK_CASSANDRA_PASSWORD"))
    cluster = Cluster(contact_points=hosts, port=9042, auth_provider=auth)
    return cluster, cluster.connect(KEYSPACE)

def read_workload(path):
    """One command per line ("sleep MS"). Blank lines and '#' comments are skipped.
    Commands are NOT validated here: an invalid one must fail at execution (status 1)."""
    cmds = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                cmds.append(line)
    return cmds


def client_id():
    """Identifies one client process: two clients on the same run never collide."""
    return f"{hostname()}-{os.getpid()}"


def task_id(cid, seq):
    """Unique task ID = client ID + sequence number (zero-padded so it sorts)."""
    return f"{cid}-{seq:07d}"

def run_task(cmd):
    """Execute one command. Returns (status, start, end).
    status 0 = success, 1 = error."""
    start = time.time()
    try:
        parts = cmd.split()
        if len(parts) != 2 or parts[0] != "sleep":
            raise ValueError(f"unknown command: {cmd!r}")
        ms = int(parts[1])
        if ms < 0:
            raise ValueError(f"negative duration: {ms}")
        time.sleep(ms / 1000.0)
        status = 0
    except Exception:
        status = 1
    return status, start, time.time()

def encode_task(run, tid, cmd):
    """Task message: only what a worker needs to execute it."""
    return json.dumps({"run": run, "id": tid, "cmd": cmd}).encode()


def encode_result(run, tid, status, worker, start, end, attempts):
    """Result message: everything the client's CSV needs from the worker side,
    so the client never has to read Cassandra."""
    return json.dumps({"run": run, "id": tid, "status": status, "worker": worker,
                       "start": start, "end": end, "attempts": attempts}).encode()


def decode(body):
    """bytes -> dict (works for both message types)."""
    return json.loads(body)

def git_commit():
    """Short commit hash of the code, from CK_COMMIT or from git; 'unknown' otherwise."""
    if os.environ.get("CK_COMMIT"):
        return os.environ["CK_COMMIT"]
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        return subprocess.check_output(["git", "-C", here, "rev-parse", "--short", "HEAD"],
                                       stderr=subprocess.DEVNULL, text=True).strip()
    except Exception:
        return "unknown"


def provenance_header():
    """First line of every raw result file (proof C4)."""
    utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return (f"# vm={hostname()} node={os.environ.get('CK_NODE', 'unknown')} "
            f"utc={utc} commit={git_commit()}")


def new_record(tid, cmd, submitted):
    """Per-task record kept by the client; result fields are filled on arrival."""
    return {"id": tid, "cmd": cmd, "submitted": submitted,
            "start": None, "end": None, "arrival": None,
            "worker": None, "attempts": None, "status": None}


def summary(records, first_publish, last_result, duplicates):
    """Build the summary text. records: dict id -> record (one per task)."""
    n = len(records)
    ok = sum(1 for r in records.values() if r["status"] == 0)
    failed = n - ok
    elapsed = max(last_result - first_publish, 1e-9)
    lines = [
        provenance_header(),
        f"tasks              : {n}",
        f"successes          : {ok}",
        f"failures           : {failed}",
        f"duplicates ignored : {duplicates}",
        f"time (s)           : {elapsed:.3f}",
        f"throughput (t/s)   : {n / elapsed:.1f}",
    ]
    return "\n".join(lines)


def write_summary(text, path=None):
    """Print the summary, and also save it to a file if a path is given."""
    print(text)
    if path:
        with open(path, "w") as f:
            f.write(text + "\n")


CSV_COLUMNS = ["id", "cmd", "submitted", "start", "end", "arrival",
               "worker", "attempts", "status"]

def write_csv(records, path):
    """One line per task, sorted by ID, after the provenance header."""
    with open(path, "w", newline="") as f:
        f.write(provenance_header() + "\n")
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writeheader()
        for tid in sorted(records):
            w.writerow(records[tid])


def exit_code(records):
    """0 if every task succeeded, 1 otherwise."""
    return 0 if all(r["status"] == 0 for r in records.values()) else 1

