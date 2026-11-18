#!/usr/bin/env python3
import base64
import json
import urllib.parse
import urllib.request

import common


def api_get(path):
    u = urllib.parse.urlparse(common.env("CK_AMQP_URL"))
    user, pw = urllib.parse.unquote(u.username), urllib.parse.unquote(u.password)
    req = urllib.request.Request(f"http://{u.hostname}:15672/api{path}")
    req.add_header("Authorization",
                   "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode())
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.load(r)


def durable_mode(qname):
    """True for a quorum QNAME.tasks (durable mode), False for a classic one."""
    vhost = urllib.parse.quote(urllib.parse.unquote(
        urllib.parse.urlparse(common.env("CK_AMQP_URL")).path.lstrip("/")) or "/", safe="")
    info = api_get(f"/queues/{vhost}/{urllib.parse.quote(qname + '.tasks', safe='')}")
    return info.get("type") == "quorum"
