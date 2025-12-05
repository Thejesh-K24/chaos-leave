import os
import time
import json
import random
from urllib.parse import parse_qs

# --- helpers to parse chaos controls ---

def parse_chaos(event):
    """
    Accept chaos config from:
      - query: ?chaos=lat:2500,err:0.03,cpu:400
      - header: X-Chaos: lat:2500,err:0.03,cpu:400
    """
    chaos_str = ""

    # API Gateway HTTP API / Lambda URL raw query
    raw_query = event.get("rawQueryString") or ""
    if raw_query:
        params = parse_qs(raw_query)
        if "chaos" in params:
            chaos_str = params["chaos"][0]

    # Fallback: header
    if not chaos_str:
        headers = event.get("headers") or {}
        chaos_str = headers.get("x-chaos", "")

    lat_ms = 0
    err_pct = 0.0
    cpu_ms = 0

    if chaos_str:
        for token in chaos_str.split(","):
            k, _, v = token.partition(":")
            k = k.strip().lower()
            v = v.strip()
            if not v:
                continue
            if k == "lat":
                lat_ms = int(float(v))
            elif k == "err":
                err_pct = float(v)
            elif k == "cpu":
                cpu_ms = int(float(v))

    return lat_ms, err_pct, cpu_ms


def cpu_spin(ms: int):
    """Busy-loop CPU for ms milliseconds."""
    if ms <= 0:
        return
    end = time.perf_counter() + (ms / 1000.0)
    x = 0
    while time.perf_counter() < end:
        x += 1  # burn cycles


# --- pretend leave management logic (baseline paper “leave app”) ---

LEAVES = []  # in-memory for now; you can replace with DynamoDB if LabRole allows


def apply_leave(payload):
    """Simplified apply-leave, acting like baseline leave management app."""
    # Just echo back for now; you can extend with DynamoDB later
    payload["id"] = len(LEAVES) + 1
    LEAVES.append(payload)
    return payload


def handler(event, context):
    # Parse chaos controls
    lat_ms, err_pct, cpu_ms = parse_chaos(event)

    # Inject CPU + latency + random error
    cpu_spin(cpu_ms)
    if lat_ms > 0:
        time.sleep(lat_ms / 1000.0)
    if err_pct > 0 and random.random() < err_pct:
        return {
            "statusCode": 500,
            "body": "Injected failure from chaos controller"
        }

    # Simple router for leave management
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
    path = event.get("requestContext", {}).get("http", {}).get("path", "/")

    if method == "POST" and path.endswith("/apply-leave"):
        try:
            body = json.loads(event.get("body") or "{}")
        except json.JSONDecodeError:
            body = {}
        leave = apply_leave(body)
        resp = {"status": "ok", "leave": leave}
    else:
        resp = {
            "status": "ok",
            "message": "Leave API baseline running",
            "total_leaves": len(LEAVES),
        }

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "data": resp,
            "chaos": {"lat_ms": lat_ms, "err_pct": err_pct, "cpu_ms": cpu_ms},
            "region": os.getenv("AWS_REGION"),
        })
    }
