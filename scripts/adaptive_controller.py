import os
import subprocess
import time
from pathlib import Path

import numpy as np
import pandas as pd

URL = os.getenv("URL") or "https://your-api.execute-api.region.amazonaws.com/leave"

RESULTS_DIR = Path("results/raw")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# hard limit: account concurrency
MAX_VUS = 10

# SLA targets
P95_SLA_MS = 1000.0      # 1 second p95
ERR_SLA = 0.05           # 5% max error rate

TEST_DURATION = "90s"    # duration for each adaptive step


def run_k6(vus: int, lat_ms: int, run_label: str) -> str:
    csv_path = RESULTS_DIR / f"adaptive_{run_label}_{vus}u_{lat_ms}ms.csv"
    cmd = [
        "k6", "run",
        "--out", f"csv={csv_path}",
        "-e", f"URL={URL}",
        "-e", f"USERS={vus}",
        "-e", f"DUR={TEST_DURATION}",
        "-e", f"LAT={lat_ms}",
        "scripts/load.js",
    ]
    print("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True)
    return str(csv_path)


def compute_metrics(csv_path: str):
    df = pd.read_csv(csv_path)

    # --- Case 1: "wide" format ---
    if "http_req_duration" in df.columns and "http_req_failed" in df.columns:
        lat = df["http_req_duration"]
        failed = df["http_req_failed"]
        p95 = float(lat.quantile(0.95))
        err_rate = float(failed.mean())
        return p95, err_rate

    # --- Case 2: "long" k6 metrics format ---
    # Detect metric name column
    metric_col = None
    for cand in ["metric", "metric_name"]:
        if cand in df.columns:
            metric_col = cand
            break

    if metric_col is None:
        raise RuntimeError(
            f"{csv_path} does not contain 'metric' or 'metric_name' column. "
            f"Columns: {df.columns.tolist()}"
        )

    # Detect value column: try common names, then fallback to first numeric column
    value_col = None
    for cand in ["value", "metric_value"]:
        if cand in df.columns:
            value_col = cand
            break

    if value_col is None:
        # Fallback: choose the first numeric column that is not the metric name
        for col in df.columns:
            if col == metric_col:
                continue
            if np.issubdtype(df[col].dtype, np.number):
                value_col = col
                break

    if value_col is None:
        raise RuntimeError(
            f"{csv_path} has no obvious numeric value column. "
            f"Columns: {df.columns.tolist()}"
        )

    # Filter rows for http_req_duration and http_req_failed
    df_lat = df[df[metric_col] == "http_req_duration"]
    if df_lat.empty:
        raise RuntimeError(f"No http_req_duration rows found in {csv_path}")

    df_err = df[df[metric_col] == "http_req_failed"]

    lat_values = df_lat[value_col]

    if df_err.empty:
        # If there is no http_req_failed metric, assume 0 error rate
        err_rate = 0.0
    else:
        failed_values = df_err[value_col]
        err_rate = float(failed_values.mean())

    p95 = float(lat_values.quantile(0.95))
    return p95, err_rate


def adaptive_for_latency(lat_ms: int):
    print(f"\n=== Adaptive search for chaos lat={lat_ms}ms ===")
    vus = 1
    best_vus = 1
    best_metrics = None
    history = []

    while vus <= MAX_VUS:
        label = f"lat{lat_ms}"
        csv_path = run_k6(vus, lat_ms, label)
        p95, err_rate = compute_metrics(csv_path)
        print(f"VUS={vus}, p95={p95:.1f}ms, err_rate={err_rate:.3f}")

        ok = (p95 <= P95_SLA_MS) and (err_rate <= ERR_SLA)

        history.append({
            "lat_ms": lat_ms,
            "vus": vus,
            "p95_ms": p95,
            "err_rate": err_rate,
            "ok": ok
        })

        if ok:
            best_vus = vus
            best_metrics = (p95, err_rate)
            vus += 1      # step up
        else:
            # SLA violated: stop increasing
            break

        # small pause between rounds
        time.sleep(5)

    print(f"Best stable VUS for lat={lat_ms}ms: {best_vus} with metrics={best_metrics}")
    return history, best_vus, best_metrics


if __name__ == "__main__":
    all_histories = []
    for lat in [300, 1200, 5000, 10000]:
        hist, best_vus, metrics = adaptive_for_latency(lat)
        all_histories.extend(hist)

    processed_dir = Path("results/processed")
    processed_dir.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(all_histories)
    out_path = processed_dir / "adaptive_history.csv"
    df.to_csv(out_path, index=False)
    print(f"\nSaved adaptive history to {out_path}")

    # -------------------------------------------------------
    # CLEAN SUMMARY BLOCK
    # -------------------------------------------------------
    print("\n=============== CLEAN SUMMARY (For Thesis Report) ===============")
    if not df.empty:
        latencies = sorted(df["lat_ms"].unique())

        for lat in latencies:
            print(f"\nChaos Level: {lat} ms")
            print("-----------------------------------------------------------")
            print("VUS |   p95 (ms)   | Error Rate | SLA-OK")
            print("-----------------------------------------------------------")

            subset = df[df["lat_ms"] == lat]
            if subset.empty:
                print("No data recorded for this chaos level.")
                continue

            for _, row in subset.iterrows():
                print(
                    f"{int(row['vus']):3d} | {row['p95_ms']:12.2f} |"
                    f"   {row['err_rate']:.3f}    |  {'YES' if row['ok'] else 'NO'}"
                )

            safe = subset[subset["ok"] == True]
            if not safe.empty:
                best = safe["vus"].max()
                print(f"\n--> Best stable VUs for {lat} ms chaos = {best}")
            else:
                print(f"\n--> SLA violated at VU=1 for {lat} ms chaos")
    else:
        print("No history collected; nothing to summarise.")

    print("===============================================================")
