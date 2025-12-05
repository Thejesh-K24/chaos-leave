import re
from pathlib import Path

import numpy as np
import pandas as pd

RAW_DIR = Path("results/raw")
OUT_PATH = Path("results/processed/summary_all.csv")


def parse_filename(path: Path):
    """
    Expected patterns:
      static_5u_300ms.csv
      adaptive_lat300_7u_300ms.csv
      adaptive_lat300_1u_300ms.csv
    We extract:
      - mode: static / adaptive / other
      - vus: int
      - lat_ms: int
    """
    name = path.name.replace(".csv", "")

    if name.startswith("static_"):
        mode = "static"
    elif name.startswith("adaptive_"):
        mode = "adaptive"
    else:
        mode = "unknown"

    # Extract VUs: look for '<num>u'
    vus_match = re.search(r"(\d+)u", name)
    vus = int(vus_match.group(1)) if vus_match else None

    # Extract latency: look for '<num>ms'
    lat_match = re.search(r"(\d+)ms", name)
    lat_ms = int(lat_match.group(1)) if lat_match else None

    return mode, vus, lat_ms


def extract_latency_and_failed(df: pd.DataFrame):
    """
    Return (lat_series, failed_series) from a k6 CSV, in a robust way.

    Supports:
      1) Wide format with 'http_req_duration' and 'http_req_failed' columns
      2) Long metrics format with metric/metric_name and value/metric_value columns
    """
    # Case 1: wide format
    if "http_req_duration" in df.columns and "http_req_failed" in df.columns:
        lat = df["http_req_duration"]
        failed = df["http_req_failed"]
        return lat, failed

    # Case 2: long metrics format
    metric_col = None
    for cand in ["metric", "metric_name"]:
        if cand in df.columns:
            metric_col = cand
            break

    if metric_col is None:
        raise RuntimeError(
            f"CSV missing 'metric' or 'metric_name' column. "
            f"Columns: {df.columns.tolist()}"
        )

    # Detect value column
    value_col = None
    for cand in ["value", "metric_value"]:
        if cand in df.columns:
            value_col = cand
            break

    if value_col is None:
        # Fallback: first numeric column that is not metric_col
        for col in df.columns:
            if col == metric_col:
                continue
            if np.issubdtype(df[col].dtype, np.number):
                value_col = col
                break

    if value_col is None:
        raise RuntimeError(
            f"No numeric value column found. Columns: {df.columns.tolist()}"
        )

    df_lat = df[df[metric_col] == "http_req_duration"]
    if df_lat.empty:
        raise RuntimeError("No http_req_duration samples in CSV.")

    df_err = df[df[metric_col] == "http_req_failed"]

    lat = df_lat[value_col]

    if df_err.empty:
        failed = pd.Series([0.0])  # assume zero failures if metric missing
    else:
        failed = df_err[value_col]

    return lat, failed


def estimate_throughput(df: pd.DataFrame, lat_count: int):
    """
    Roughly estimate throughput (requests/sec) if possible.

    We try:
      - If there is 'time' or 'timestamp', use min..max as total duration.
      - Otherwise, return None.
    """
    time_col = None
    for cand in ["time", "timestamp"]:
        if cand in df.columns:
            time_col = cand
            break

    if time_col is None:
        return None

    try:
        t = pd.to_datetime(df[time_col])
        total_seconds = (t.max() - t.min()).total_seconds()
        if total_seconds <= 0:
            return None
        return lat_count / total_seconds
    except Exception:
        return None


def analyze_file(path: Path):
    df = pd.read_csv(path)

    mode, vus, lat_ms = parse_filename(path)
    lat_series, failed_series = extract_latency_and_failed(df)

    avg_ms = float(lat_series.mean())
    p95_ms = float(lat_series.quantile(0.95))
    err_rate = float(failed_series.mean())
    availability = 1.0 - err_rate

    throughput = estimate_throughput(df, len(lat_series))

    return {
        "file": path.name,
        "mode": mode,
        "vus": vus,
        "lat_ms": lat_ms,
        "avg_ms": avg_ms,
        "p95_ms": p95_ms,
        "error_rate": err_rate,
        "availability": availability,
        "throughput_req_s": throughput,
    }


def main():
    rows = []

    if not RAW_DIR.exists():
        raise RuntimeError(f"{RAW_DIR} does not exist. Make sure you have k6 CSVs in results/raw/")

    for csv_path in RAW_DIR.glob("*.csv"):
        print(f"Analyzing {csv_path}")
        try:
            row = analyze_file(csv_path)
            rows.append(row)
        except Exception as e:
            print(f"  Skipping {csv_path} due to error: {e}")

    if not rows:
        raise RuntimeError("No valid CSV files analyzed. Check results/raw content.")

    df = pd.DataFrame(rows)
    df.sort_values(by=["mode", "lat_ms", "vus"], inplace=True)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_PATH, index=False)

    print(f"\nSummary written to {OUT_PATH}")
    print(df)


if __name__ == "__main__":
    main()
