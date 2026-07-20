"""
Incremental daily ingestion from FRED (St. Louis Fed).

Reads existing parquet partitions under data/macro_daily, fetches only the
missing days per series, merges, and writes back atomically. Adapted from
crypto-market-state's fred_incremental Kedro node — same incremental
contract and FRED client, no Kedro.

Requires FRED_API_KEY in the environment (see .env.example).

Run:
    python scripts/download_fred.py
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.common import atomic_write_parquet, load_tickers, partition_path, read_partition

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
DEFAULT_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_SLEEP = 1.5


def _resolve_fred_api_key() -> str:
    key = os.getenv("FRED_API_KEY")
    if not key:
        raise RuntimeError("FRED_API_KEY not found in environment. Set it in .env or export it.")
    return key


def _fetch_observations(params: dict) -> dict:
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(FRED_BASE_URL, params=params, timeout=DEFAULT_TIMEOUT)
            if response.status_code == 200:
                return response.json()
            last_error = RuntimeError(f"FRED API returned status {response.status_code}")
        except Exception as e:
            last_error = e
        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_SLEEP)
    raise RuntimeError(f"FRED request failed after {MAX_RETRIES} attempts: {last_error}")


def fetch_fred_series(series_id: str, start_date: str, end_date: str, api_key: str) -> pd.DataFrame:
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": end_date,
    }
    payload = _fetch_observations(params)
    observations = payload.get("observations", [])
    df = pd.DataFrame(observations)
    if df.empty:
        return pd.DataFrame(columns=["date", "value"])

    df = df[["date", "value"]].copy()
    df["date"] = pd.to_datetime(df["date"], utc=True)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    return df


def main() -> None:
    api_key = _resolve_fred_api_key()
    config = load_tickers()
    global_start_date = config["global"]["start_date"]
    series_list = config["fred"]["series"]

    start_date_utc = pd.to_datetime(global_start_date, utc=True).normalize()
    today_utc = datetime.now(timezone.utc).date()
    today_str = today_utc.isoformat()

    print(f"[FRED INFO] Processing {len(series_list)} series")
    report_rows = []

    for idx, cfg in enumerate(series_list, 1):
        series_id, name, category = cfg["id"], cfg["name"], cfg.get("category", "unknown")
        print(f"[FRED INFO] [{idx}/{len(series_list)}] {series_id} ({name}) - {category}")

        existing_df = read_partition("macro_daily", series_id)
        if not existing_df.empty:
            existing_df = existing_df.copy()
            existing_df["date"] = pd.to_datetime(existing_df["date"], utc=True)
            print(f"[FRED INFO]   Loaded existing data: {len(existing_df)} rows")
        else:
            print("[FRED INFO]   No existing data, starting fresh")

        if not existing_df.empty:
            next_start_date = (existing_df["date"].max().date() + timedelta(days=1)).isoformat()
        else:
            next_start_date = start_date_utc.date().isoformat()

        print(f"[FRED INFO]   Next start date: {next_start_date}")

        if next_start_date > today_str:
            print("[FRED INFO]   Already up-to-date")
            report_rows.append({"series": series_id, "inserted_from": None, "inserted_to": None, "rows": 0})
            continue

        try:
            new_df = fetch_fred_series(series_id, next_start_date, today_str, api_key)
        except Exception as e:
            print(f"[FRED ERROR]   Failed to fetch {series_id}: {e}")
            report_rows.append({"series": series_id, "inserted_from": None, "inserted_to": None, "rows": 0})
            continue

        if new_df.empty:
            print("[FRED INFO]   No new data available")
            report_rows.append({"series": series_id, "inserted_from": None, "inserted_to": None, "rows": 0})
            continue

        print(f"[FRED INFO]   Fetched {len(new_df)} new rows")

        merged = pd.concat([existing_df, new_df], axis=0, ignore_index=True) if not existing_df.empty else new_df
        merged["date"] = pd.to_datetime(merged["date"], utc=True)
        merged["value"] = pd.to_numeric(merged["value"], errors="coerce").astype("float64")
        merged = merged.dropna(subset=["value"])
        merged = merged.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

        if merged.empty:
            print("[FRED WARN]   No valid data after cleaning")
            report_rows.append({"series": series_id, "inserted_from": None, "inserted_to": None, "rows": 0})
            continue

        atomic_write_parquet(merged, partition_path("macro_daily", series_id))

        report_rows.append({
            "series": series_id,
            "inserted_from": new_df["date"].min().date().isoformat(),
            "inserted_to": new_df["date"].max().date().isoformat(),
            "rows": int(len(new_df)),
        })
        print(f"[FRED INFO]   Final: {len(merged)} rows, {merged['date'].min().date()} → {merged['date'].max().date()}")

    print("\n==============================================")
    print("FRED INCREMENTAL UPDATE REPORT")
    print("==============================================")
    print(pd.DataFrame(report_rows).to_string(index=False))
    print("==============================================\n")


if __name__ == "__main__":
    main()
