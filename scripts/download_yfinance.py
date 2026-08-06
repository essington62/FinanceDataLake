"""
Incremental daily ingestion from Yahoo Finance.

Reads existing parquet partitions under data/business_day (assets) and
data/macro_daily (indices), downloads only the missing days, merges,
and writes back atomically. Adapted from crypto-market-state's
yfinance_incremental Kedro node — same incremental contract, no Kedro.

Run:
    python scripts/download_yfinance.py
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.common import atomic_write_parquet, load_tickers, partition_path, read_partition

MAX_RETRIES = 3
RETRY_SLEEP = 5.0
INTER_TICKER_SLEEP = 1.5

_OHLCV_RENAME = {
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Adj Close": "adj_close",
    "Volume": "volume",
}


def _normalize_ohlcv_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    for up, lo in _OHLCV_RENAME.items():
        if up in df.columns:
            if lo in df.columns:
                mask = df[lo].isna() & df[up].notna()
                if mask.any():
                    df = df.copy()
                    df.loc[mask, lo] = df.loc[mask, up]
                df = df.drop(columns=[up])
            else:
                df = df.rename(columns={up: lo})
    return df


def _download_yf_daily(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    df = pd.DataFrame()
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            df = yf.download(
                tickers=ticker,
                start=start_date,
                end=end_date,
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False,
            )
            last_error = None
            break
        except Exception as e:
            last_error = e
            print(f"[YFINANCE WARN] {ticker}: attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP * attempt)

    if last_error is not None:
        print(f"[YFINANCE ERROR] Failed to download {ticker} after {MAX_RETRIES} attempts: {last_error}")
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.reset_index()

    date_col = next((c for c in df.columns if c.lower() in ("date", "datetime")), None)
    if date_col is None:
        print(f"[YFINANCE WARN] No date column found for {ticker}")
        return pd.DataFrame()
    df = df.rename(columns={date_col: "date"})
    df["date"] = pd.to_datetime(df["date"], utc=True)

    required_cols = ["date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
    df = df[[c for c in required_cols if c in df.columns]]

    for col in ["Open", "High", "Low", "Close", "Adj Close", "Volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    if ticker in ("^VIX", "^MOVE", "^OVX"):
        for col in ["Open", "High", "Low", "Close"]:
            if col in df.columns:
                mask = (df[col] <= 0) | df[col].isna()
                if mask.any():
                    df[col] = df[col].mask(mask).ffill()
                    if len(df) > 0 and (df[col].iloc[0] <= 0 or pd.isna(df[col].iloc[0])):
                        df[col] = df[col].mask(df[col] <= 0).bfill()

    df = _normalize_ohlcv_columns(df)

    ohlc_present = [c for c in ("open", "high", "low", "close") if c in df.columns]
    if ohlc_present:
        df = df.dropna(subset=ohlc_present, how="all")

    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    return df


def _update_group(items: list[dict], subdir: str, global_start_date: str, group_label: str) -> None:
    start_date_utc = pd.to_datetime(global_start_date, utc=True).date()
    today_utc = datetime.now(timezone.utc).date()
    tomorrow_str = (today_utc + timedelta(days=1)).isoformat()

    print(f"[YFINANCE INFO] Processing {len(items)} {group_label} items")
    report_rows = []

    for item in items:
        ticker, name, category = item.get("ticker"), item.get("name"), item.get("category", "unknown")
        if not ticker or not name:
            print(f"[YFINANCE WARN] Skipping item with missing ticker or name: {item}")
            continue

        print(f"[YFINANCE INFO] Processing {name} ({ticker}) - {category}")

        existing_df = read_partition(subdir, name)
        if not existing_df.empty:
            existing_df = _normalize_ohlcv_columns(existing_df.copy())
            existing_df["date"] = pd.to_datetime(existing_df["date"], utc=True)
            print(f"[YFINANCE INFO]   Loaded existing data: {len(existing_df)} rows")
        else:
            print("[YFINANCE INFO]   No existing data, starting fresh")

        if not existing_df.empty:
            next_start_date = (existing_df["date"].max().date() + timedelta(days=1)).isoformat()
        else:
            next_start_date = start_date_utc.isoformat()

        print(f"[YFINANCE INFO]   Next start date: {next_start_date}")

        if next_start_date >= tomorrow_str:
            print("[YFINANCE INFO]   Already up-to-date")
            report_rows.append({"asset": name, "inserted_from": None, "inserted_to": None, "rows": 0})
            continue

        df_new = _download_yf_daily(ticker, next_start_date, tomorrow_str)
        time.sleep(INTER_TICKER_SLEEP)
        if df_new.empty:
            print(f"[YFINANCE WARN]   No data downloaded for {ticker}")
            report_rows.append({"asset": name, "inserted_from": None, "inserted_to": None, "rows": 0})
            continue

        print(f"[YFINANCE INFO]   Downloaded {len(df_new)} new rows")

        merged = pd.concat([existing_df, df_new], axis=0, ignore_index=True) if not existing_df.empty else df_new
        merged["date"] = pd.to_datetime(merged["date"], utc=True)
        for col in ["open", "high", "low", "close", "adj_close", "volume"]:
            if col in merged.columns:
                merged[col] = pd.to_numeric(merged[col], errors="coerce").astype("float64")
        ohlc_present = [c for c in ("open", "high", "low", "close") if c in merged.columns]
        if ohlc_present:
            merged = merged.dropna(subset=ohlc_present, how="all")
        merged = merged.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

        atomic_write_parquet(merged, partition_path(subdir, name))

        report_rows.append({
            "asset": name,
            "inserted_from": df_new["date"].min().date().isoformat(),
            "inserted_to": df_new["date"].max().date().isoformat(),
            "rows": int(len(df_new)),
        })
        print(f"[YFINANCE INFO]   Final: {len(merged)} rows, range {merged['date'].min().date()} → {merged['date'].max().date()}")

    if report_rows:
        print("\n==============================================")
        print(f"YFINANCE INCREMENTAL UPDATE REPORT — {group_label.upper()}")
        print("==============================================")
        print(pd.DataFrame(report_rows).to_string(index=False))
        print("==============================================\n")


def main() -> None:
    config = load_tickers()
    global_start_date = config["global"]["start_date"]
    yf_config = config["yfinance"]

    _update_group(yf_config.get("indices", []), "macro_daily", global_start_date, "indices")
    _update_group(yf_config.get("assets", []), "business_day", global_start_date, "assets")


if __name__ == "__main__":
    main()
