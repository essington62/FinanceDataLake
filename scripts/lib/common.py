from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = Path(os.environ.get("DATA_DIR", PROJECT_ROOT / "data"))
CONF_DIR = PROJECT_ROOT / "conf"


def load_tickers() -> dict:
    with open(CONF_DIR / "tickers.yml") as f:
        return yaml.safe_load(f)


def partition_path(subdir: str, name: str) -> Path:
    d = DATA_DIR / subdir
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{name}.parquet"


def read_partition(subdir: str, name: str) -> pd.DataFrame:
    path = partition_path(subdir, name)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def atomic_write_parquet(df: pd.DataFrame, path: Path) -> None:
    tmp_path = path.with_suffix(".parquet.tmp")
    df.to_parquet(tmp_path, compression="snappy", index=False)
    os.replace(tmp_path, path)
