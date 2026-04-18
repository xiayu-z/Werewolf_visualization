"""Small IO helpers shared by pipeline and analysis scripts."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


def ensure_dir(path: str | Path) -> Path:
    directory = Path(path)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def table_path(root: str | Path, table_name: str, write_format: str = "parquet") -> Path:
    root = ensure_dir(root)
    if write_format == "parquet":
        return root / f"{table_name}.parquet"
    if write_format == "csv":
        return root / f"{table_name}.csv.gz"
    raise ValueError(f"Unsupported write format: {write_format}")


def write_frame(df: pd.DataFrame, root: str | Path, table_name: str, write_format: str = "parquet") -> Path:
    path = table_path(root=root, table_name=table_name, write_format=write_format)
    if write_format == "parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False, compression="gzip")
    return path


def read_table(root: str | Path, table_name: str) -> pd.DataFrame:
    root = Path(root)
    parquet_path = root / f"{table_name}.parquet"
    csv_path = root / f"{table_name}.csv.gz"
    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    if csv_path.exists():
        return pd.read_csv(csv_path)
    raise FileNotFoundError(f"Could not find table '{table_name}' in {root}")


def list_existing_tables(root: str | Path, table_names: Iterable[str]) -> list[str]:
    existing: list[str] = []
    for table_name in table_names:
        try:
            read_table(root, table_name)
        except FileNotFoundError:
            continue
        existing.append(table_name)
    return existing
