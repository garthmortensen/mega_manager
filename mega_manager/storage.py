"""Persist collected project snapshots to disk as CSV."""

from datetime import datetime
from pathlib import Path

import pandas as pd
import structlog

log = structlog.get_logger(__name__)


def save(
    df: pd.DataFrame,
    output_dir: str | Path = "output",
) -> Path:
    """Write *df* to a timestamped CSV and return the path."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = out / f"gitlab_snapshot_{timestamp}.csv"
    df.to_csv(csv_path, index=False)
    log.info("storage.saved", path=str(csv_path), rows=len(df))
    return csv_path


def save_tables(
    tables: dict[str, pd.DataFrame],
    output_dir: str | Path = "output",
) -> list[Path]:
    """Write each named DataFrame to its own timestamped CSV.

    All files in one call share the same timestamp so they sort together.

    Parameters
    ----------
    tables:
        Mapping of table name → DataFrame, e.g.
        ``{"issues": df_issues, "members": df_members}``.
    output_dir:
        Directory to write files into.  Created if it doesn't exist.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    saved: list[Path] = []

    for name, df in tables.items():
        csv_path = out / f"{name}_{timestamp}.csv"
        df.to_csv(csv_path, index=False)
        log.info("storage.saved", path=str(csv_path), rows=len(df))
        saved.append(csv_path)

    return saved
