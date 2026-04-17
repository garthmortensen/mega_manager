"""Persist collected project snapshots to a SQLite database."""

from datetime import datetime
from pathlib import Path

import pandas as pd
import structlog
from sqlalchemy import create_engine

log = structlog.get_logger(__name__)

DB_NAME = "mega_manager.db"


def _engine(output_dir: Path):
    return create_engine(f"sqlite:///{output_dir / DB_NAME}")


def save(
    df: pd.DataFrame,
    table_name: str = "data",
    output_dir: str | Path = "output",
) -> Path:
    """Append *df* to *table_name* in the SQLite database and return the db path."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    collected_at = datetime.now().isoformat(timespec="seconds")
    tagged = df.copy()
    tagged.insert(0, "collected_at", collected_at)

    engine = _engine(out)
    tagged.to_sql(table_name, engine, if_exists="append", index=False)

    db_path = out / DB_NAME
    log.info("storage.saved", table=table_name, rows=len(df), db=str(db_path))
    return db_path


def save_tables(
    tables: dict[str, pd.DataFrame],
    output_dir: str | Path = "output",
) -> list[Path]:
    """Append each named DataFrame to its own table in the SQLite database.

    All rows written in one call share the same ``collected_at`` timestamp,
    making it easy to identify data from a single run.

    Parameters
    ----------
    tables:
        Mapping of table name → DataFrame, e.g.
        ``{"issues": df_issues, "members": df_members}``.
    output_dir:
        Directory containing ``mega_manager.db``.  Created if absent.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    collected_at = datetime.now().isoformat(timespec="seconds")
    engine = _engine(out)
    db_path = out / DB_NAME

    for name, df in tables.items():
        tagged = df.copy()
        tagged.insert(0, "collected_at", collected_at)
        tagged.to_sql(name, engine, if_exists="append", index=False)
        log.info("storage.saved", table=name, rows=len(df), db=str(db_path))

    return [db_path]
