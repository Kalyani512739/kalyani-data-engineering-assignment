import duckdb
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def _upsert_dim(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, table: str, pk: str):
    """Insert dimension rows, silently skip duplicates (idempotent)."""
    if df.empty:
        return
    con.register('_tmp', df)
    con.execute(f"""
        INSERT OR IGNORE INTO {table}
        SELECT * FROM _tmp
    """)
    con.unregister('_tmp')


def _insert_facts(con: duckdb.DuckDBPyConnection, df: pd.DataFrame):
    """Batch insert fact rows, skip duplicates via event_id PK."""
    if df.empty:
        return
    con.register('_tmp_facts', df)
    con.execute("""
        INSERT OR IGNORE INTO fact_events
        SELECT * FROM _tmp_facts
    """)
    con.unregister('_tmp_facts')


def load(con: duckdb.DuckDBPyConnection, transformed: dict) -> int:
    """
    Loads one chunk into the DB. Returns rows inserted into fact_events.
    """
    _upsert_dim(con, transformed['dim_category'], 'dim_category', 'category_id')
    _upsert_dim(con, transformed['dim_product'],  'dim_product',  'product_id')
    _upsert_dim(con, transformed['dim_user'],     'dim_user',     'user_id')

    before = con.execute("SELECT COUNT(*) FROM fact_events").fetchone()[0]
    _insert_facts(con, transformed['fact'])
    after  = con.execute("SELECT COUNT(*) FROM fact_events").fetchone()[0]

    inserted = after - before
    logger.info(f"Inserted {inserted} fact rows")
    return inserted
