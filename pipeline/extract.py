import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

COLUMNS = [
    'event_time', 'event_type', 'product_id',
    'category_id', 'category_code', 'brand',
    'price', 'user_id', 'user_session'
]

DTYPES = {
    'event_type':     'str',
    'product_id':     'Int64',
    'category_id':    'Int64',
    'category_code':  'str',
    'brand':          'str',
    'price':          'float64',
    'user_id':        'Int64',
    'user_session':   'str'
}

def extract(filepath: str, chunk_size: int = 100_000):
    """
    Yields (chunk_df, rows_read, rows_skipped) for each chunk.
    Malformed rows are skipped and counted, never crash the pipeline.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    logger.info(f"Starting extract from {path.name}")
    rows_read = 0
    rows_skipped = 0

    reader = pd.read_csv(
        filepath,
        names=COLUMNS,
        header=0,
        dtype=DTYPES,
        parse_dates=['event_time'],
        chunksize=chunk_size,
        on_bad_lines='warn',     # skip malformed rows, log warning
        engine='python'
    )

    for chunk in reader:
        before = len(chunk)

        # drop rows where critical fields are missing
        chunk = chunk.dropna(subset=['event_time', 'event_type', 'product_id', 'user_id', 'user_session'])

        after = len(chunk)
        skipped = before - after
        rows_read   += after
        rows_skipped += skipped

        if skipped > 0:
            logger.warning(f"Dropped {skipped} rows with NULL in critical columns")

        yield chunk, rows_read, rows_skipped

    logger.info(f"Extract complete — read: {rows_read}, skipped: {rows_skipped}")
