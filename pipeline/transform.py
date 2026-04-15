import pandas as pd
import hashlib
import logging

logger = logging.getLogger(__name__)

VALID_EVENT_TYPES = {'view', 'cart', 'purchase'}
PRICE_MAX = 10_000      # flag anything above this
DATE_MIN  = pd.Timestamp('2019-09-01')
DATE_MAX  = pd.Timestamp('2019-12-31')


def _make_event_id(row) -> int:
    """Deterministic hash of (user_id, product_id, event_time) → stable int ID."""
    key = f"{row['user_id']}_{row['product_id']}_{row['event_time']}"
    return int(hashlib.md5(key.encode()).hexdigest()[:16], 16)


def transform(chunk: pd.DataFrame, quality_log: dict) -> dict:
    """
    Returns dict with keys: fact, dim_product, dim_category, dim_user
    Mutates quality_log in place with violation counts.
    """
    df = chunk.copy()
    initial = len(df)

    # ── 1. Timestamp validation ──────────────────────────────────────
    bad_dates = df[
        (df['event_time'] < DATE_MIN) | (df['event_time'] > DATE_MAX)
    ]
    if len(bad_dates) > 0:
        quality_log['bad_timestamps'] = quality_log.get('bad_timestamps', 0) + len(bad_dates)
        logger.warning(f"{len(bad_dates)} rows outside expected date range")
    df = df[(df['event_time'] >= DATE_MIN) & (df['event_time'] <= DATE_MAX)]

    # ── 2. Event type validation ─────────────────────────────────────
    invalid_types = df[~df['event_type'].isin(VALID_EVENT_TYPES)]
    if len(invalid_types) > 0:
        quality_log['invalid_event_types'] = quality_log.get('invalid_event_types', 0) + len(invalid_types)
    df = df[df['event_type'].isin(VALID_EVENT_TYPES)]

    # ── 3. Price validation ──────────────────────────────────────────
    bad_prices = df[df['price'] < 0]
    quality_log['negative_prices'] = quality_log.get('negative_prices', 0) + len(bad_prices)

    large_prices = df[df['price'] > PRICE_MAX]
    quality_log['large_prices'] = quality_log.get('large_prices', 0) + len(large_prices)

    # flag but do NOT drop — large/zero prices are kept, just logged
    df['price_flagged'] = (df['price'] < 0) | (df['price'] > PRICE_MAX)
    df = df[df['price'] >= 0]   # only drop negatives

    # ── 4. Deduplication ─────────────────────────────────────────────
    before_dedup = len(df)
    df = df.drop_duplicates(subset=['user_id', 'product_id', 'event_time'])
    dupes = before_dedup - len(df)
    quality_log['duplicates_dropped'] = quality_log.get('duplicates_dropped', 0) + dupes
    if dupes > 0:
        logger.info(f"Dropped {dupes} duplicate events")

    # ── 5. Derive columns ────────────────────────────────────────────
    df['event_month'] = df['event_time'].dt.to_period('M').astype(str)

    # parse category hierarchy from category_code (e.g. "electronics.smartphone")
    split = df['category_code'].str.split('.', n=1, expand=True)
    df['category_main'] = split[0]
    df['category_sub']  = split[1] if 1 in split.columns else None

    # generate stable event_id
    df['event_id'] = df.apply(_make_event_id, axis=1)

    quality_log['rows_transformed'] = quality_log.get('rows_transformed', 0) + len(df)
    quality_log['rows_dropped']     = quality_log.get('rows_dropped', 0) + (initial - len(df))

    # ── 6. Split into dimension and fact frames ───────────────────────
    dim_category = (
        df[['category_id', 'category_code', 'category_main', 'category_sub']]
        .drop_duplicates(subset=['category_id'])
        .dropna(subset=['category_id'])
    )

    dim_product = (
        df[['product_id', 'category_id', 'brand']]
        .drop_duplicates(subset=['product_id'])
    )

    dim_user = (
        df[['user_id']]
        .drop_duplicates()
    )

    fact = df[[
        'event_id', 'event_time', 'event_type',
        'product_id', 'user_id', 'category_id',
        'user_session', 'price', 'event_month'
    ]]

    return {
        'fact':         fact,
        'dim_product':  dim_product,
        'dim_category': dim_category,
        'dim_user':     dim_user
    }
