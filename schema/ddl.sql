-- ─────────────────────────────────────────
-- dim_category
-- category_code is nullable: many rows have
-- category_id but no category_code in the raw data
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_category (
    category_id     BIGINT      PRIMARY KEY,
    category_code   VARCHAR,                    -- nullable: not always present
    category_main   VARCHAR,                    -- parsed from category_code (e.g. "electronics")
    category_sub    VARCHAR                     -- parsed from category_code (e.g. "smartphone")
);

CREATE INDEX IF NOT EXISTS idx_category_main ON dim_category(category_main);

-- ─────────────────────────────────────────
-- dim_product
-- brand is nullable: some products have no brand
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_product (
    product_id      BIGINT      PRIMARY KEY,
    category_id     BIGINT      REFERENCES dim_category(category_id),
    brand           VARCHAR                     -- nullable: missing in raw data
);

CREATE INDEX IF NOT EXISTS idx_product_brand      ON dim_product(brand);
CREATE INDEX IF NOT EXISTS idx_product_category   ON dim_product(category_id);

-- ─────────────────────────────────────────
-- dim_user
-- Minimal: user_id is the only reliable column
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_user (
    user_id         BIGINT      PRIMARY KEY
);

-- ─────────────────────────────────────────
-- fact_events  (central fact table)
-- price lives here, not in dim_product,
-- because the same product appears at
-- different prices across events
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_events (
    event_id        BIGINT      PRIMARY KEY,    -- synthetic: hash of (user_id, product_id, event_time)
    event_time      TIMESTAMP   NOT NULL,
    event_type      VARCHAR(10) NOT NULL,       -- 'view' | 'cart' | 'purchase'
    product_id      BIGINT      NOT NULL REFERENCES dim_product(product_id),
    user_id         BIGINT      NOT NULL REFERENCES dim_user(user_id),
    category_id     BIGINT      REFERENCES dim_category(category_id),  -- denorm for query speed
    user_session    VARCHAR     NOT NULL,
    price           NUMERIC(10,2),              -- nullable: views sometimes have price=0 or NULL
    event_month     VARCHAR(7)  NOT NULL        -- '2019-10' or '2019-11', for partition filtering
);

-- indexes that directly support Section C queries
CREATE INDEX IF NOT EXISTS idx_events_event_type  ON fact_events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_user_id     ON fact_events(user_id);
CREATE INDEX IF NOT EXISTS idx_events_product_id  ON fact_events(product_id);
CREATE INDEX IF NOT EXISTS idx_events_event_time  ON fact_events(event_time);
CREATE INDEX IF NOT EXISTS idx_events_month       ON fact_events(event_month);
CREATE INDEX IF NOT EXISTS idx_events_category    ON fact_events(category_id);

-- composite index for funnel query (Q1 in Section C)
CREATE INDEX IF NOT EXISTS idx_events_cat_type    ON fact_events(category_id, event_type);

-- composite index for session aggregation (Q2 in Section C)
CREATE INDEX IF NOT EXISTS idx_events_session     ON fact_events(user_session, user_id);
