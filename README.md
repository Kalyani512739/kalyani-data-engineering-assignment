# kalyani-data-engineering-assignment
Name : Kalyani 
Roll no : A50105222145
College : Amity University Gurugram
Branch: Computer Science / Data Engineering  

---

## Overview

End-to-end data engineering pipeline over the 
[eCommerce Behavior Dataset](https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store).
Covers schema design, ETL, benchmarking, data quality, and analytical queries
across ~14M events from October and November 2019.

---

## Repo structure

```
├── README.md
├── data/
│   └── raw/               # empty — CSVs not committed (see .gitignore)
├── schema/
│   ├── er_diagram.png
│   └── ddl.sql
├── pipeline/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── run_pipeline.py
├── notebooks/
│   ├── 01_schema_design.ipynb
│   ├── 02_etl_pipeline.ipynb
│   ├── 03_benchmarks.ipynb
│   └── 04_queries.ipynb
├── reports/
│   ├── performance_report.pdf
│   ├── performance_charts.png
│   ├── q1_funnel.csv
│   ├── q2_sessions.csv
│   ├── q3_top_brands.csv
│   ├── q4_churn.csv
│   ├── q5_hourly.csv
│   └── pipeline_run.json
└── requirements.txt
```

---

## Reproduction steps

### 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/your-name-data-engineering-assignment.git
cd your-name-data-engineering-assignment
pip install -r requirements.txt
```

### 2. Add the raw data
Download `2019-Oct.csv` and `2019-Nov.csv` from the assignment Drive link.
Place them in `data/raw/`. They are gitignored and must not be committed.

### 3. Run schema setup
Open `notebooks/01_schema_design.ipynb` and run all cells.
This creates `ecommerce.duckdb` and validates the schema.

### 4. Run the ETL pipeline
```bash
python pipeline/run_pipeline.py
```
Or run all cells in `notebooks/02_etl_pipeline.ipynb`.
Re-running on the same files is safe — the pipeline is idempotent.

### 5. Run benchmarks
Open `notebooks/03_benchmarks.ipynb` and run all cells.
Outputs charts and `reports/pipeline_run.json`.

### 6. Run analytical queries
Open `notebooks/04_queries.ipynb` and run all cells.
Results saved to `reports/` as CSV files.

---

## Performance benchmarks

### Data load time

| File       | Rows      | Time (s) | Rows/sec  |
|------------|-----------|----------|-----------|
| 2019-Oct   | X,XXX,XXX | XX.Xs    | X,XXX,XXX |
| 2019-Nov   | X,XXX,XXX | XX.Xs    | X,XXX,XXX |
| **Total**  | XX,XXX,XXX| XX.Xs    | X,XXX,XXX |

### Batch insert throughput

| Batch size | Time (s) | Rows/sec  |
|------------|----------|-----------|
| 10,000     | XX.Xs    | X,XXX,XXX |
| 50,000     | XX.Xs    | X,XXX,XXX |
| 100,000    | XX.Xs    | X,XXX,XXX |
| 500,000    | XX.Xs    | X,XXX,XXX |

**Optimal batch size:** XXX,XXX rows

### Memory usage

| Metric            | Value   |
|-------------------|---------|
| Baseline RAM      | XXX MB  |
| Peak RAM          | XXX MB  |
| Pipeline overhead | XXX MB  |

### Disk I/O

| Source          | Size (MB) |
|-----------------|-----------|
| 2019-Oct.csv    | XXXX      |
| 2019-Nov.csv    | XXXX      |
| Raw total       | XXXX      |
| ecommerce.duckdb| XXXX      |
| Compression ratio| X.XXx   |

### Query execution time: index vs no index

| Query       | No index (s) | With index (s) | Speedup |
|-------------|--------------|----------------|---------|
| Q1 Funnel   | X.XXXX       | X.XXXX         | X.Xx    |
| Q3 Brands   | X.XXXX       | X.XXXX         | X.Xx    |
| Q5 Hourly   | X.XXXX       | X.XXXX         | X.Xx    |

---

## Schema design decisions

The schema follows 3NF with one fact table (`fact_events`) and three
dimension tables (`dim_product`, `dim_category`, `dim_user`).
`price` lives in `fact_events` because the same product appears at
different prices across events — it is a transactional measurement,
not a product attribute. `category_id` is denormalised into
`fact_events` to avoid a join through `dim_product` in the funnel
query, which operates on tens of millions of rows.
Every index maps directly to a WHERE, JOIN, or GROUP BY clause in
one of the five analytical queries.

---

## Known limitations

- Timestamps are in UTC; no timezone conversion applied.
- `brand` and `category_code` are nullable — approximately XX% of
  rows have no brand in the raw data.
- `event_id` is a truncated MD5 hash — collision probability is
  negligible at this scale but not zero.
- DuckDB is single-node; horizontal scaling would require migration
  to a distributed engine (Spark, BigQuery, Redshift).
