import duckdb
import logging
import json
from datetime import datetime
from pathlib import Path

from extract   import extract
from transform import transform
from load      import load

# ── config ───────────────────────────────────────────────────────────
DB_PATH    = '/content/drive/MyDrive/YOUR_FOLDER/ecommerce.duckdb'
LOG_PATH   = '/content/drive/MyDrive/YOUR_FOLDER/reports/pipeline_run.json'
FILES      = {
    '2019-10': '/content/drive/MyDrive/YOUR_FOLDER/2019-Oct.csv',
    '2019-11': '/content/drive/MyDrive/YOUR_FOLDER/2019-Nov.csv',
}
CHUNK_SIZE = 100_000

# ── logging setup ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def run():
    run_log = {
        'start_time':       datetime.now().isoformat(),
        'files':            {},
        'total_extracted':  0,
        'total_loaded':     0,
        'quality_summary':  {}
    }

    con = duckdb.connect(DB_PATH)
    quality_log = {}

    for month, filepath in FILES.items():
        logger.info(f"── Processing {month} ──")
        file_log = {'rows_extracted': 0, 'rows_loaded': 0, 'chunks': 0}

        for chunk, rows_read, rows_skipped in extract(filepath, CHUNK_SIZE):

            transformed = transform(chunk, quality_log)
            inserted    = load(con, transformed)

            file_log['rows_extracted'] = rows_read
            file_log['rows_loaded']   += inserted
            file_log['chunks']        += 1

            if file_log['chunks'] % 10 == 0:
                logger.info(f"  {month}: {file_log['chunks']} chunks processed, "
                            f"{file_log['rows_loaded']:,} rows loaded so far")

        run_log['files'][month] = file_log
        run_log['total_extracted'] += file_log['rows_extracted']
        run_log['total_loaded']    += file_log['rows_loaded']
        logger.info(f"── {month} done: {file_log['rows_loaded']:,} rows loaded ──")

    run_log['end_time']       = datetime.now().isoformat()
    run_log['quality_summary'] = quality_log

    # data quality summary
    total = run_log['total_extracted']
    dropped = quality_log.get('rows_dropped', 0)
    pass_rate = round((total - dropped) / total * 100, 2) if total > 0 else 0
    run_log['quality_summary']['pass_rate_pct'] = pass_rate

    # write log
    Path(LOG_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, 'w') as f:
        json.dump(run_log, f, indent=2)

    logger.info(f"Pipeline complete — {run_log['total_loaded']:,} rows loaded")
    logger.info(f"Pass rate: {pass_rate}%")
    logger.info(f"Log written to {LOG_PATH}")
    con.close()


if __name__ == '__main__':
    run()
