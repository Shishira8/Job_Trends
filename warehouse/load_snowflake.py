
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from dotenv import load_dotenv
import pandas as pd
from transformations.transform import read_raw_from_s3, transform_jobs
from warehouse.snowflake_conn import get_snowflake_conn, truncate_stage_tables, write_stage_tables, merge_stage_to_final
from datetime import datetime

load_dotenv()

logger = logging.getLogger("load_snowflake")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def load_to_snowflake(ingestion_date: str):
    jobs = read_raw_from_s3(ingestion_date)
    jobs_df, skills_df = transform_jobs(jobs, ingestion_date)
    conn = get_snowflake_conn()
    try:
        truncate_stage_tables(conn)
        write_stage_tables(conn, jobs_df, skills_df)
        merge_stage_to_final(conn)
        logger.info("Data loaded to Snowflake successfully.")
    finally:
        conn.close()

if __name__ == "__main__":
    today = datetime.utcnow().strftime("%Y-%m-%d")
    load_to_snowflake(today)
