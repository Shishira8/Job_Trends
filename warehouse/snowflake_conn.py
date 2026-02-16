import os
import logging
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

load_dotenv()

logger = logging.getLogger("snowflake_conn")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def get_snowflake_conn():
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    return conn

def truncate_stage_tables(conn):
    with conn.cursor() as cur:
        logger.info("Truncating stage tables...")
        cur.execute("TRUNCATE TABLE IF EXISTS JOBS_STAGE;")
        cur.execute("TRUNCATE TABLE IF EXISTS JOB_SKILLS_STAGE;")
        logger.info("Stage tables truncated.")

def write_stage_tables(conn, jobs_df: pd.DataFrame, skills_df: pd.DataFrame):
    logger.info(f"Writing {len(jobs_df)} jobs to JOBS_STAGE...")
    write_pandas(conn, jobs_df, 'JOBS_STAGE')
    logger.info(f"Writing {len(skills_df)} skills to JOB_SKILLS_STAGE...")
    write_pandas(conn, skills_df, 'JOB_SKILLS_STAGE')
    logger.info("Write to stage tables complete.")

def merge_stage_to_final(conn):
    with conn.cursor() as cur:
        logger.info("Merging JOBS_STAGE into JOBS...")
        cur.execute("""
            MERGE INTO JOBS tgt
            USING JOBS_STAGE src
            ON tgt.JOB_ID = src.JOB_ID
            WHEN MATCHED THEN UPDATE SET
                COMPANY = src.COMPANY,
                TITLE = src.TITLE,
                LEVEL = src.LEVEL,
                POSTED_DATE = src.POSTED_DATE,
                LOCATION = src.LOCATION,
                RAW_PAYLOAD = src.RAW_PAYLOAD,
                INGESTED_AT = src.INGESTED_AT
            WHEN NOT MATCHED THEN INSERT (
                JOB_ID, COMPANY, TITLE, LEVEL, POSTED_DATE, LOCATION, RAW_PAYLOAD, INGESTED_AT
            ) VALUES (
                src.JOB_ID, src.COMPANY, src.TITLE, src.LEVEL, src.POSTED_DATE, src.LOCATION, src.RAW_PAYLOAD, src.INGESTED_AT
            );
        """)
        logger.info("Merging JOB_SKILLS_STAGE into JOB_SKILLS...")
        cur.execute("""
            MERGE INTO JOB_SKILLS tgt
            USING JOB_SKILLS_STAGE src
            ON tgt.JOB_ID = src.JOB_ID AND tgt.SKILL = src.SKILL
            WHEN MATCHED THEN UPDATE SET
                INGESTED_AT = src.INGESTED_AT
            WHEN NOT MATCHED THEN INSERT (
                JOB_ID, SKILL, INGESTED_AT
            ) VALUES (
                src.JOB_ID, src.SKILL, src.INGESTED_AT
            );
        """)
        logger.info("Merge complete.")
