import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Any
import boto3
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("write_raw_to_s3")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

def write_raw_to_s3(jobs: List[Dict[str, Any]], ingestion_date: str = None) -> str:
    """
    Write raw job postings to S3 as a JSON file, partitioned by ingestion_date.
    Returns the S3 key.
    """
    if not ingestion_date:
        ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")
    key = f"raw/ingestion_date={ingestion_date}/jobs.json"
    logger.info(f"Writing {len(jobs)} jobs to s3://{S3_BUCKET}/{key}")
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(jobs, indent=2),
            ContentType="application/json",
        )
        logger.info(f"Successfully wrote jobs to s3://{S3_BUCKET}/{key}")
        return key
    except Exception as e:
        logger.error(f"Failed to write to S3: {e}")
        raise

if __name__ == "__main__":
    # Example usage: fetch jobs and write to S3
    from fetch_jobs import fetch_jobs
    jobs = fetch_jobs()
    write_raw_to_s3(jobs)
