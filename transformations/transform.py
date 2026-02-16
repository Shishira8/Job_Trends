import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd

import boto3
from dotenv import load_dotenv
from .skills import extract_skills

load_dotenv()

logger = logging.getLogger("transform")
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

def read_raw_from_s3(ingestion_date: str) -> List[Dict[str, Any]]:
    key = f"raw/ingestion_date={ingestion_date}/jobs.json"
    logger.info(f"Reading raw jobs from s3://{S3_BUCKET}/{key}")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return json.loads(obj["Body"].read())

def normalize_company(name: str) -> str:
    if not name:
        return "Unknown"
    return name.strip().lower().replace(", inc.", "").replace(" inc", "").replace(" ltd", "").replace(" llc", "").replace(".com", "").replace(" inc.", "").replace("  ", " ").title()

def classify_level(title: str, description: str) -> str:
    text = f"{title} {description}".lower()
    if any(x in text for x in ["intern", "junior", "entry level", "graduate"]):
        return "entry"
    if any(x in text for x in ["senior", "lead", "principal", "staff"]):
        return "senior"
    if any(x in text for x in ["mid-level", "mid level", "midlevel", "associate"]):
        return "mid"
    return "mid"  # default

def dedupe_jobs(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    deduped = []
    for job in jobs:
        job_id = job.get("id")
        if job_id and job_id not in seen:
            deduped.append(job)
            seen.add(job_id)
    return deduped

def transform_jobs(jobs: List[Dict[str, Any]], ingestion_date: str) -> (pd.DataFrame, pd.DataFrame):
    deduped = dedupe_jobs(jobs)
    logger.info(f"Deduped to {len(deduped)} jobs")
    job_rows = []
    skill_rows = []
    for job in deduped:
        job_id = job.get("id")
        company = normalize_company(job.get("company", {}).get("display_name", ""))
        title = job.get("title", "")
        description = job.get("description", "")
        posted_date = job.get("created", "")
        location = job.get("location", {}).get("display_name", "")
        level = classify_level(title, description)
        raw_payload = json.dumps(job)
        job_rows.append({
            "JOB_ID": job_id,
            "COMPANY": company,
            "TITLE": title,
            "LEVEL": level,
            "POSTED_DATE": posted_date,
            "LOCATION": location,
            "RAW_PAYLOAD": raw_payload,
            "INGESTED_AT": ingestion_date,
        })
        skills = extract_skills(f"{title} {description}")
        for skill in skills:
            skill_rows.append({
                "JOB_ID": job_id,
                "SKILL": skill,
                "INGESTED_AT": ingestion_date,
            })
    jobs_df = pd.DataFrame(job_rows)
    skills_df = pd.DataFrame(skill_rows)
    return jobs_df, skills_df

if __name__ == "__main__":
    # Example usage: transform jobs for today's date
    today = datetime.utcnow().strftime("%Y-%m-%d")
    jobs = read_raw_from_s3(today)
    jobs_df, skills_df = transform_jobs(jobs, today)
    print(jobs_df.head())
    print(skills_df.head())
