import os
import requests
import logging
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
import time

load_dotenv()

ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")

logger = logging.getLogger("fetch_jobs")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

API_URL = "https://api.adzuna.com/v1/api/jobs/us/search/{}"

RATE_LIMIT_SLEEP = 1.2  # seconds between requests
MAX_RETRIES = 3


def fetch_jobs(
    query: str = "software",
    location: str = "",
    results_per_page: int = 50,
    max_pages: int = 10,
    app_id: Optional[str] = None,
    app_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch job postings from Adzuna API with paging and retries.
    """
    app_id = app_id or ADZUNA_APP_ID
    app_key = app_key or ADZUNA_APP_KEY
    all_jobs = []
    for page in range(1, max_pages + 1):
        params = {
            "app_id": app_id,
            "app_key": app_key,
            "results_per_page": results_per_page,
            "what": query,
            "where": location,
            "content-type": "application/json",
        }
        url = API_URL.format(page)
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(f"Fetching page {page} (attempt {attempt})")
                resp = requests.get(url, params=params, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                jobs = data.get("results", [])
                logger.info(f"Fetched {len(jobs)} jobs from page {page}")
                all_jobs.extend(jobs)
                break
            except Exception as e:
                logger.warning(f"Error fetching page {page} (attempt {attempt}): {e}")
                if attempt == MAX_RETRIES:
                    logger.error(f"Failed to fetch page {page} after {MAX_RETRIES} attempts.")
                else:
                    time.sleep(RATE_LIMIT_SLEEP * attempt)
        time.sleep(RATE_LIMIT_SLEEP)
    return all_jobs

if __name__ == "__main__":
    jobs = fetch_jobs()
    print(f"Fetched {len(jobs)} jobs.")
