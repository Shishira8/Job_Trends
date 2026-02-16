# IT Job Skill Trends by Seniority

This project analyzes **job trends in the IT industry** to understand **which technical skills are in demand** and how that demand varies by **seniority level** (entry, mid, senior).

The goal is to answer questions like:
- Which skills are most commonly requested across IT roles?
- How does skill demand differ between entry-level and senior roles?
- How many **distinct companies** are asking for a given skill at each level?

This is an end-to-end **data engineering project** focused on building a reliable pipeline from raw job data to analytics-ready insights.

---

## Project Overview

The project collects job postings from public job data sources, processes and enriches them, and stores the results in a cloud data warehouse for analysis.

Each job posting is:
- classified by **seniority level** (entry / mid / senior)
- analyzed to extract **technical skills**
- linked to the **company** requesting those skills

The final output enables analysis of **skill demand by company and seniority level**, which can be used for trend analysis and visualization.

---

## Tech Stack

**Programming & Data Processing**
- Python
- pandas

**Data Warehouse**
- Snowflake

**Ingestion & Transformation**
- API-based job data ingestion
- Custom Python logic for:
  - seniority classification
  - skill extraction and normalization

**Orchestration (planned)**
- Prefect (for scheduled batch pipelines)

**Visualization (planned)**
- Streamlit dashboard for interactive exploration

**Development Tools**
- Git & GitHub
- Virtual environments (venv)
- Environment-based configuration (`.env`)

---


## Pipeline Usage (MVP)

1. **Ingest jobs from API and store raw data in S3:**
  ```bash
  python ingestion/write_raw_to_s3.py
  ```
  (Fetches jobs from Adzuna and writes to S3 as JSON, partitioned by date)

2. **Transform and enrich jobs:**
  ```bash
  python transformations/transform.py
  ```
  (Reads raw jobs from S3, dedupes, normalizes, classifies seniority, extracts skills)

3. **Load to Snowflake:**
  ```bash
  python warehouse/load_snowflake.py
  ```
  (Loads transformed data into Snowflake stage tables and merges into final tables)

**.env file must be configured with your Snowflake, AWS, and Adzuna credentials.**

---

## Current Status

✅ Ingestion, transformation, and Snowflake loading are working end-to-end.

Planned updates:
- Analytics SQL queries and dashboard
- Orchestration and automation

---

## Why This Project

This project is designed to demonstrate:
- real-world data engineering workflows
- cloud data warehousing with Snowflake
- data modeling for analytics
- transforming messy, unstructured data into actionable insights
