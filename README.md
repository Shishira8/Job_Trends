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

## Current Status

🚧 This project is under active development.

Initial setup completed:
- Snowflake warehouse, database, schema, and core tables
- Local Python environment and project scaffolding
- Repository structure and configuration

Future updates will include:
- Automated ingestion pipelines
- Data transformations and loading logic
- Analytics queries
- Interactive dashboard
- Scheduling and monitoring

---

## Why This Project

This project is designed to demonstrate:
- real-world data engineering workflows
- cloud data warehousing with Snowflake
- data modeling for analytics
- transforming messy, unstructured data into actionable insights
