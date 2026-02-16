import os
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

st.set_page_config(page_title="IT Skill Demand by Seniority", layout="wide")

def get_conn():
    required = [
        "SNOWFLAKE_ACCOUNT","SNOWFLAKE_USER","SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE","SNOWFLAKE_WAREHOUSE","SNOWFLAKE_DATABASE","SNOWFLAKE_SCHEMA"
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing .env variables: {missing}")

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


@st.cache_data(ttl=600)
def run_query(sql: str) -> pd.DataFrame:
    """Runs a SQL query in Snowflake and returns a DataFrame."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=cols)
        finally:
            cur.close()
    finally:
        conn.close()

# --- SQL you said you already have ---
SKILL_LEVEL_COMPANY_SQL = """
SELECT
  js.skill AS SKILL,
  j.level AS LEVEL,
  COUNT(DISTINCT j.company) AS COMPANY_COUNT
FROM job_skills js
JOIN jobs j
  ON js.job_id = j.job_id
WHERE j.company IS NOT NULL
GROUP BY js.skill, j.level
ORDER BY js.skill, company_count DESC;
"""

TOP_SKILLS_SQL = """
SELECT
  js.skill AS SKILL,
  COUNT(DISTINCT j.company) AS DISTINCT_COMPANIES
FROM job_skills js
JOIN jobs j
  ON js.job_id = j.job_id
WHERE j.company IS NOT NULL
GROUP BY js.skill
ORDER BY DISTINCT_COMPANIES DESC
LIMIT 25;
"""

st.title("IT Job Skill Demand by Seniority")
st.caption("Counts are based on DISTINCT companies mentioning the skill in job postings.")

# Sidebar controls
st.sidebar.header("Controls")
show_debug = st.sidebar.toggle("Show raw SQL results", value=False)

# Load data
with st.spinner("Loading data from Snowflake..."):
    df = run_query(SKILL_LEVEL_COMPANY_SQL)
    top_df = run_query(TOP_SKILLS_SQL)

if df.empty:
    st.warning("No data returned. Verify JOBS/JOB_SKILLS tables have rows and your query is correct.")
    st.stop()

# Normalize column names (Snowflake returns uppercase by default)
df.columns = [c.lower() for c in df.columns]
top_df.columns = [c.lower() for c in top_df.columns]

# Clean levels order
LEVEL_ORDER = ["entry", "mid", "senior"]

# Skill selector
all_skills = sorted(df["skill"].dropna().unique().tolist())
default_skill = all_skills[0] if all_skills else None
selected_skill = st.selectbox("Select a skill", options=all_skills, index=0)

# Filter for selected skill
skill_df = df[df["skill"] == selected_skill].copy()
skill_df["company_count"] = pd.to_numeric(skill_df["company_count"], errors="coerce").fillna(0).astype(int)

# Ensure all levels exist for consistent chart
pivot = (
    skill_df.pivot_table(index="level", values="company_count", aggfunc="sum")
    .reindex(LEVEL_ORDER)
    .fillna(0)
    .reset_index()
)

# Layout
col1, col2 = st.columns([2, 1], gap="large")

with col1:
    st.subheader(f"Distinct companies requesting **{selected_skill}** by seniority")
    chart_df = pivot.set_index("level")
    st.bar_chart(chart_df["company_count"])

    st.markdown("#### Breakdown")
    st.dataframe(pivot, use_container_width=True)

with col2:
    st.subheader("Top skills (by distinct companies)")
    st.dataframe(top_df, use_container_width=True, height=520)

if show_debug:
    st.markdown("---")
    st.subheader("Raw query results")
    st.dataframe(df, use_container_width=True)
