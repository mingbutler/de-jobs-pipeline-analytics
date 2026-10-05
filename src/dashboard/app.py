import pandas as pd
import plotly.express as px
import streamlit as st
from databricks import sql

st.set_page_config(page_title="DE Job Market", layout="wide")

@st.cache_data(ttl=3600)
def load(table: str) -> pd.DataFrame:
    with sql.connect(
        server_hostname=st.secrets["DATABRICKS_HOST"],
        http_path=st.secrets["DATABRICKS_HTTP_PATH"],
        access_token=st.secrets["DATABRICKS_TOKEN"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM workspace.gold.{table}")
            return cur.fetchall_arrow().to_pandas()

skills = load("skill_demand")
exp = load("experience_requirements")
loc = load("location_salary_trend")

st.title("Data Engineering Job Market")

# KPIs
c1, c2, c3 = st.columns(3)
c1.metric("Distinct skills tracked", len(skills))
c2.metric("Top skill", skills.iloc[0]["skill"])
c3.metric("Cities hiring", len(loc))

# Skills
st.subheader("Most in-demand skills")
n = st.slider("Show top N", 5, 40, 15)
top = skills.nlargest(n, "posting_count").sort_values("posting_count")
st.plotly_chart(px.bar(top, x="posting_count", y="skill", orientation="h"), width='stretch')

# Experience
st.subheader("Years of experience required")
order = ["0-2", "3-5", "5-8", "8+", "Not Found"]
st.plotly_chart(px.bar(exp, x="experience_range", y="count", category_orders={"experience_range": order}), width='stretch')

# Location
st.subheader("Where the jobs are")
states = sorted(loc["state"].unique())
picked = st.multiselect("Filter by state", states)
view = loc[loc["state"].isin(picked)] if picked else loc
st.dataframe(view, width='stretch', hide_index=True)