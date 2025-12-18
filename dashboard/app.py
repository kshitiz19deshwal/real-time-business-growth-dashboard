import streamlit as st
import pandas as pd
import plotly.express as px
import os

# ------------------------------------
# PAGE CONFIG
# ------------------------------------
st.set_page_config(
    page_title="Real-Time Business Growth Dashboard",
    layout="wide"
)

# ------------------------------------
# AUTO REFRESH (EVERY 60 SECONDS)
# ------------------------------------
from streamlit.runtime.scriptrunner import add_script_run_ctx
from streamlit_autorefresh import st_autorefresh

st_autorefresh(interval=60 * 1000, key="auto_refresh")

# ------------------------------------
# RUN PIPELINE ON EVERY REFRESH
# ------------------------------------
def run_pipeline():
    os.system("python src/ingestion.py")
    os.system("python src/processing.py")
    os.system("python src/metrics.py")
    os.system("python src/intelligence.py")

run_pipeline()

# ------------------------------------
# LOAD DATA
# ------------------------------------
@st.cache_data(show_spinner=False)
def load_data():
    core = pd.read_csv("data/processed/core_kpis.csv")
    growth = pd.read_csv("data/processed/growth_metrics.csv")
    risk = pd.read_csv("data/processed/risk_metrics.csv")
    demand = pd.read_csv("data/processed/demand_alignment.csv")

    growth["event_time"] = pd.to_datetime(growth["event_time"])
    risk["event_time"] = pd.to_datetime(risk["event_time"])
    demand["event_time"] = pd.to_datetime(demand["event_time"])

    return core, growth, risk, demand

core, growth, risk, demand = load_data()

# ------------------------------------
# TITLE
# ------------------------------------
st.title("📊 Real-Time Business Performance & Growth Dashboard")
st.caption("Auto-refreshing every 60 seconds | Live data + intelligence")

# ------------------------------------
# KPIs
# ------------------------------------
st.subheader("🔑 Executive KPIs")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Revenue", round(core["total_revenue"][0], 2))
c2.metric("Avg Hourly Revenue", round(core["avg_hourly_revenue"][0], 2))
c3.metric("Avg Price", round(core["avg_price"][0], 2))
c4.metric("Total Trades", int(core["total_trades"][0]))

# ------------------------------------
# REVENUE GROWTH
# ------------------------------------
st.subheader("📈 Revenue Growth")

growth_plot = growth.dropna(subset=["revenue_growth_pct"])

fig_growth = px.line(
    growth_plot,
    x="event_time",
    y="revenue_growth_pct",
    title="Revenue Growth (%) Over Time"
)

st.plotly_chart(fig_growth, width="stretch")

# ------------------------------------
# VOLATILITY RISK
# ------------------------------------
st.subheader("🚨 Volatility Risk")

risk_plot = risk.dropna(subset=["price_volatility"])

fig_risk = px.line(
    risk_plot,
    x="event_time",
    y="price_volatility",
    title="Price Volatility Over Time"
)

st.plotly_chart(fig_risk, width="stretch")

if risk["high_volatility_flag"].sum() > 0:
    st.warning("⚠️ High volatility detected in recent periods")
else:
    st.success("✅ Market volatility is within safe limits")

# ------------------------------------
# DEMAND VS REVENUE
# ------------------------------------
st.subheader("🔍 Demand vs Revenue Alignment")

demand_plot = demand.dropna(subset=["revenue_trend", "demand_trend"])

fig_demand = px.line(
    demand_plot,
    x="event_time",
    y=["revenue_trend", "demand_trend"],
    title="Demand vs Revenue Trend Comparison"
)

st.plotly_chart(fig_demand, width="stretch")

# ------------------------------------
# AUTO INSIGHTS
# ------------------------------------
st.subheader("🧠 Automated Business Insights")

with open("data/processed/auto_insights.txt", "r", encoding="utf-8") as f:
    st.text(f.read())

# ------------------------------------
# DEBUG PANEL (VERY IMPORTANT)
# ------------------------------------
with st.expander("🔎 Data Health Check"):
    st.write("Growth rows:", growth.shape)
    st.write("Risk rows:", risk.shape)
    st.write("Demand rows:", demand.shape)
    st.dataframe(growth.tail())
