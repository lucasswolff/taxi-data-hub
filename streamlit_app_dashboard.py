import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

# get snowflake session
session = get_active_session()

st.title("🚕 NYC Taxi Trips Dashboard")
st.markdown("Visualizing taxi trips in NYC by pickup and dropoff locations.")

# load data from agg view
@st.cache_data
def load_data():
    query = """
        SELECT 
            pickup_year,
            pickup_month,
            pickup_day,
            pickup_borough,
            pickup_zone,
            dropoff_borough,
            dropoff_zone,
            number_of_trips,
            trip_distance,
            fare_amount,
            total_amount
        FROM AGG_DAILY_TRIPS_LOCATION
    """
    return session.sql(query).to_pandas()

df = load_data()

# filters side bar
st.sidebar.header("Filters")

#### Filters
years = sorted(df["PICKUP_YEAR"].unique())
selected_year = st.sidebar.selectbox("Year", years, index=len(years)-1)

months = sorted(df[df["PICKUP_YEAR"] == selected_year]["PICKUP_MONTH"].unique())
selected_month = st.sidebar.selectbox("Month", months)

pickup_boroughs = sorted(df["PICKUP_BOROUGH"].dropna().unique())
selected_pickup = st.sidebar.multiselect("Pickup Borough(s)", pickup_boroughs, default=pickup_boroughs)

dropoff_boroughs = sorted(df["DROPOFF_BOROUGH"].dropna().unique())
selected_dropoff = st.sidebar.multiselect("Dropoff Borough(s)", dropoff_boroughs, default=dropoff_boroughs)

# filter df to apply filters for daily view (KPIs, daily chart and borough chart)
filtered_df = df[
    (df["PICKUP_YEAR"] == selected_year) &
    (df["PICKUP_MONTH"] == selected_month) &
    (df["PICKUP_BOROUGH"].isin(selected_pickup)) &
    (df["DROPOFF_BOROUGH"].isin(selected_dropoff))
]

st.markdown(
    f"#### Showing data for **{selected_year}-{str(selected_month).zfill(2)}**"
)

# KPIs
st.subheader("Key Metrics")
col1, col2 = st.columns(2)
col1.metric("Total Trips", f"{filtered_df['NUMBER_OF_TRIPS'].sum():,}")
col2.metric("Avg Distance (miles)", f"{filtered_df['TRIP_DISTANCE'].mean():.2f}")

col3, col4  = st.columns(2)

col3.metric("Total Revenue ($)", f"{filtered_df['TOTAL_AMOUNT'].sum():,.0f}")

if filtered_df['NUMBER_OF_TRIPS'].sum() > 0:
    avg_per_trip = filtered_df['TOTAL_AMOUNT'].sum() / filtered_df['NUMBER_OF_TRIPS'].sum()
else:
    avg_per_trip = 0

col4.metric("Avg $ per Trip", f"{avg_per_trip:.2f}")


# trips per day chart
st.subheader("Trips per Day")

daily_df = (
    filtered_df.groupby("PICKUP_DAY", as_index=False)["NUMBER_OF_TRIPS"]
    .sum()
)

daily_chart = (
    alt.Chart(daily_df)
    .mark_bar()
    .encode(
        x=alt.X("PICKUP_DAY:O", title="Day"),
        y=alt.Y("NUMBER_OF_TRIPS:Q", title="Trips"),
        tooltip=["PICKUP_DAY", "NUMBER_OF_TRIPS"]
    )
)
st.altair_chart(daily_chart, use_container_width=True)

# pickup vs dropoff boroughs bar chart 
st.subheader("Pickup vs Dropoff Boroughs")

borough_df = (
    filtered_df.groupby(["PICKUP_BOROUGH", "DROPOFF_BOROUGH"], as_index=False)["NUMBER_OF_TRIPS"]
    .sum()
)

borough_chart = (
    alt.Chart(borough_df)
    .mark_bar()
    .encode(
        x=alt.X("PICKUP_BOROUGH:N", title="Pickup Borough"),
        y=alt.Y("NUMBER_OF_TRIPS:Q", title="Trips"),
        color="DROPOFF_BOROUGH:N",
        tooltip=["PICKUP_BOROUGH", "DROPOFF_BOROUGH", "NUMBER_OF_TRIPS"]
    )
)
st.altair_chart(borough_chart, use_container_width=True)


# monthly line chart (ignore year month filter) 
st.subheader("Monthly Trend of Trips (All Years)")

monthly_df = df[
    (df["PICKUP_BOROUGH"].isin(selected_pickup)) &
    (df["DROPOFF_BOROUGH"].isin(selected_dropoff))
].copy()

# Create YYYY-MM-01 string for display
monthly_df["YEAR_MONTH"] = (
    monthly_df["PICKUP_YEAR"].astype(str) + "-" +
    monthly_df["PICKUP_MONTH"].astype(str).str.zfill(2) + "-01"
)

monthly_trend = (
    monthly_df.groupby("YEAR_MONTH", as_index=False)
    .agg({"NUMBER_OF_TRIPS": "sum"})
)

line_chart = (
    alt.Chart(monthly_trend)
    .mark_line(point=True)
    .encode(
        x=alt.X("YEAR_MONTH:O", title="Month"),  # treat as ordered string, not date
        y=alt.Y("NUMBER_OF_TRIPS:Q", title="Trips"),
        tooltip=["YEAR_MONTH", "NUMBER_OF_TRIPS"]
    )
)
st.altair_chart(line_chart, use_container_width=True)