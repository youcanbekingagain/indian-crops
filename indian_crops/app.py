import streamlit as st
import pandas as pd
import geopandas as gpd
import plotly.express as px
from query_database import PostgresDataHandler
from dotenv import load_dotenv
import os


# PostgreSQL connection using session state to persist the connection
def get_postgres_handler():
    if "data_handler" not in st.session_state:
        username = "avnadmin"
        password = os.getenv("AIVEN_PASSWD")
        host = "pg-244c7c97-rainfall-climate.i.aivencloud.com"
        port = "14469"
        database_name = "defaultdb"
        st.session_state.data_handler = PostgresDataHandler(
            username, password, host, port, database_name
        )
    return st.session_state.data_handler


# Load GeoJSON boundaries
@st.cache_data
def load_geojson(filepath):
    return gpd.read_file(filepath)


# Query data from PostgreSQL using PostgresDataHandler
def query_rainfall_data(data_handler, frequency, date):
    return data_handler.query_table(query_date=date, frequency=frequency)


# Plot rainfall data
def plot_india_states(geojson_data, rainfall_data):
    fig = px.choropleth_mapbox(
        rainfall_data,
        geojson=geojson_data,
        locations="state",
        featureidkey="properties.ST_NM",
        color="rain",
        color_continuous_scale="Viridis",
        mapbox_style="carto-positron",
        zoom=3,
        center={"lat": 22.5937, "lon": 78.9629},
        opacity=0.5,
        labels={"rain": "Rainfall (mm)"},
    )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    st.plotly_chart(fig)


# Plot the districts of a selected state
def plot_state_districts(geojson_data, rainfall_data, selected_state):
    state_districts = geojson_data[geojson_data["ST_NM"] == selected_state]

    # Re-project to a projected CRS
    state_districts = state_districts.to_crs(epsg=3857)

    fig = px.choropleth_mapbox(
        rainfall_data,
        geojson=state_districts,
        locations="district",
        featureidkey="shapeName",
        color="rain",
        color_continuous_scale="Viridis",
        mapbox_style="carto-positron",
        zoom=6,
        center={
            "lat": state_districts.geometry.centroid.y.mean(),
            "lon": state_districts.geometry.centroid.x.mean(),
        },
        opacity=0.5,
        labels={"rain": "Rainfall (mm)"},
    )
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    st.plotly_chart(fig)


# Streamlit app
def main():
    st.title("Indian District Rainfall Data Visualization")
    states_geojson_filepath = (
        "./indian_states.geojson"  # Path to your states GeoJSON file
    )
    districts_geojson_filepath = "./geoBoundaries-IND-ADM2_simplified.geojson"  # Path to your districts GeoJSON file
    states_geojson_data = load_geojson(states_geojson_filepath)
    districts_geojson_data = load_geojson(districts_geojson_filepath)

    region = st.sidebar.selectbox("Select region:", ["India", "Middle East"], index=0)

    frequency = st.sidebar.radio(
        "Select frequency:", ["daily", "monthly", "yearly"], index=0
    )

    if frequency == "daily":
        selected_date = st.sidebar.date_input(
            "Select date:",
            pd.to_datetime("2023-01-01"),
            min_value=pd.to_datetime("2018-01-01"),
            max_value=pd.to_datetime("2023-12-31"),
        )
    elif frequency == "monthly":
        selected_year = st.sidebar.selectbox("Select year:", range(2018, 2024))
        selected_month = st.sidebar.selectbox(
            "Select month:",
            [
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
            ],
        )
        selected_date = pd.to_datetime(f"{selected_year}-{selected_month}-01")
    else:  # yearly
        selected_year = st.sidebar.selectbox("Select year:", range(2018, 2024))
        selected_date = pd.to_datetime(f"{selected_year}-01-01")

    # PostgreSQL connection
    data_handler = get_postgres_handler()

    # Query rainfall data
    if region == "India":
        rainfall_data = query_rainfall_data(data_handler, frequency, selected_date)

        # Aggregating data by state
        rainfall_data_state = (
            rainfall_data.groupby("state")
            .agg({"rain": "mean", "tmin": "mean", "tmax": "mean"})
            .reset_index()
        )

        # Plot states of India
        plot_india_states(states_geojson_data, rainfall_data_state)

        # Get selected state from map click (for demonstration, we'll use a selectbox)
        selected_state = st.selectbox(
            "Select a state to zoom in:", rainfall_data_state["state"].unique()
        )

        # Aggregating data by district within the selected state
        rainfall_data_district = rainfall_data[rainfall_data["state"] == selected_state]

        # Plot districts of the selected state
        plot_state_districts(
            districts_geojson_data, rainfall_data_district, selected_state
        )
    elif region == "Middle East":
        st.write("Middle East map visualization is not yet implemented.")


if __name__ == "__main__":
    main()
