import streamlit as st
import pandas as pd
from query_database import PostgresDataHandler
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()


# Initialize PostgreSQL handler
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


# Function to highlight min and max values in dataframe
def highlight_min_max(data):
    def highlight_col(col):
        try:
            numeric_data = pd.to_numeric(col.str.replace(",", ""), errors="coerce")
            min_val = numeric_data[:-1].min()  # Exclude the last row
            max_val = numeric_data[:-1].max()  # Exclude the last row
            return [
                (
                    "background-color: yellow"
                    if v == min_val
                    else "background-color: lightgreen" if v == max_val else ""
                )
                for v in numeric_data
            ]
        except ValueError:
            return ["" for _ in col]

    # Apply the function to all columns except the first one
    styles = data.iloc[:, 1:].apply(highlight_col, axis=0)
    # Add empty styles for the first column
    first_col_style = pd.DataFrame(
        [""] * len(data), index=data.index, columns=[data.columns[0]]
    )
    # Concatenate the first column styles with the rest
    return pd.concat([first_col_style, styles], axis=1)


def main():
    st.title("Crop Insurance Data Explorer")

    handler = get_postgres_handler()

    # Sidebar for selection
    st.sidebar.header("Filter Options")
    years = [
        "2018",
        "2019",
        "2020",
        "2021",
        "2022",
        "2023",
        "2024",
    ]  # Update with your available years
    seasons = ["Rabi", "Kharif"]  # Update with your available seasons
    schemes = ["WBCIS", "PMFBY"]  # Update with your available schemes

    selected_year = st.sidebar.selectbox("Select Year", years)
    selected_season = st.sidebar.selectbox("Select Season", seasons)
    selected_scheme = st.sidebar.selectbox("Select Scheme", schemes)

    if "state_view" not in st.session_state:
        st.session_state.state_view = False

    if not st.session_state.state_view:
        df = handler.query_crop_data(selected_year, selected_season, selected_scheme)
        if df is not None and not df.empty:
            df.reset_index(drop=True, inplace=True)
            styled_df = df.style.apply(highlight_min_max, axis=None)
            st.dataframe(styled_df, use_container_width=True)
            state = st.sidebar.selectbox(
                "Select State to view Districts", df["State/UT"].unique()
            )

            if st.sidebar.button("View Districts"):
                st.session_state.selected_state = state
                st.session_state.state_view = True
        else:
            st.write("No data available for the selected filters.")
    else:
        state = st.session_state.selected_state
        df = handler.query_district_data(
            selected_year, selected_season, selected_scheme, state
        )
        if df is not None and not df.empty:
            df.reset_index(drop=True, inplace=True)  # Remove default numbering
            styled_df = df.style.apply(highlight_min_max, axis=None)
            st.dataframe(styled_df, use_container_width=True)

            if st.sidebar.button("Back to States"):
                st.session_state.state_view = False
        else:
            st.write("No data available for the selected filters.")


if __name__ == "__main__":
    main()
