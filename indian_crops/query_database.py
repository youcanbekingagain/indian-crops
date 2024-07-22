from datetime import datetime
from sqlalchemy import create_engine, text, exc
import pandas as pd
from pathlib import Path

load_dotenv()


class PostgresDataHandler:
    def __init__(
        self,
        username,
        password,
        host="localhost",
        port="3000",
        database_name="postgres",
        role=None,
    ):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database_name = database_name
        self.role = role
        self.engine = self.create_engine()

    def create_engine(self):
        try:
            if self.role:
                db_url = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database_name}?options=-c%20role={self.role}"
            else:
                db_url = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database_name}"
            engine = create_engine(db_url)
            print("Successfully connected to the database.")
            return engine
        except Exception as e:
            print(f"An error occurred while connecting to the database: {e}")
            return None

    def store_data_in_postgres(self, df, table_name="rainfall_data"):
        try:
            if self.engine:
                df.to_sql(table_name, self.engine, if_exists="append", index=False)
                print(f"Data successfully stored in {table_name} table.")
            else:
                print("Engine is not initialized. Cannot store data.")
        except Exception as e:
            print(f"An error occurred while storing data: {e}")

    def query_data(
        self,
        latitude,
        longitude,
        query_date,
        table_name="rainfall_data",
        initial_tolerance=0.4,
    ):
        try:
            if self.engine:
                query = text(
                    f"""
                    SELECT lat, lon, time, rain, tmin, tmax
                    FROM {table_name}
                    WHERE time::date = :time
                    AND lat BETWEEN :lat_min AND :lat_max
                    AND lon BETWEEN :lon_min AND :lon_max
                    AND rain != -999
                    ORDER BY ABS(lat - :lat) + ABS(lon - :lon) ASC
                    LIMIT 1
                    """
                )
                with self.engine.connect() as conn:
                    result = conn.execute(
                        query,
                        {
                            "lat_min": latitude - initial_tolerance,
                            "lat_max": latitude + initial_tolerance,
                            "lon_min": longitude - initial_tolerance,
                            "lon_max": longitude + initial_tolerance,
                            "lat": latitude,
                            "lon": longitude,
                            "time": query_date,
                        },
                    )

                    result_df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    # If data is found, return the dataframe
                    if not result_df.empty:
                        return result_df
                    else:
                        return pd.DataFrame(
                            [
                                (
                                    latitude,
                                    longitude,
                                    query_date + " 00:00:00",
                                    -999,
                                    -999,
                                    -999,
                                )
                            ],
                            columns=["lat", "lon", "time", "rain", "tmin", "tmax"],
                        )

            else:
                print("Engine is not initialized. Cannot query data.")
                return None
        except Exception as e:
            print(f"An error occurred while querying data: {e}")
            return None

    def mid_east_query_data(
        self, country, query_year, query_month, table_name="middle_east"
    ):
        try:
            if self.engine:
                query = text(
                    f"""
                    SELECT year, month, tmax, tmin, precipitation
                    FROM {table_name}
                    WHERE country = :country
                    AND year = :year
                    AND month ILIKE :month  -- Case insensitive comparison
                    """
                )

                with self.engine.connect() as conn:
                    result = conn.execute(
                        query,
                        {
                            "country": country,
                            "year": query_year,
                            "month": query_month,  # No need for .upper() or .capitalize()
                        },
                    )

                    result_df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    # If data is found, return the dataframe
                    if not result_df.empty:
                        return result_df
                    else:
                        return pd.DataFrame(
                            {
                                "country": [country],
                                "year": [query_year],
                                "month": [
                                    query_month
                                ],  # Return the input query_month as is
                                "tmax": [-999],
                                "tmin": [-999],
                                "precipitation": [-999],
                            }
                        )

            else:
                print("Engine is not initialized. Cannot query data.")
                return None
        except Exception as e:
            print(f"An error occurred while querying data: {e}")
            return None

    def query_table(self, query_date, frequency):
        try:
            if not self.engine:
                print("Engine is not initialized. Cannot query data.")
                return None

            if frequency == "daily":
                table_name = "rainfall_data_partitioned_districts"
                query = text(
                    f"""
                    SELECT state, district, rain, tmin, tmax
                    FROM {table_name}
                    WHERE time::date = :time
                    """
                )
                params = {"time": query_date}
            elif frequency == "monthly":
                table_name = "rainfall_data_partitioned_districts_monthly"
                query = text(
                    f"""
                    SELECT state, district, rain, tmin, tmax
                    FROM {table_name}
                    WHERE year = :year AND month = :month
                    """
                )
                params = {
                    "year": query_date.year,
                    "month": query_date.strftime("%b").upper(),
                }
            elif frequency == "yearly":
                table_name = "rainfall_data_partitioned_districts_yearly"
                query = text(
                    f"""
                    SELECT state, district, rain, tmin, tmax
                    FROM {table_name}
                    WHERE year = :year
                    """
                )
                params = {"year": query_date.year}
            else:
                raise ValueError(
                    "Invalid frequency. Choose from 'daily', 'monthly', or 'yearly'."
                )

            # Print the query and params for debugging
            print("SQL Query for Debugging:")
            print(query)
            print("Params for Debugging:")
            print(params)

            with self.engine.connect() as conn:
                result = conn.execute(query, params)
                result_df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return result_df

        except Exception as e:
            print(f"An error occurred while querying data: {e}")
            return None


# Usage
username = "postgres"
password = "Shadowreaper#1"
host = "localhost"
port = "3000"
database_name = "postgres"
role = "amanadmin"  # Add your role here

data_handler = PostgresDataHandler(username, password, host, port, database_name, role)
