from datetime import datetime
from sqlalchemy import create_engine, text, exc
import pandas as pd
from pathlib import Path


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

    def query_crop_data(self, year, season, scheme):
        try:
            if self.engine:
                query = text(
                    f"""
                    SELECT "S. No", "State/UT", "Notified Districts", "Insurance Units", "Farmers",
                           "Application Loanee", "Application Non-Loanee", "Thousand Hect. Area Insured", "Amount In Lac. Farmers Premium",
                           "Amount In Lac. State Premium", "Amount In Lac. GOI Premium", "Amount In Lac. Gross Premium", "Sum Insured (In Lac.)",
                           "Gender (%) Male", "Gender (%) Female", "Gender (%) Others", "Category (%) SC", "Category (%) ST",
                           "Category (%) OBC", "Category (%) GEN", "Type of Farmer (%) Marginal", "Type of Farmer (%) Small",
                           "Type of Farmer (%) Others", "Claim Paid (In Lac.) Prevented Sowing", "Claim Paid (In Lac.) Localized",
                           "Claim Paid (In Lac.) Mid-term", "Claim Paid (In Lac.) Yield Based", "Claim Paid (In Lac.) Post Harvest",
                           "Claim Paid (In Lac.) WBCIS", "Claim Paid (In Lac.) Total Claim Paid", "Claim Paid (In Lac.) Total Farmer Benefit(Actual)"
                    FROM crop_states
                    WHERE "Year" = :year AND "Season" = :season AND "Scheme" = :scheme
                    """
                )

                with self.engine.connect() as conn:
                    result = conn.execute(
                        query, {"year": year, "season": season, "scheme": scheme}
                    )
                    result_df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    return result_df
            else:
                print("Engine is not initialized. Cannot query data.")
                return None
        except Exception as e:
            print(f"An error occurred while querying data: {e}")
            return None

    def query_district_data(self, year, season, scheme, state):
        try:
            if self.engine:
                query = text(
                    f"""
                    SELECT "Serial Number", "District Name", "Insurance Units", "Farmers",
                           "Loanee Application", "Non-Loanee Application", "Area Insured", "Farmers Premium", "State Premium",
                           "GOI Premium", "Gross Premium", "Sum Insured (In Lac.)", "Male Gender (%)", "Female Gender (%)",
                           "Other Gender (%)", "Scheduled Caste (%)", "Scheduled Tribe (%)", "Other Backward Class (%)", "General (%)",
                           "Marginal Farmer (%)", "Small Farmer (%)", "Other Farmer Type (%)", "Prevented Sowing Claim", "Localized Claim",
                           "Mid-term Claim", "Yield Based Claim", "Post Harvest Claim", "WBCIS Claim", "Total Claim Paid",
                           "Total Farmer Benefit (Actual)"
                    FROM crop_districts
                    WHERE "Year" = :year AND "Season" = :season AND "Scheme" = :scheme AND "State Name" = :state
                    """
                )

                with self.engine.connect() as conn:
                    result = conn.execute(
                        query,
                        {
                            "year": year,
                            "season": season,
                            "scheme": scheme,
                            "state": state,
                        },
                    )
                    result_df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    return result_df
            else:
                print("Engine is not initialized. Cannot query data.")
                return None
        except Exception as e:
            print(f"An error occurred while querying data: {e}")
            return None
