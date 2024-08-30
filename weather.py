import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

# Define Snowflake SQLAlchemy engine
engine = create_engine(URL(
    account='hllhsod-ks58901',
    user='ANUJA',
    password='Ijkl@123',
    warehouse='ETL_WH',
    database='WEATHER_DB',
    schema='WEATHER_SCH'
))

# Function to load data from Snowflake into a DataFrame
def load_data_from_snowflake(table_name: str) -> pd.DataFrame:
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, con=engine)
    return df


def load_and_join_data():
    weather_blr_df = load_data_from_snowflake('weather_bangalore')
    weather_hyd_df = load_data_from_snowflake('weather_hyderabad')

    #Load the data into snowflake date wise by merging weather data for bangalore and hyderabad
    joined_df = pd.merge(weather_blr_df, weather_hyd_df, how='inner', on='date')
    joined_df.to_sql('weather_daily', con=engine, if_exists="append", index=False)

    #joining both table to combine the data
    combine_df=pd.concat([weather_blr_df,weather_hyd_df], ignore_index=True)
    combine_df.to_sql('weather', con=engine, if_exists="append", index=False)


if __name__ == "__main__":
    load_and_join_data()
