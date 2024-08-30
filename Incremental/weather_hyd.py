import requests
from dagster import asset, job, schedule
import pandas as pd
from flatten_json import flatten
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from datetime import date


# Create a Snowflake SQLAlchemy engine
engine = create_engine(URL(
    account='hllhsod-ks58901',
    user='ANUJA',
    password='Ijkl@123',
    warehouse='ETL_WH',
    database='WEATHER_DB',
    schema='WEATHER_SCH'
))
connection=engine.connect()


@asset(
    config_schema={
        'api_key': str,
        'location': str,
        'dt': str
    }
)
def fetch_weather_asset(context):
    api_key = context.op_config['api_key']
    location = context.op_config['location']
    dt = context.op_config['dt']
    return fetch_weather_data(api_key, location, dt)



#Extracting and transforming the data 
def fetch_weather_hyd_data(api_key: str, location: str, dt: str) -> dict:
    url = "http://api.weatherapi.com/v1/current.json"
    params = {
        'key': api_key,
        'q': location,
        'dt': dt
    }
    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise an exception for HTTP errors

    #flattening of json data
    data=flatten(response.json())
    df=pd.json_normalize(data)

    #drop column
    df=df.drop(columns="location_localtime_epoch")
    
    #update column names
    column_mapping = {
        "location_name": "location",
        "location_region": "state",
        "location_country": "country",
        "location_lat": "lat",
        "location_lon": "lon",
        "location_tz_id": "tz_id",
        "location_localtime": "local_time",
        "forecast_forecastday_0_date": "date",
        "forecast_forecastday_0_day_maxtemp_c": "maxtemp_c",
        "forecast_forecastday_0_day_maxtemp_f": "maxtemp_f",
        "forecast_forecastday_0_day_mintemp_c": "mintemp_c",
        "forecast_forecastday_0_day_mintemp_f": "mintemp_f",
        "forecast_forecastday_0_day_avgtemp_c": "avgtemp_c",
        "forecast_forecastday_0_day_avgtemp_f": "avgtemp_f",
        "forecast_forecastday_0_day_maxwind_mph": "maxwind_mph",
        "forecast_forecastday_0_day_maxwind_kph": "maxwind_kph",
        "forecast_forecastday_0_day_totalprecip_mm": "totalprecip_mm",
        "forecast_forecastday_0_day_totalprecip_in": "totalprecip_in",
        "forecast_forecastday_0_day_totalsnow_cm": "totalsnow_cm",
        "forecast_forecastday_0_day_avgvis_km": "avgvis_km",
        "forecast_forecastday_0_day_avgvis_miles": "avgis_miles",
        "forecast_forecastday_0_day_avghumidity": "avghumidity",
        "forecast_forecastday_0_day_daily_will_it_rain": "daily_will_it_rain",
        "forecast_forecastday_0_day_daily_chance_of_rain": "daily_chance_of_rain",
        "forecast_forecastday_0_day_daily_will_it_snow": "daily_will_it_snow",
        "forecast_forecastday_0_day_daily_chance_of_snow": "daily_chance_of_snow",
        "forecast_forecastday_0_day_condition_text": "condition_text",
        "forecast_forecastday_0_day_condition_icon": "condition_icon",
        "forecast_forecastday_0_day_condition_code": "condition_code",
        "forecast_forecastday_0_day_uv": "uv" 

    }
    df.rename(columns=column_mapping, inplace=True)
    return(load_data_to_snowflake(df))
    
#loading the data 
def load_data_to_snowflake(df: pd.DataFrame):
    #load data into snowflake using pandas' to_sql()
    #if table exist append 
    df.to_sql('weather_hyderabad', con=engine, if_exists="append", index=False)

    
@job
def weather_hyd_job():
    csv_data=fetch_weather_asset()


# Define the schedule to run the job every hour
@schedule(cron_schedule="0 * * * *", job=weather_hyd_job, execution_timezone="UTC")
def hourly_weather_hyd_schedule(_context):
    return {
        'ops': {
            'fetch_weather_asset': {
                'config': {
                    'api_key': 'a677fb9afa584a0d9e0131232242808',
                    'location': 'Hyderabad',
                    'dt': str(date.today())
                }
            }
        }
    }

def main():
    # Define the configuration for the job
    config = {
        'ops': {
            'fetch_weather_asset': {
                'config': {
                    'api_key': 'a677fb9afa584a0d9e0131232242808',  
                    'location': 'Hyderabad',
                    'dt': str(date.today())
                }
            }
        }
    }
    # Execute the job with the Dagster instance
    result = weather_hyd_job.execute_in_process(run_config=config)
    

if __name__ == "__main__":
    main()

