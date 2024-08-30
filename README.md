# Weather-Data-pipeline

Data Source: weatherapi.com

Overview: I have extracted the data for two different locations (Bangalore: weather_blr.py and Hyderabad: weather_hyd.py) from weatherapi.com using dagster in json format and performed transformations(flattening of data, dropping and renaming columns) using pandas and then load the transformed data into snowflake. Also, I have combine the data for both location using pandas, scheduled the data load on a hourly basis using dagster and performed testing.

Task 1: Extract, transform and Load historic weather data for bangalore into snowflake.
Reference: Historic data/weather_blr.py

Task 2: Extract, transform and Load historic weather data for hyderabad into snowflake.
Reference: Historic data/weather_hyd.py

Task 3: Extract, transform and Load incremental weather data for bangalore into snowflake and schedule it on hourly basis.
Reference: Incremental/weather_blr.py

Task 4: Extract, transform and Load incremental weather data for hyderabad into snowflake and schedule it on hourly basis.
Reference: Incremental/weather_blr.py

Task 5: Combine the bangalore and hyderabad data and load it into snowflake.
Reference: weather.py

Task 6: Test cases for the above tasks.
Reference: test.py

