import openmeteo_requests
import pandas as pd
import requests_cache
from dotenv import load_dotenv
import os
from retry_requests import retry
from datetime import datetime
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col


def download_api_data(start_date, end_date, url):

	# Setup the Open-Meteo API client with cache and retry on error
	cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
	retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
	openmeteo = openmeteo_requests.Client(session = retry_session)

	params = {
		# Queens, Brooklyn, EWM, Bronx, Manhattan, Staten Island, NYC (used for Unknown)
		# N/A (Outside of NYC) will not be sourced - null when joined
		"latitude": [40.6815, 40.6501, 40.6971, 40.8499, 40.7834, 40.5623, 40.7143],
		"longitude": [-73.8365, -73.9496, -74.1756, -73.8664, -73.9663, -74.1399, -74.006],
		"start_date": start_date,
		"end_date": end_date,
		"hourly": ["temperature_2m", "precipitation", "rain", "snowfall"],
		"timezone": "America/New_York",
	}

	responses = openmeteo.weather_api(url, params=params)

	df_weather = None  
	borough_list = ['Queens', 'Brooklyn', 'EWM', 'Bronx', 'Manhattan', 'Staten Island', 'Unknown']

	for i, response in enumerate(responses):
		# Process hourly data. The order of variables needs to be the same as requested.
		hourly = response.Hourly()
		hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
		hourly_precipitation = hourly.Variables(1).ValuesAsNumpy()
		hourly_rain = hourly.Variables(2).ValuesAsNumpy()
		hourly_snowfall = hourly.Variables(3).ValuesAsNumpy()

		hourly_data = {"datetime": pd.date_range(
			start = pd.to_datetime(hourly.Time(), unit = "s"),
			end = pd.to_datetime(hourly.TimeEnd(), unit = "s"),
			freq = pd.Timedelta(seconds = hourly.Interval()),
			inclusive = "left"
		)}
		
		hourly_data["latitude"] = response.Latitude()
		hourly_data["longitude"] = response.Longitude()

		hourly_data["borough"] = borough_list[i]

		hourly_data["temperature_2m"] = hourly_temperature_2m
		hourly_data["precipitation"] = hourly_precipitation
		hourly_data["rain"] = hourly_rain
		hourly_data["snowfall"] = hourly_snowfall

		hourly_dataframe = pd.DataFrame(data = hourly_data)

		if df_weather is None:
			df_weather = hourly_dataframe.copy()
		else:
			df_weather = pd.concat([df_weather, hourly_dataframe], ignore_index=True)

	df_weather['api_inserted_datetime'] = datetime.now().replace(microsecond=0)

	return df_weather
	


def upload_snowflake(df_weather, table_name, connection_params):
    # Create Snowpark session
    session = Session.builder.configs(connection_params).create()
	
    try:
        snow_df_weather = session.create_dataframe(df_weather)

        # Write to Snowflake
        print('Writing to snowflake...')
        snow_df_weather.write.save_as_table(table_name, mode="overwrite")
    
    finally:
          session.close()

def main():
    url = "https://archive-api.open-meteo.com/v1/archive"
    table_name = 'weather_raw'

    start_date = '2023-12-31'
    end_date = datetime.now().replace(day=1).strftime("%Y-%m-%d")
    
    load_dotenv()
    connection_params = {
        'user': os.getenv("SNOWFLAKE_USER"),
        'password': os.getenv("SNOWFLAKE_PASSWORD"),
        'account': os.getenv("SNOWFLAKE_ACCOUNT"),
        'role': os.getenv("SNOWFLAKE_ROLE"),
        'warehouse': os.getenv("SNOWFLAKE_WAREHOUSE"),
        'database': os.getenv("SNOWFLAKE_DATABASE"),
        'schema': 'raw'
    }

    print(f'Downloading API weather data')
    df_weather = download_api_data(start_date, end_date, url)
    
    upload_snowflake(df_weather, table_name, connection_params)

    print(f'API weather data successfully uploaded to Snowflake')
               
if __name__ == '__main__':
    main()