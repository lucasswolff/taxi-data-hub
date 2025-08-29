import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from datetime import datetime

start_date = "2023-12-31"
end_date = datetime.now().replace(day=1).strftime("%Y-%m-%d")

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

url = "https://archive-api.open-meteo.com/v1/archive"

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

df_weather.to_parquet("raw_data/weather/weather_api.parquet", engine="pyarrow", index=False)