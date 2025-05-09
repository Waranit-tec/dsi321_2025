import requests
import pandas as pd
from datetime import datetime
import pytz
from prefect import flow, task  # Prefect flow and task decorators
import fsspec

# API endpoint and parameters
WEATHER_ENDPOINT = "https://api.openweathermap.org/data/2.5/weather"
API_KEY = "70e208d9d8ba1534136297fb1f3fe396"  # Replace with your actual API key

# Define location (using only Satitram Alumni's coordinates)
locations = {
    "Satitram Alumni": {"lat": 13.754174, "lon": 100.615676},
}

# Fetch weather data for the specified location
@task
def get_weather_data(location_name='Satitram Alumni'):
    lat = locations[location_name]['lat']
    lon = locations[location_name]['lon']

    params = {
        "lat": lat,
        "lon": lon,
        "appid": API_KEY,
        "units": "metric",
        "lang": "th"  # Thai language for weather description
    }

    try:
        # Make API request
        response = requests.get(WEATHER_ENDPOINT, params=params)
        response.raise_for_status()  # Raise an exception for bad status codes
        data = response.json()

        # Convert timestamp to Bangkok time
        thai_tz = pytz.timezone('Asia/Bangkok')
        timestamp = datetime.now(thai_tz)
        created_at = datetime.fromtimestamp(data['dt'], tz=thai_tz)

        # Create dictionary with required fields
        weather_dict = {
            'timestamp': timestamp,
            'year': timestamp.year,
            'month': timestamp.month,
            'day': timestamp.day,
            'hour': timestamp.hour,
            'minute': timestamp.minute,
            'created_at': created_at,
            'location': location_name,
            'temperature': data['main']['temp'],
            'feels_like': data['main']['feels_like'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'wind_speed': data['wind']['speed'],
            'visibility': data.get('visibility'),
            'weather_main': data['weather'][0]['main'],
            'weather_description': data['weather'][0]['description']
        }

        return weather_dict

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except KeyError as e:
        print(f"Error processing data: Missing key {e}")
        return None


# Main flow to orchestrate the weather data collection and storage
@flow(name="main-flow", log_prints=True)
def main_flow():
    # Use only 'Satitram Alumni' location
    location_name = "Satitram Alumni"

    # Get weather data for 'Satitram Alumni' and create a DataFrame
    weather_data = get_weather_data(location_name)

    # Convert the dictionary to a DataFrame
    df = pd.DataFrame([weather_data])

    # lakeFS credentials from your docker-compose.yml
    ACCESS_KEY = "access_key"
    SECRET_KEY = "secret_key"

    # lakeFS endpoint (running locally)
    lakefs_endpoint = "http://lakefs-dev:8000/"

    # lakeFS repository, branch, and file path
    repo = "weather"
    branch = "main"
    path = "weather.parquet"

    # Construct the full lakeFS S3-compatible path
    lakefs_s3_path = f"s3a://{repo}/{branch}/{path}"

    # Configure storage_options for lakeFS (S3-compatible)
    storage_options = {
        "key": ACCESS_KEY,
        "secret": SECRET_KEY,
        "client_kwargs": {
            "endpoint_url": lakefs_endpoint
        }
    }

    # Save DataFrame as Parquet file to lakeFS using fsspec
    fs = fsspec.filesystem("s3", **storage_options)

    df.to_parquet(
        lakefs_s3_path,
        storage_options=storage_options,
        partition_cols=['year', 'month', 'day', 'hour'],
        engine='pyarrow',  # Ensure you're using 'pyarrow' for parquet engine
        filesystem=fs
    )