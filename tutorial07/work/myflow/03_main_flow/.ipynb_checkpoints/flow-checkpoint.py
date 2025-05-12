import requests
import pandas as pd
from datetime import datetime
import pytz
from prefect import flow, task # Prefect flow and task decorators

@task
def get_weather_data(province_context={'province':None, 'lat':None, 'lon':None}):
    # API endpoint and parameters
    WEATHER_ENDPOINT = "https://api.openweathermap.org/data/2.5/weather"
    API_KEY = "70e208d9d8ba1534136297fb1f3fe396"  # Replace with your actual API key
    province=province_context['province']
    
    params = {
        "lat": province_context['lat'],
        "lon": province_context['lon'],
        "appid": API_KEY,
        "units": "metric",
        "lang": "th"
    }
    try:
        # Make API request
        response = requests.get(WEATHER_ENDPOINT, params=params)
        response.raise_for_status()  # Raise an exception for bad status codes
        data = response.json()
        
        # Convert timestamp to datetime
        # created_at = datetime.fromtimestamp(data['dt'])

        dt = datetime.now()
        thai_tz = pytz.timezone('Asia/Bangkok')
        created_at = dt.replace(tzinfo=thai_tz)


        timestamp = datetime.now()
        location_name = province
        
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
        
        # Create DataFrame
        # df = pd.DataFrame([weather_dict])
        
        # return df
        return weather_dict

    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except KeyError as e:
        print(f"Error processing data: Missing key {e}")
        return None


@flow(name="main-flow", log_prints=True)
def main_flow(parameters={}):
    provinces = {
    "Satitram Alumni": {
        "lat": 13.752916,
        "lon": 100.618616
    },
    "Siam Paragon": {
        "lat": 13.746239,
        "lon": 100.534345
    },
    "MBK Center": {
        "lat": 13.746141,
        "lon": 100.530547
    },
    "Chulalongkorn University": {
        "lat": 13.746724,
        "lon": 100.530898
    },
    "Erawan Shrine": {
        "lat": 13.746656,
        "lon": 100.541333
    },
    "CentralWorld": {
        "lat": 13.746774,
        "lon": 100.539462
    },
    "Pratunam Market": {
        "lat": 13.749609,
        "lon": 100.539115
    },
    "Jim Thompson House": {
        "lat": 13.746603,
        "lon": 100.529531
    },
    "Bangkok Art and Culture Centre": {
        "lat": 13.746599,
        "lon": 100.531101
    },
    "Lumphini Park": {
        "lat": 13.727924,
        "lon": 100.542287
    },
    "Wat Arun": {
        "lat": 13.743682,
        "lon": 100.488146
    },
    "Wat Pho": {
        "lat": 13.746697,
        "lon": 100.493469
    },
    "Grand Palace": {
        "lat": 13.750046,
        "lon": 100.491346
    },
    "Asiatique The Riverfront": {
        "lat": 13.703379,
        "lon": 100.509145
    },
    "ICONSIAM": {
        "lat": 13.723621,
        "lon": 100.517333
    },
    "Khao San Road": {
        "lat": 13.749136,
        "lon": 100.495136
    },
    "Terminal 21": {
        "lat": 13.736657,
        "lon": 100.561172
    },
    "The Mall Bangkapi": {
        "lat": 13.767090,
        "lon": 100.640134
    },
    "Dusit Zoo": {
        "lat": 13.766410,
        "lon": 100.525019
    },
    "Sukhumvit Road": {
        "lat": 13.731731,
        "lon": 100.571259
    }
}
    
    df=pd.DataFrame([get_weather_data(
        {
            'province':province,
            'lat':provinces[province]['lat'],
            'lon':provinces[province]['lon'],
        }
    ) for province in list(provinces.keys())])
    
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
    df.to_parquet(
        lakefs_s3_path,
        storage_options=storage_options,
        partition_cols=['year','month','day','hour'],
    )