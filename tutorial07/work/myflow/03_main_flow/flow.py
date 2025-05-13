import requests
import pandas as pd
from datetime import datetime
import pytz
from prefect import flow, task

# Helper function to extract precipitation data
def get_precipitation(data):
    if 'rain' in data and '1h' in data['rain']:
        return data['rain']['1h']
    elif 'snow' in data and '1h' in data['snow']:
        return data['snow']['1h']
    else:
        return 0.0

@task
def get_weather_data(location_context={'location': None, 'province': None, 'lat': None, 'lon': None}):
    WEATHER_ENDPOINT = "https://api.openweathermap.org/data/2.5/weather"
    API_KEY = "70e208d9d8ba1534136297fb1f3fe396"  # ใส่ API key ของคุณ

    location = location_context['location']
    province = location_context['province']
    
    params = {
        "lat": location_context['lat'],
        "lon": location_context['lon'],
        "appid": API_KEY,
        "units": "metric"
    }
    
    try:
        response = requests.get(WEATHER_ENDPOINT, params=params)
        response.raise_for_status()
        data = response.json()
        
        dt = datetime.now()
        thai_tz = pytz.timezone('Asia/Bangkok')
        created_at = dt.replace(tzinfo=thai_tz)
        timestamp = datetime.now()
        
        weather_dict = {
            'timestamp': timestamp,
            'year': timestamp.year,
            'month': timestamp.month,
            'day': timestamp.day,
            'hour': timestamp.hour,
            'minute': timestamp.minute,
            'created_at': created_at,
            'province': province,
            'location_name': location,
            'api_location': data['name'],
            'weather_main': data['weather'][0]['main'],
            'weather_description': data['weather'][0]['description'],
            'main.temp': data['main']['temp'],
            'main.humidity': data['main']['humidity'],
            'wind.speed': data['wind']['speed'],
            'precipitation': get_precipitation(data)
        }
        
        return weather_dict
    
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching data for {location}: {e}")
        return None
    except KeyError as e:
        print(f"❌ Missing key in response for {location}: {e}")
        return None

@flow(name="main-flow", log_prints=True)
def main_flow(parameters={}):
    locations = {
        "Satitram Alumni": {"province": "Bangkok", "lat": 13.752916, "lon": 100.618616},
        "Ramkhamhaeng University": {"province": "Bangkok", "lat": 13.7552, "lon": 100.6201},
        "Rajamangala National Stadium": {"province": "Bangkok", "lat": 13.7627, "lon": 100.6200},
        "Ramkhamhaeng Hospital": {"province": "Bangkok", "lat": 13.7485, "lon": 100.6265},
        "Hua Mak Police Station": {"province": "Bangkok", "lat": 13.7500, "lon": 100.6200},
        "Bang Kapi District Office": {"province": "Bangkok", "lat": 13.7640, "lon": 100.6440},
        "The Mall Bangkapi": {"province": "Bangkok", "lat": 13.7650, "lon": 100.6430},
        "Tawanna Market": {"province": "Bangkok", "lat": 13.7655, "lon": 100.6425},
        "Big C Huamark": {"province": "Bangkok", "lat": 13.7445, "lon": 100.6205},
        "Makro Ladprao": {"province": "Bangkok", "lat": 13.7940, "lon": 100.6110},
        "Siam Paragon": {"province": "Bangkok", "lat": 13.7458, "lon": 100.5343},
        "ICONSIAM": {"province": "Bangkok", "lat": 13.7292, "lon": 100.5103},
        "HomePro Rama 9": {"province": "Bangkok", "lat": 13.7430, "lon": 100.6155},
        "The Mall Ramkhamhaeng": {"province": "Bangkok", "lat": 13.7526, "lon": 100.6095},
        "Healthy Park": {"province": "Bangkok", "lat": 13.7565, "lon": 100.6275}
    }

    # รวบรวมข้อมูล
    records = []
    for location in locations:
        result = get_weather_data({
            'location': location,
            'province': locations[location]['province'],
            'lat': locations[location]['lat'],
            'lon': locations[location]['lon'],
        })
        if result is not None:
            records.append(result)
        else:
            print(f"⚠️ Skipped {location} due to API error or missing data.")

    df = pd.DataFrame(records)
    print("📦 Data collected:", len(df), "records")

    if not df.empty:
        # lakeFS S3-compatible config
        ACCESS_KEY = "access_key"
        SECRET_KEY = "secret_key"
        lakefs_endpoint = "http://lakefs-dev:8000/"
        repo = "weather"
        branch = "main"
        path = "weather.parquet"

        lakefs_s3_path = f"s3a://{repo}/{branch}/{path}"

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
            partition_cols=['year', 'month', 'day', 'hour'],
        )
        print(f"✅ Data written to {lakefs_s3_path}")
    else:
        print("⚠️ No data to write to Parquet.")