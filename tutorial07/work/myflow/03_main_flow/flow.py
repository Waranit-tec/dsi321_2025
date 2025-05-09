import requests
import pandas as pd
from datetime import datetime
import pytz

# API endpoint and parameters
WEATHER_ENDPOINT = "https://api.openweathermap.org/data/2.5/weather"
API_KEY = "70e208d9d8ba1534136297fb1f3fe396"  # ← เปลี่ยนเป็นของคุณเอง

locations = {
    "Satitram Alumni": {"lat": 13.754174, "lon": 100.615676},
}

def get_weather_data(location_name='Satitram Alumni'):
    lat = locations[location_name]['lat']
    lon = locations[location_name]['lon']

    params = {
        "lat": lat,
        "lon": lon,
        "appid": API_KEY,
        "units": "metric",
        "lang": "th"
    }

    try:
        response = requests.get(WEATHER_ENDPOINT, params=params)
        response.raise_for_status()
        data = response.json()

        thai_tz = pytz.timezone('Asia/Bangkok')
        timestamp = datetime.now(thai_tz)
        created_at = datetime.fromtimestamp(data['dt'], tz=thai_tz)

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