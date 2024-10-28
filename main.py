import json
import openmeteo_requests
import requests_cache
import sqlite3
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import streamlit as st
from datetime import date
from svgpath2mpl import parse_path
from retry_requests import retry



def get_weather():
    # Set up the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/forecast"

    # Prepare the parameters: coords + variables + date
    params = {
        "latitude": 51.5085,
        "longitude": -0.1257,
        "hourly": ["temperature_2m", "precipitation_probability", "precipitation", "weather_code", "wind_speed_10m"],
        "timezone": "Europe/London"
    }
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_precipitation_probability = hourly.Variables(1).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(2).ValuesAsNumpy()
    hourly_weather_code = hourly.Variables(3).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(4).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    ), "temperature_2m": hourly_temperature_2m, "precipitation_probability": hourly_precipitation_probability,
        "precipitation": hourly_precipitation, "weather_code": hourly_weather_code,
        "wind_speed_10m": hourly_wind_speed_10m}

    hourly_dataframe = pd.DataFrame(data=hourly_data)
    new_df = process_df(hourly_dataframe)
    new_df.to_csv(f"{date.today().strftime("%d-%m-%Y")}_7_day_forecast.csv", encoding='utf-8', index=False)


def process_df(dataframe):
    out_df = dataframe
    # get dictionary of weather codes
    with open("jsons/weather_codes.json") as wc_json:
        weather_codes = json.load(wc_json)

    # make a new column in the dataframe containing the weather string matching provided weather code in that row
    weather = []
    for value in dataframe['weather_code']:
        x = str(int(value))
        weather.append(weather_codes[x])
    out_df['weather'] = weather

    # Round temperature to 1 digit and convert to int
    out_df['temperature_2m'] = dataframe['temperature_2m'].round(1)
    out_df['temperature_2m'] = out_df['temperature_2m'].astype(int)

    # Round windspeed to 1 digit and convert to int
    out_df['wind_speed_10m'] = dataframe['wind_speed_10m'].round(1)
    out_df['wind_speed_10m'] = out_df['wind_speed_10m'].astype(int)

    # Round precipitation to 1 digit
    out_df['precipitation'] = dataframe['precipitation'].round(1)

    return out_df


def csv_to_db():
    conn = sqlite3.connect('weather_data.db')
    c = conn.cursor()

    c.execute('''
    DROP TABLE IF EXISTS weekly;
    ''')

    c.execute('''
    CREATE TABLE IF NOT EXISTS weekly(
    id INTEGER NOT NULL PRIMARY KEY,
    date text,
    temperature_2m INTEGER,
    precipitation_probability INTEGER,
    precipitation DECIMAL,
    wind_speed_10m INTEGER,
    weather_code INTEGER,
    weather text
    )    
    ''')

    # Set name of your CSV here that you want to add to db
    weekly = pd.read_csv('28-10-2024_7_day_forecast.csv')
    weekly.to_sql('weekly', conn, if_exists='append', index = False)

    c.close()


def make_markers(weather_code):
    # Match the weather code to the relevant svg marker name
    marker_codes = {
        0: 'clear',
        1: 'partly_cloudy',
        2: 'partly_cloudy',
        3: 'cloudy',
        45: 'cloudy',
        51: 'rain',
        53: 'rain',
        55: 'rain',
        56: 'rain',
        57: 'rain',
        61: 'rain',
        63: 'rain',
        65: 'rain',
        66: 'hail',
        67: 'hail',
        71: 'snow',
        73: 'snow',
        75: 'snow',
        77: 'snow',
        80: 'showers',
        81: 'showers',
        82: 'showers',
        95: 'storms',
        96: 'storms',
        99: 'storms'
    }


    with open("jsons/marker_paths.json") as marker_paths_json:
        marker_paths = json.load(marker_paths_json)


    if weather_code in marker_codes:
        custom_marker = parse_path(marker_paths[marker_codes.get(weather_code)])
        custom_marker.vertices -= custom_marker.vertices.mean(axis=0)
        custom_marker = custom_marker.transformed(mpl.transforms.Affine2D().rotate_deg(180))
        custom_marker = custom_marker.transformed(mpl.transforms.Affine2D().scale(-1, 1))

    else:
        custom_marker = parse_path(marker_paths['generic'])
        custom_marker.vertices -= custom_marker.vertices.mean(axis=0)
        custom_marker = custom_marker.transformed(mpl.transforms.Affine2D().rotate_deg(180))
        custom_marker = custom_marker.transformed(mpl.transforms.Affine2D().scale(-1, 1))

    return custom_marker


def graph_it():

    # Get 1 day hourly data from database
    conn = sqlite3.connect('weather_data.db')
    query = 'SELECT * FROM weekly LIMIT 24 OFFSET 1;'
    df = pd.read_sql_query(query, conn)

    # Get the hours and minutes from the date column
    df['date'] = pd.to_datetime(df['date'])
    df['hour'] = df['date'].dt.time.astype(str)
    df['hour'] = df['hour'].str[:-3]

    # Plot a bar chart of wind speed
    fig, ax1 = plt.subplots(figsize=(14,6))
    ax1.set_ylabel("Wind speed")
    ax1.bar(df['hour'], df['wind_speed_10m'], color='cyan', alpha=0.5)

    # Plot a line graph of temperature over the wind speed bars, with custom markers showing weather
    ax2 = ax1.twinx()
    ax2.set_ylabel("Temperature")

    # Get unique weather codes
    weather_set = set(df['weather_code'])

    for weather in weather_set:
        indices = df.index[df['weather_code'] == weather].tolist()
        custom_marker = make_markers(weather)
        ax2.plot(df['hour'], df['temperature_2m'], marker=custom_marker, markevery=indices, markersize=20)

    # Set the title of the graph
    df['days_months'] = df['date'].dt.strftime('%d/%m')
    day = df['days_months'].unique()
    plt.suptitle(f"Weather, temperature and windspeed for {day[0]}")

    fig # streamlit will draw this.



if __name__ == '__main__':
    # get_weather()
    # csv_to_db()
    graph_it()



