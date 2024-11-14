import datetime
import json
import openmeteo_requests
import requests_cache
import sqlite3
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import streamlit as st
from streamlit import session_state
from svgpath2mpl import parse_path
from retry_requests import retry


def setup():
    if "current_location" not in st.session_state:
        st.session_state.current_location = "london"

    if "current_df" not in st.session_state:
        st.session_state.current_df = get_data_from_db("london")


def get_weather(location):
    # Set up the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/forecast"

    with open("jsons/coords.json") as coords_json:
        coords_list = json.load(coords_json)

    coords = {
        "latitude": coords_list[location][0],
        "longitude": coords_list[location][1],
    }

    # Prepare the parameters: coords + variables
    weather_types = {
        "hourly": ["temperature_2m", "precipitation_probability", "precipitation", "weather_code", "wind_speed_10m"],
        "timezone": "Europe/London"
    }
    params = coords | weather_types
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
    new_df.to_csv(f"weekly_{location}_forecast.csv", encoding='utf-8', index=False)


def process_df(dataframe):
    out_df = dataframe
    # Get dictionary of weather codes
    with open("jsons/weather_codes.json") as wc_json:
        weather_codes = json.load(wc_json)

    # Make a new column in the dataframe containing the weather string matching provided weather code in that row
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


def csv_to_db(location):
    conn = sqlite3.connect('weather_data.db')
    c = conn.cursor()

    c.execute(f'''
    DROP TABLE IF EXISTS weekly_{location};
    ''')

    c.execute(f'''
    CREATE TABLE IF NOT EXISTS weekly_{location}(
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
    weekly = pd.read_csv(f'weekly_{location}_forecast.csv')
    weekly.to_sql(f'weekly_{location}', conn, if_exists='append', index = False)

    c.close()


def check_date():
    # check today's date when app is run and refresh db if it is out of date
    st.session_state.todays_date = datetime.date.today()
    current_location = st.session_state.current_location
    df = pd.read_csv(f'weekly_{current_location}_forecast.csv', usecols=[0], skiprows=1, nrows=1, parse_dates=[0])
    start_date = df.iloc[0,0]

    if not start_date.strftime('%Y-%m-%d') == st.session_state.todays_date:
        get_weather(current_location)
        csv_to_db(current_location)

    # To look at - If a session runs over 1 day into another the cache maybe doesn't update
    # Solution maybe to pass today's date to the get data from db call, as different args may force
    # the function to run again


@st.cache_data
def get_data_from_db(location):
    # Get weekly data from database for location
    conn = sqlite3.connect('weather_data.db')
    query = f'SELECT * FROM weekly_{location};'
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def update_session(location):
    st.session_state.current_location = location
    get_weather(location)
    csv_to_db(location)     #Consider looking at if not exists
    st.session_state.current_df = get_data_from_db(location)


def make_markers(weather_code):
    # Match the weather code to the relevant svg marker name


    with open("jsons/marker_codes.json") as marker_codes_json:
        marker_codes = json.load(marker_codes_json)

    converted_markers = {int(key): value for key, value in marker_codes.items()}


    with open("jsons/marker_paths.json") as marker_paths_json:
        marker_paths = json.load(marker_paths_json)


    if weather_code in converted_markers:
        custom_marker = parse_path(marker_paths[converted_markers.get(weather_code)])
        custom_marker.vertices -= custom_marker.vertices.mean(axis=0)
        custom_marker = custom_marker.transformed(mpl.transforms.Affine2D().rotate_deg(180))
        custom_marker = custom_marker.transformed(mpl.transforms.Affine2D().scale(-1, 1))

    else:
        custom_marker = parse_path(marker_paths['generic'])
        custom_marker.vertices -= custom_marker.vertices.mean(axis=0)
        custom_marker = custom_marker.transformed(mpl.transforms.Affine2D().rotate_deg(180))
        custom_marker = custom_marker.transformed(mpl.transforms.Affine2D().scale(-1, 1))

    return custom_marker


def graph_it(day):
    df = st.session_state.current_df

    # Get the hours and minutes from the date column
    df['date'] = pd.to_datetime(df['date'])
    df['hour'] = df['date'].dt.time.astype(str)
    df['hour'] = df['hour'].str[:-3]

    # Make a df with only the selected day's values
    today = day[0:2]
    df['day'] = df['date'].dt.day
    day_int = int(today)
    todays_df = df[df['day'] == day_int]
    todays_df.reset_index(drop=True, inplace=True)

    # Plot a bar chart of wind speed
    fig, ax1 = plt.subplots(figsize=(14,6))
    ax1.set_ylabel("Wind speed", color='cyan')
    ax1.bar(todays_df['hour'], todays_df['wind_speed_10m'], color='cyan', alpha=0.5)

    # Plot a line graph of temperature over the wind speed bars, with custom markers showing weather
    ax2 = ax1.twinx()
    ax2.set_ylabel("Temperature")

    # Get unique weather codes
    weather_set = set(todays_df['weather_code'])

    for weather in weather_set:
        indices = todays_df.index[todays_df['weather_code'] == weather].tolist()
        custom_marker = make_markers(weather)
        ax2.plot(todays_df['hour'], todays_df['temperature_2m'], color='orange', marker=custom_marker, markevery=indices, markersize=25, alpha=0.7)

    return fig


def get_unique_dates(df):
    df['date'] = pd.to_datetime(df['date'])
    df['days_months'] = df['date'].dt.strftime('%d/%m')
    dates_list = df['days_months'].unique()
    return dates_list


def display_it():
    # What streamlit will display
    st.title('Weather forecast for the week')
    st.subheader(st.session_state.current_location.title())

    locations = ['london', 'tignes', 'whistler', 'bologna', 'vienna']
    # side panel with key locations
    with st.sidebar:
        for location in locations:
            st.button(location.title(), on_click=update_session, args=(location,))

    dates_list = get_unique_dates(get_data_from_db(st.session_state.current_location)).tolist()
    tabs = st.tabs(dates_list)


    for tab, day in zip(tabs, dates_list):
        with tab:
            st.subheader(f'This is the date: {day}')
            st.pyplot(graph_it(day))


            # look in to multiple page app


if __name__ == '__main__':
    setup()
    update_session(st.session_state.current_location)
    check_date()
    display_it()



