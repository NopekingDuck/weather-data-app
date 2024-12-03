import datetime
import json
from urllib.parse import urlencode
import urllib3
from urllib3.util.retry import Retry
from urllib3.util import Timeout
import sqlite3
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import streamlit as st
from svgpath2mpl import parse_path


def setup():
    if "current_location" not in st.session_state:
        st.session_state.current_location = "london"

    if "todays_date" not in st.session_state:
        st.session_state.todays_date = datetime.date.today()

    if "current_df" not in st.session_state:
        st.session_state.current_df = get_data_from_db(
            "london", st.session_state.todays_date
        )


def prepare_coordinates(location):
    with open("jsons/coords.json") as coords_json:
        coords_list = json.load(coords_json)

    coords = {
        "latitude": coords_list[location][0],
        "longitude": coords_list[location][1],
    }
    return coords


def make_url(coords):
    url = "https://api.open-meteo.com/v1/forecasts"
    weather_types = {
        "hourly": [
            "temperature_2m",
            "precipitation_probability",
            "precipitation",
            "weather_code",
            "wind_speed_10m",
        ],
        "timezone": "Europe/London",
    }
    params = coords | weather_types
    encoded_params = urlencode(params, doseq=True)
    full_url = url + "?" + encoded_params
    return full_url


@st.cache_data
def get_data_from_api(url):
    retries = Retry(
        total=5,
        backoff_factor=0.2,
        status_forcelist=[500, 502, 503, 504]
    )

    http = urllib3.PoolManager(retries=retries)

    try:
        response = http.request("GET", url, timeout=Timeout(connect=1.0, read=2.0))
        if response.status >= 400:
            st.write(f"Error connecting to api: HTTP error status code: {response.status}")
        else:
            print("Request successful")
            data = response.data
            values = json.loads(data)
            return values
    except urllib3.exceptions.MaxRetryError as e:
        st.write(f"Max retries exceeded with url: {e.reason}")
    except urllib3.exceptions.TimeoutError as e:
        st.write(f"Request timed out: {e}")
    except Exception as e:
        st.write(f"An unexpected error occurred: {e}")


def response_to_pandas(response):
    hourly = response["hourly"]
    hourly_time = hourly["time"]
    hourly_temperature_2m = hourly["temperature_2m"]
    hourly_precipitation_probability = hourly["precipitation_probability"]
    hourly_precipitation = hourly["precipitation"]
    hourly_weather_code = hourly["weather_code"]
    hourly_wind_speed_10m = hourly["wind_speed_10m"]

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly_time[0]),
            end=pd.to_datetime(hourly_time[len(hourly_time) - 1]),
            freq="h",
        ),
        "temperature_2m": hourly_temperature_2m,
        "precipitation_probability": hourly_precipitation_probability,
        "precipitation": hourly_precipitation,
        "weather_code": hourly_weather_code,
        "wind_speed_10m": hourly_wind_speed_10m,
    }

    hourly_dataframe = pd.DataFrame(data=hourly_data)

    return hourly_dataframe


def process_df(dataframe):
    out_df = dataframe
    # Get dictionary of weather codes
    with open("jsons/weather_codes.json") as wc_json:
        weather_codes = json.load(wc_json)

    # Make a new column in the dataframe containing the weather string matching provided weather code in that row
    weather = []
    for value in dataframe["weather_code"]:
        x = str(int(value))
        weather.append(weather_codes[x])
    out_df["weather"] = weather

    # Round temperature to 1 digit and convert to int
    out_df["temperature_2m"] = dataframe["temperature_2m"].round(1)
    out_df["temperature_2m"] = out_df["temperature_2m"].astype(int)

    # Round windspeed to 1 digit and convert to int
    out_df["wind_speed_10m"] = dataframe["wind_speed_10m"].round(1)
    out_df["wind_speed_10m"] = out_df["wind_speed_10m"].astype(int)

    # Round precipitation to 1 digit
    out_df["precipitation"] = dataframe["precipitation"].round(1)

    return out_df


def csv_to_db(location):
    conn = sqlite3.connect("weather_data.db")
    c = conn.cursor()

    c.execute(
        f"""
    DROP TABLE IF EXISTS weekly_{location};
    """
    )

    c.execute(
        f"""
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
    """
    )

    # Set name of your CSV here that you want to add to db
    weekly = pd.read_csv(f"weekly_{location}_forecast.csv")
    weekly.to_sql(f"weekly_{location}", conn, if_exists="append", index=False)

    c.close()


def check_date():
    # check today's date when app is run and refresh db if it is out of date
    current_location = st.session_state.current_location
    df = pd.read_csv(
        f"weekly_{current_location}_forecast.csv",
        usecols=[0],
        skiprows=1,
        nrows=1,
        parse_dates=[0],
    )
    start_date = df.iloc[0, 0]

    if not start_date.strftime("%Y-%m-%d") == st.session_state.todays_date:
        update_session(current_location)


@st.cache_data
def get_data_from_db(location, date):
    # Get weekly data from database for location
    conn = sqlite3.connect("weather_data.db")
    query = f"SELECT * FROM weekly_{location};"
    df = pd.read_sql_query(query, conn)
    conn.close()
    print("Cache updated")
    return df


def update_session(location):
    st.session_state.current_location = location
    coords = prepare_coordinates(location)
    url = make_url(coords)
    data = get_data_from_api(url)
    df = response_to_pandas(data)
    processed_df = process_df(df)
    processed_df.to_csv(
        f"weekly_{location}_forecast.csv", encoding="utf-8", index=False
    )
    csv_to_db(location)
    st.session_state.current_df = get_data_from_db(
        location, st.session_state.todays_date
    )
    ### Either check the api data is already in csv form or find another way to cache to reduce api calls


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
        custom_marker = custom_marker.transformed(
            mpl.transforms.Affine2D().rotate_deg(180)
        )
        custom_marker = custom_marker.transformed(
            mpl.transforms.Affine2D().scale(-1, 1)
        )

    else:
        custom_marker = parse_path(marker_paths["generic"])
        custom_marker.vertices -= custom_marker.vertices.mean(axis=0)
        custom_marker = custom_marker.transformed(
            mpl.transforms.Affine2D().rotate_deg(180)
        )
        custom_marker = custom_marker.transformed(
            mpl.transforms.Affine2D().scale(-1, 1)
        )

    return custom_marker


def graph_it(day):
    df = st.session_state.current_df

    # Get the hours and minutes from the date column
    df["date"] = pd.to_datetime(df["date"])
    df["hour"] = df["date"].dt.time.astype(str)
    df["hour"] = df["hour"].str[:-3]

    # Make a df with only the selected day's values
    today = day[0:2]
    df["day"] = df["date"].dt.day
    day_int = int(today)
    todays_df = df[df["day"] == day_int]
    todays_df.reset_index(drop=True, inplace=True)

    # Plot a bar chart of wind speed
    fig, ax1 = plt.subplots(figsize=(14, 6))
    ax1.set_ylabel("Wind speed", color="cyan")
    ax1.bar(todays_df["hour"], todays_df["wind_speed_10m"], color="cyan", alpha=0.5)

    # Plot a line graph of temperature over the wind speed bars, with custom markers showing weather
    ax2 = ax1.twinx()
    ax2.set_ylabel("Temperature")

    # Get unique weather codes
    weather_set = set(todays_df["weather_code"])

    for weather in weather_set:
        indices = todays_df.index[todays_df["weather_code"] == weather].tolist()
        custom_marker = make_markers(weather)
        ax2.plot(
            todays_df["hour"],
            todays_df["temperature_2m"],
            color="orange",
            marker=custom_marker,
            markevery=indices,
            markersize=25,
            alpha=0.7,
        )

    return fig


def get_unique_dates(df):
    df["date"] = pd.to_datetime(df["date"])
    df["days_months"] = df["date"].dt.strftime("%d/%m")
    dates_list = df["days_months"].unique()
    return dates_list


def display_it():
    # What streamlit will display
    st.title("Weather forecast for the week")
    st.subheader(st.session_state.current_location.title())

    locations = ["london", "tignes", "whistler", "bologna", "vienna"]
    # side panel with key locations
    with st.sidebar:
        for location in locations:
            st.button(location.title(), on_click=update_session, args=(location,))

    # Create 1 tab for each day of data
    dates_list = get_unique_dates(
        get_data_from_db(
            st.session_state.current_location, st.session_state.todays_date
        )
    ).tolist()
    tabs = st.tabs(dates_list)

    for tab, day in zip(tabs, dates_list):
        with tab:
            st.subheader(f"This is the date: {day}")
            st.pyplot(graph_it(day))


if __name__ == "__main__":
    setup()
    update_session(st.session_state.current_location)
    check_date()
    display_it()
