#!/home/Envs/weather/bin/python3

import os
import re
import json
import requests

from pathlib import Path
from datetime import datetime

from weather import WeatherData
from sql_fns import load_weather_data


def main():
    files = WeatherData.read_files()
    for f in files:
        try:
            parse_weather(f)
        except:
            WeatherData.move_error_file(f)
            continue
        WeatherData.move_processed_file(f)


def parse_weather(file_path):
    print("Parsing...%s" % file_path.name)
    data = json.loads(file_path.read_text())
    try:
        data_list = data['features']  # multiple weather histories were called
    except:
        data_list = [data]  # single weather history was called
    for data_dict in data_list:
        properties = data_dict['properties']
        station_id = properties['station'].split('/')[-1]
        text_description = properties['textDescription'][:50]
        timestamp = re.sub(r'T', ' ', properties['timestamp'].split('+')[0])
        temperature = properties['temperature']['value']
        temperature_unit = properties['temperature']['unitCode'].split(':')[-1]
        dewpoint = properties['dewpoint']['value']
        dewpoint_unit = properties['dewpoint']['unitCode'].split(':')[-1]
        wind_direction = properties['windDirection']['value']
        wind_direction_unit = properties['windDirection']['unitCode'].split(':')[-1]
        wind_speed = properties['windSpeed']['value']
        wind_speed_unit = properties['windSpeed']['unitCode'].split(':')[-1]
        wind_gust = properties['windGust']['value']
        wind_gust_unit = properties['windGust']['unitCode'].split(':')[-1]
        barometric_pressure = properties['barometricPressure']['value']
        barometric_pressure_unit = properties['barometricPressure']['unitCode'].split(':')[-1]
        sea_level_pressure = properties['seaLevelPressure']['value']
        sea_level_pressure_unit = properties['seaLevelPressure']['unitCode'].split(':')[-1]
        visibility = properties['visibility']['value']
        visibility_unit = properties['visibility']['unitCode'].split(':')[-1]
        precipitation_last_hour = properties['precipitationLastHour']['value']
        precipitation_last_hour_unit = properties['precipitationLastHour']['unitCode'].split(':')[-1]
        relative_humidity = properties['relativeHumidity']['value']
        relative_humidity_unit = properties['relativeHumidity']['unitCode'].split(':')[-1]
        wind_chill = properties['windChill']['value']
        wind_chill_unit = properties['windChill']['unitCode'].split(':')[-1]
        heat_index = properties['heatIndex']['value']
        heat_index_unit = handle_none(properties['heatIndex']['unitCode'])

    conditions = (station_id,
                  timestamp,
                  text_description,
                  temperature,
                  temperature_unit,
                  dewpoint,
                  dewpoint_unit,
                  wind_direction,
                  wind_direction_unit,
                  wind_speed,
                  wind_speed_unit,
                  wind_gust,
                  wind_gust_unit,
                  barometric_pressure,
                  barometric_pressure_unit,
                  visibility,
                  visibility_unit,
                  precipitation_last_hour,
                  precipitation_last_hour_unit,
                  relative_humidity,
                  relative_humidity_unit,
                  wind_chill,
                  wind_chill_unit,
                  heat_index,
                  heat_index_unit)
    load_weather_data(conditions)
    return None


def handle_none(value):
    try:
        new_value = value.split(':')[-1]
    except:
        new_value = value
    return new_value


def read_weather_data(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data


if __name__ == '__main__':
    main()
