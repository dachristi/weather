
import os
import re
import json
import shutil
import requests

from pathlib import Path
from datetime import datetime

#from sql_fns import query_stations
#from sql_fns import properties_query
from sql_fns import load_weather_data


def main():
    #property_ids = properties_query()
    files = ['weather_data/%s' % f for f in os.listdir('weather_data')
             if f.endswith('json')]
    src_file_directory = Path('weather_data')
    dst_file_directory = src_file_directory / 'processed'
    files = (p for p in src_file_directory.iterdir() if p.suffix == '.json')
    for f in files:
        parse_weather(f)
        shutil.move(str(f), str(dst_file_directory), copy_function='copy2')


def parse_weather(file_path):
    print("Parsing...%s" % file_path.name)
    #data = read_weather_data(data_file)
    data = json.loads(file_path.read_text())
    try:
        data_list = data['features']
    except:
        data_dict = data
    #for data_dict in data_list:
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
