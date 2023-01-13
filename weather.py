#!/home/Envs/weather/bin/python3


import re
import json
import shutil
import requests

from pathlib import Path
from datetime import datetime

from sql_fns import query_stations


class WeatherApi(object):
    '''Class for file objects containing API data'''
    url_latest = 'https://api.weather.gov/stations/%s/observations/latest'
    url_history = 'https://api.weather.gov/stations/%s/observations'

    def __init__(self, station_id):
        self.station_id = station_id


class WeatherData(object):
    '''Class for file objects containing API data'''
    new_file_directory = Path(__file__).parent.resolve() / 'data'
    processed_file_directory = new_file_directory / 'processed'
    error_file_directory = new_file_directory / 'error'

    def __init__(self, station_id):
        self.station_id = station_id

        # Create data directories
        new_file_directory.mkdir(exist_ok=True)
        processed_file_directory.mkdir(exist_ok=True)
        error_file_directory.mkdir(exist_ok=True)

    def store_weather_data(self, json_data):
        dt = re.sub(r'[\:\-\s]+', '_', str(datetime.now())).split('.')[0]
        file_path = (str(WeatherData.new_file_directory / '%s_%s.json')
                        % (dt, self.station_id))
        with open(file_path, 'w') as f:
            json.dump(json_data, f)
        return None

    def read_files():
        files = (p for p in WeatherData.new_file_directory.iterdir()
                 if p.suffix == '.json')
        return files

    def move_processed_file(file_path):
        shutil.move(str(file_path), str(WeatherData.processed_file_directory),
            copy_function='copy2')
        return None

    def move_error_file(file_path):
        shutil.move(str(file_path), str(WeatherData.error_file_directory),
            copy_function='copy2')
        return None


def main():
    station_id_list = query_stations()
    weather_api_call(station_id_list)


def weather_api_call(station_id_list):
    '''Loop through each station_id in the list and call the weather API URL'''
    url_template = 'https://api.weather.gov/stations/%s/observations/latest'
    for station_id in station_id_list:
        url = url_template % station_id
        dt = str(datetime.now()).split('.')[0]
        print('Processing...%s %s' % (url, dt))
        r = requests.get(url)
        weather_data = WeatherData(station_id)
        weather_data.store_weather_data(r.json())
        #store_weather_data(station_id, r.json())
    return None


def store_weather_data(station_id, json_data):
    dt = re.sub(r'[\:\-\s]+', '_', str(datetime.now())).split('.')[0]
    file_path = 'weather_data/%s_%s.json' % (dt, station_id)
    with open(file_path, 'w') as f:
        json.dump(json_data, f)
    return None


def weighted_average_distance(stations):
    '''stations parameter is a dictionary {'station_id': 'distance'}.
    This function returns a new dictionary of wieghts {station_id: wieght}'''
    sum_of_distances = sum(stations.values())
    for key in stations.keys():
        weight = 1 - stations[key] / sum_of_distances
        stations[key] = weight
    return stations


if __name__ == '__main__':
    main()
