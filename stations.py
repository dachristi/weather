#!/home/pc/Envs/weather/bin/python
# -*- coding: utf-8 -*-

import argparse
import requests
import json
import re

from datetime import datetime

from sql_fns import query_property_coordinates
from sql_fns import store_station_data
from sql_fns import store_weather_data
from sql_fns import query_property_ids
from sql_fns import query_station_ids


class WeatherStations(object):
    '''
    Given coordinates, retreive the list of stations and store them in a table.
    '''

    def __init__(self, property_id):
        self.property_id = property_id
        self.latitude, self.longitude = query_property_coordinates(self.property_id)

    def get_stations(self):
        url = ('https://api.weather.gov/points/%s,%s/stations'
                % (self.latitude, self.longitude))
        r = requests.get(url)
        station_count = self.parse_station_data(r.text)
        return station_count

    def parse_station_data(self, r_content):
        station_count = 0
        data = json.loads(r_content)
        for station_data in data['features']:
            try:
                station_longitude, station_latitude  = station_data['geometry']['coordinates']
                station_id = station_data['properties']['stationIdentifier']
                store_station_data(self.property_id, round(station_latitude, 7), round(station_longitude, 7), station_id)
                station_count += 1
            except:
                print(station_data)
                continue
        return station_count


class WeatherApi(object):
    def __init__(self, weather_station_id):
        self.weather_station_id = weather_station_id

    def get_latest_weather_conditions(self):
        url = 'https://api.weather.gov/stations/%s/observations/latest' % self.weather_station_id
        r = requests.get(url)
        self.parse_weather_data(r.text)
        return None

    def parse_weather_data(self, r_content):
        try:
            data = json.loads(r_content)
        except:
            print('Bad API response')
            with open('tmp/bad_api_response.txt', 'w') as f:
                f.write(r_content)
            return None
        try:
            timestamp_raw = data['properties']['timestamp']
            if timestamp_raw is None:
                return None
            timestamp = re.sub(r'T', ' ', timestamp_raw).split('+')[0]
            if timestamp is None:
                return None
            temperature_celsius = data['properties']['temperature']['value']
            if temperature_celsius is None:
                return None
            temperature = 9/5 * temperature_celsius + 32
            relative_humidity = data['properties']['relativeHumidity']['value']
            if relative_humidity is None:
                return None
            weather_icon = data['properties']['icon']
            if weather_icon is None:
                return None
            if weather_icon == 'https://api.weather.gov/icons/land/day/wind_?size=medium':
                return None
        except:
            return None
        store_weather_data(self.weather_station_id, timestamp, temperature, relative_humidity, weather_icon)
        return None


def initialize_stations_table():
    station_file_path = 'station_data.json'
    stations_data = read_station_data(station_file_path)
    store_station_data(stations_data)


def store_station_data(stations_data):
    '''Store station data in the stations table.'''
    data_list = []
    for station in stations_data:
        station_id = station['properties']['stationIdentifier']
        coordinate_1 = station['geometry']['coordinates'][0]
        coordinate_2 = station['geometry']['coordinates'][1]
        elevation = station['properties']['elevation']['value']
        elevation_units = station['properties']['elevation']['unitCode'].split(':')[1]
        url = station['id']
        tz = station['properties']['timeZone']
        station_name = station['properties']['name']

        data_list.append((station_id, coordinate_1, coordinate_2, elevation,
                          elevation_units, url, tz, station_name))
    sql_store_station_data(data_list)


def read_station_data(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    stations = data['features']
    return stations


def stations_api():
    '''Pull the data and store it in a file'''

    url = 'https://api.weather.gov/stations'
    stations_text = requests.get(url)
    stations_json = stations_text.json()
    with open('station_data.json', 'w') as f:
        json.dump(stations_json, f)
    return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--init_stations', action='store_true', help='Initialize stations table')
    args = parser.parse_args()
    if args.init_stations:
        property_ids =
        WeatherStations
        print('Done')
        return None
    station_ids = query_station_ids()
    station_count = 0
    for station_id in station_ids:
        weather = WeatherApi(station_id)
        weather.get_latest_weather_conditions()
        station_count += 1
    print(str(datetime.now()).split('.')[0], "Gather data for %d weather stations." % station_count)
    return None


if __name__ == '__main__':
    main()





'''
CREATE TABLE weather_data
(
id INT(5) AUTO_INCREMENT NOT NULL,
station_id VARCHAR(4),
ts TIMESTAMP,
temperature DECIMAL(4,1),
relative_humidity DECIMAL(4,1),
weather_icon VARCHAR(100),
PRIMARY KEY (id),
UNIQUE KEY (station_id, ts)
)
;


CREATE TABLE weather_stations
(
id INT(5) AUTO_INCREMENT NOT NULL,
property_id INT(3),
latitude DECIMAL(7,6),
longitude DECIMAL(7,6),
station_id VARCHAR(4),

PRIMARY KEY (id),
UNIQUE KEY (property_id, station_id)
)
;
'''
