
import re
import json
import requests

from datetime import datetime

from sql_fns import query_stations


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
        store_weather_data(station_id, r.json())
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
