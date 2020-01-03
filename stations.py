
import json
import requests
from sql_fns import sql_store_station_data


def main():
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


if __name__ == '__main__':
    main()
