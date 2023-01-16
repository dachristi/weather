
import json
import requests

from sql_fns import sql_store_station_data
from sql_fns import query_station_data
from sql_fns import query_property_data
from sql_fns import store_nearby_stations

from haversine import distance


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
    url = 'https://api.weather.gov/stations'
    r_stations = requests.get(url)
    stations_json = r_stations.json()
    with open('station_data.json', 'w') as f:
        json.dump(stations_json, f)
    return None


def nearby_stations(radius=50):
    '''
    Select stations that are within proximity to location of interest.
    '''
    stations = query_station_data()
    properties = query_property_data()
    station_count = 0
    for p in properties:
        property_latitude = p['latitude']
        property_longitude = p['longitude']
        property_id = p['id']
        for station in stations:
            station_latitude = station['latitude']
            station_longitude = station['longitude']
            station_id = station['station_id']
            d = distance(station_latitude,
                         station_longitude,
                         property_latitude,
                         property_longitude)
            if d <= radius:
                store_nearby_stations(property_id, station_id, d)
                station_count += 1
            else:
                continue
    print('Loaded %d stations into table.' % station_count)
    return None


if __name__ == '__main__':
    main()
