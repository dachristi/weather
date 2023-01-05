#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from pathlib import Path
from datetime import datetime

import mysql.connector


class MySQL(object):
    working_dir = Path('.')
    #config_file = '/Users/dc/Documents/projects/weather/config.json'
    config_file = working_dir / 'config.json'
    with open(config_file, 'r') as f:
        config = json.load(f)

    def __init__(self, cmd):
        self.cmd = cmd

        self.cnx = mysql.connector.connect(user=MySQL.config['mysql']['user'],
                                   password=MySQL.config['mysql']['password'],
                                   host=MySQL.config['mysql']['host'],
                                   database=MySQL.config['mysql']['database'])
        self.cursor = self.cnx.cursor(dictionary=True, buffered=True)

    def query(self, *args):
        if args:
            self.cursor.execute(self.cmd, args)
            results = self.cursor.fetchall()
        else:
            self.cursor.execute(self.cmd)
            results = self.cursor.fetchall()
        return results

    def insert(self, args):
        if args:
            self.cursor.execute(self.cmd, args)
        else:
            self.cursor.execute(self.cmd)
        return None


def sql_store_station_data(data_list):
    '''Insert select station data from the json file'''

    cmd = '''
            INSERT INTO stations
            (station_id, degrees_east, degrees_north, elevation,
                    elevation_unit, url, tz, name)
            VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s);
            '''

    sql = MySQL(cmd)
    for data in data_list:
        sql.insert(data)
    sql.cursor.close()
    sql.cnx.commit()
    sql.cnx.close()


def load_weather_data(data):

    cmd = '''INSERT INTO weather_conditions_raw
            (station_id,
             ts,
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
            VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s)
            ;
            '''
    sql = MySQL(cmd)
    results = sql.insert(data)
    sql.cursor.close()
    sql.cnx.commit()
    sql.cnx.close()


def query_stations():
    '''query the set of stations in the nearby_stations table and return
        the station_ids'''

    cmd = '''
            SELECT DISTINCT station_id
            FROM nearby_stations
            WHERE disabled = 0
            ;
            '''
    sql = MySQL(cmd)
    results = sql.query()
    sql.cursor.close()
    sql.cnx.commit()
    sql.cnx.close()
    station_id_list = [item['station_id'] for item in results]
    return station_id_list


def stations_call(property_id):
    '''property_id is needed in the event a station is represented by
       multiple properties'''

    cmd = '''
            SELECT station_id, distance
            FROM nearby_stations
            WHERE property_id = %s
            ;
            '''

    sql = MySQL(cmd)
    results = sql.query(property_id)
    data = {item['station_id']: float(item['distance']) for item in results}
    sql.cursor.close()
    sql.cnx.commit()
    sql.cnx.close()
    data = {item['station_id']: item['distance'] for item in results}
    return data


def epoch_time_converter(datetime_object):
    total_seconds = (datetime_object - datetime(1970, 1, 1)).total_seconds()
    return int(total_seconds) * 1000
