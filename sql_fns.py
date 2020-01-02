#!/usr/bin/env python
# -*- coding: utf-8 -*-


import json
import mysql.connector

from datetime import datetime


class MySQL(object):
    config_file = '/Users/dc/Documents/projects/weather/config.json'
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
    '''Select the buiding and data from train table'''

    cmd = '''
            INSERT INTO stations
            (station_id, coordinate_1, coordinate_2, elevation,
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


def weather_api_call(station_ids):
    pass


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
