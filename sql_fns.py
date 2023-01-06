#!/home/dchristiansen/Envs/email/bin/python
# -*- coding: utf-8 -*-


import re
import sys
import json
import mysql.connector
from os import listdir
from datetime import datetime, timedelta
from calendar import monthrange

from sql_object import MySQL


def query_property_coordinates(property_id):

    cmd = '''
        SELECT latitude, longitude
        FROM properties
        WHERE id = %s
        ;
        '''

    mysql = MySQL(cmd)
    data = mysql.select(property_id)
    latitude = float(data[0]['latitude'])
    longitude = float(data[0]['longitude'])
    mysql.cursor.close()
    mysql.cnx.close()
    return latitude, longitude


def query_station_ids():
    cmd = '''
            SELECT DISTINCT station_id
            FROM weather_stations
            ;
            '''
    mysql = MySQL(cmd)
    results = mysql.select()
    data = [item['station_id'] for item in results]
    mysql.cursor.close()
    mysql.cnx.close()
    return data


def store_station_data(property_id, latidude, longitude, station_id):

    cmd = '''
            INSERT INTO weather_stations
            (property_id, latitude, longitude, station_id)
            VALUES
            (%s, %s, %s, %s)
            ;
            '''
    mysql = MySQL(cmd)
    mysql.insert(property_id, latidude, longitude, station_id)
    mysql.cursor.close()
    mysql.cnx.commit()
    mysql.cnx.close()
    return None


def store_weather_data(station_id, timestamp, temperature, relative_humidity, weather_icon):

    cmd = '''
            INSERT INTO weather_data
            (station_id, ts, temperature, relative_humidity, weather_icon)
            VALUES
            (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              temperature = %s,
              relative_humidity = %s,
              weather_icon = %s
            ;
            '''
    mysql = MySQL(cmd)
    mysql.insert(station_id, timestamp, temperature, relative_humidity, weather_icon, temperature, relative_humidity, weather_icon)
    mysql.cursor.close()
    mysql.cnx.commit()
    mysql.cnx.close()
    return None


def query_property_ids():
    cmd = '''
            SELECT id
            FROM properties
            ;
            '''
    mysql = MySQL(cmd)
    results = mysql.select()
    data = [item['property_id'] for item in results]
    mysql.cursor.close()
    mysql.cnx.close()
    return data






































#
