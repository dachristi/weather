#!/usr/bin/env python
# -*- coding: utf-8 -*-


import json
import mysql.connector

from pathlib import Path


class MySQL(object):
    cur_path = Path('.')
    config_file = cur_path / 'config.json'
    #config_file = ('/home/gsmart/metron/export/etc/config.json')
    #with open(config_file, 'r') as json_data_file:
    with config_file.open('r') as json_data_file:
        config = json.load(json_data_file)


    def __init__(self, cmd):
        self.cmd = cmd

        self.cnx = mysql.connector.connect(user=MySQL.config['mysql']['user'],
                                      password=MySQL.config['mysql']['password'],
                                      host=MySQL.config['mysql']['host'],
                                      database=MySQL.config['mysql']['database'])
        self.cursor = self.cnx.cursor(dictionary=True, buffered=True)


    def insert(self, *args):
        if args:
            self.cursor.execute(self.cmd, args)
        else:
            self.cursor.execute(self.cmd)
        return None


    def select(self, *args):
        if args:
            print(args)
            self.cursor.execute(self.cmd, args)
        else:
            self.cursor.execute(self.cmd)
        return self.cursor.fetchall()
