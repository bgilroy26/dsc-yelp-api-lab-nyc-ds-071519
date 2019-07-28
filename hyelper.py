import requests
import sys
import config
import time
import json
import mysql.connector
import pandas as pd
import matplotlib as plt
from mysql.connector import errorcode

class Hyelper:
    def yelp_review_call(api_key, biz_id):
        url = f'https://api.yelp.com/v3/businesses/{biz_id}/reviews'
        headers = {'Authorization': 'Bearer {}'.format(api_key)}
        response = requests.get(url, headers=headers)
        return response

    def yelp_call(url_params, api_key):
        url = 'https://api.yelp.com/v3/businesses/search'
        headers = {'Authorization': 'Bearer {}'.format(api_key)}
        response = requests.get(url, headers=headers, params=url_params)
        return response


    def get_businesses(url_params, api_key):
        response = yelp_call(url_params, api_key)
        print(response.json().keys())
        if 'businesses' in response.json():
            data = response.json()['businesses']
        else:
            raise KeyError
        return data

    def all_results(url_params, api_key):
        #declare url here
        #NOTE refactor into Config later
        response = yelp_call(url_params, api_key)
        num = response.json()['total']
        print('{} total matches found.'.format(num))
        cur = 0
        results = []
        while cur < num and cur < 1000:
            url_params['offset'] = cur
            results.append(get_businesses(url_params, api_key))
            time.sleep(.5) #Wait a second
            cur += 50
        return results

    def connect_to_yelp():
        cnx = mysql.connector.connect(**config.config)
        c = cnx.cursor()
        statement = """USE yelp"""
        c.execute(statement)
        return (c, cnx)
        test_id = df[0][0]["id"]

def create_database(cursor):
    """Creates Database and Catch errors"""
    try:
        c.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

try:
    c.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(DB_NAME))
    #if DB doesn't exist, create the databse for you
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(c)
        print("Database {} created successfully.".format(DB_NAME))
        cnx.database = DB_NAME
    else:
        print(err)
        exit(1)



TABLES = {}
TABLES['restaurants'] = """
        CREATE TABLE `restaurants` (
           `restaurant_id` varchar(32) NOT NULL,
           `name` TEXT NOT NULL,
           `rating` DECIMAL(2,1),
           `price` TEXT,
           PRIMARY KEY(restaurant_id)
        ) ENGINE=InnoDB
        """

TABLES['reviews'] = """
        CREATE TABLE `reviews` (
           `review_id` VARCHAR(32) NOT NULL,
           `time_created` DATE NOT NULL,
           `review_text` TINYTEXT,
           `user_rating` DECIMAL(2,1),
           `restaurant_id` VARCHAR(32),
           PRIMARY KEY(review_id),
           FOREIGN KEY(restaurant_id) REFERENCES restaurants(restaurant_id)
        ) ENGINE=InnoDB
        """

for table_name in TABLES:
    create_table_script = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        c.execute(create_table_script)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")
c.close()
cnx.close()
