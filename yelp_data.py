import requests
import sys
import config
import time
import json
import mysql.connector
import pandas as pd
import matplotlib as plt
from mysql.connector import errorcode
%load_ext autoreload

%autoreload
# Your code here; use a function or loop to retrieve all the results from your original request

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

term = 'bbq'
location = 'Nashville TN'
url_params = {  'term': term.replace(' ', '+'),
                'location': location.replace(' ', '+'),
                'limit' : 50
             }
df = all_results(url_params, config.api_key)


print(len(df))biz_id = df[0][0]["id"]
response = yelp_review_call(config.api_key, biz_id)#method for connecting to yelp database, return tuple of cnx/c to avoid scoping
def connect_to_yelp():
    cnx = mysql.connector.connect(**config.config)
    c = cnx.cursor()
    statement = """USE yelp"""
    c.execute(statement)
    return (c, cnx)
    test_id = df[0][0]["id"]
response = yelp_review_call(config.api_key, test_id)reviews = json.loads(response.text)
reviews['reviews'][0]
#time created, id, rating, {foregin key}import pandas as pd
import mysql.connector
from mysql.connector import errorcode

cnx = mysql.connector.connect(**config.config)
c = cnx.cursor()

c.execute("""DROP DATABASE IF EXISTS yelp""")

DB_NAME = 'yelp'

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


#returns true if all the necessary keys are in the restaurant dict
def all_rest_elements(restaurant):
    #this the way check if all vital keys in restaurant
    if all (k in restaurant for k in ("name","id", "rating", "price")):
        if len(restaurant['price']) > 0:
            return True
    return False


#same but for review
def all_review_elements(review):
    if all (k in review for k in ("id","restaurant_id", "rating", "time_created")):
        return True
    else:
        return False
    
cnx = mysql.connector.connect(**config.config)
c = cnx.cursor()

c.execute("""USE yelp""")
def populate_businesses(df):

#above should be an extra quote, jupyter broke
    for load in df:
        for restaurant in load:
            if all_rest_elements(restaurant):
                #assign ids before for formatting the sql statement
                rest_id = restaurant['id']
                name = restaurant['name']
                rating = restaurant['rating']
                price = restaurant['price']
                #sql for inserting this restaurants
                insert_str = f"""
                    INSERT INTO restaurants
                    (restaurant_id, name, rating, price) 
                    VALUES ("{rest_id}", 
                            "{name}", 
                            "{rating}", 
                            "{price}")
                    """    
                #execute and save    
                c.execute(insert_str)
                cnx.commit()
    #return dummy string
    return "Data Added"
    
populate_businesses(df)
cnx.close()
#use select to retrieve list of all business ids
def get_all_biz_ids():
    c, cnx = connect_to_yelp()
    select_ids = """SELECT restaurant_id
                    FROM restaurants"""
    c.execute(select_ids)
    all_ids = c.fetchall()
    return all_ids

def add_biz_id(data, biz_id):
    for review in data:
        review["restaurant_id"] = biz_id
    #data will be edited, so just return data
    return data

#retrieves all reviews and puts them into json format
def get_reviews(api_key):
    all_biz_ids = get_all_biz_ids()
    #initialize all_reviews for mapping
    all_reviews = []
    #go through each of the biz ids, get the data corresponding to it,
    #and add the biz id for foreign key use later
    for biz_id_tup in all_biz_ids:
        biz_id = biz_id_tup[0]
        #gets a dictionary from the API adding biz Id to the url
        response = yelp_review_call(api_key, biz_id)
        data = response.json()['reviews']
        #add restaurant_id
        data_with_biz = add_biz_id(data, biz_id)
        all_reviews.extend(data_with_biz)
    return all_reviews

reviews = get_reviews(config.api_key)
reviews[4]#some review text has quotes and was causing conflicts when adding to db
#replace " with \"
def replace_quotes(text):
    return text.replace(r'"', r'\"')c, cnx = connect_to_yelp()

def populate_reviews(df):

    for review in reviews:
        if all_review_elements(review):
            #assign ids before for formatting the sql statement
            rev_id = review['id']
            rest_id = review['restaurant_id']
            time_stamp = review['time_created']
            rev_rating = review['rating']
            rev_text = replace_quotes(review['text'])
            #sql for inserting this restaurants
            insert_str = f"""
                INSERT INTO reviews
                (review_id, time_created, review_text, user_rating, restaurant_id) 
                VALUES ("{rev_id}", 
                        "{time_stamp}",
                        "{rev_text}",
                        "{rev_rating}", 
                        "{rest_id}")
                """    
            #execute and save    
            c.execute(insert_str)
            cnx.commit()
    #return dummy string
    return "Data Added"
    
populate_reviews(reviews)
cnx.close()
c, cnx = connect_to_yelp()
def display_query(query):
    c.execute(query)
    df = pd.DataFrame(c.fetchall())
    df.columns = [column[0] for column in c.description]
    return df.head() 


def top_five():
    top_five_query = """SELECT name, rating
                    FROM restaurants
                    ORDER BY rating DESC
                    LIMIT 5"""
    return display_query(top_five_query)

top_five()# Your code here; use a function or loop to retrieve all the results from your original request
def bottom_five():
    bottom_five_query = """SELECT name, rating
                    FROM restaurants
                    ORDER BY rating 
                    LIMIT 5"""
    return display_query(bottom_five_query)

bottom_five()def rating_per_price():
    average_query = """SELECT price, ROUND(AVG(rating), 2) as average_rating
                    FROM restaurants
                    GROUP BY price"""
    return display_query(average_query)

rating_per_price()def highly_rated_restaurants():
    top_query = """SELECT COUNT(*) AS top_rated
                    FROM restaurants
                    WHERE rating > 4.5"""
    return display_query(top_query)

highly_rated_restaurants()def low_rated_restaurants():
    bottom_query = """SELECT COUNT(*) AS bottom_rated
                        FROM restaurants
                        WHERE rating < 3"""
    return display_query(bottom_query)

low_rated_restaurants()def oldest_review():
    oldest_query = """SELECT review_text, time_created, user_rating
                        FROM reviews
                        ORDER BY time_created
                        LIMIT 1"""
    return display_query(oldest_query)

oldest_review()def rating_for_old_review():
    old_rating_query = """SELECT name, time_created, rating
                        FROM restaurants 
                        JOIN reviews
                        ORDER BY time_created ASC
                        LIMIT 1"""
    return display_query(old_rating_query)

rating_for_old_review()def newest_review_for_best():
    new_best_q = """SELECT review_text, user_rating, time_created
                    FROM reviews
                    WHERE restaurant_id = (SELECT restaurant_id
                                            FROM restaurants
                                            ORDER BY rating DESC
                                            LIMIT 1)                  
                    ORDER BY time_created DESC
                    LIMIT 1"""
    return display_query(new_best_q)
newest_review_for_best()def oldest_review_for_worst():
    old_worst_q = """SELECT review_text, user_rating, time_created
                    FROM reviews
                    WHERE restaurant_id = (SELECT restaurant_id
                                            FROM restaurants
                                            ORDER BY rating
                                            LIMIT 1)                  
                    ORDER BY time_created
                    LIMIT 1"""
    return display_query(old_worst_q)
oldest_review_for_worst()def graph_price_count():
    average_query = """SELECT price, COUNT(*) AS total
                    FROM restaurants
                    GROUP BY price"""
    return display_query(average_query)

graph_price_count()#Your code here
import requests
import sys
import config
import time
import json
import mysql.connector
import pandas as pd
import matplotlib as plt
from mysql.connector import errorcode
%load_ext autoreload

%autoreload

# Your code here; use a function or loop to retrieve all the results from your original request

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

term = 'bbq'
location = 'Nashville TN'
url_params = {  'term': term.replace(' ', '+'),
                'location': location.replace(' ', '+'),
                'limit' : 50
             }
df = all_results(url_params, config.api_key)


print(len(df))

biz_id = df[0][0]["id"]
response = yelp_review_call(config.api_key, biz_id)
#method for connecting to yelp database, return tuple of cnx/c to avoid scoping
def connect_to_yelp():
    cnx = mysql.connector.connect(**config.config)
    c = cnx.cursor()
    statement = """USE yelp"""
    c.execute(statement)
    return (c, cnx)
    
test_id = df[0][0]["id"]
response = yelp_review_call(config.api_key, test_id)
reviews = json.loads(response.text)
reviews['reviews'][0]
#time created, id, rating, {foregin key}
import pandas as pd
import mysql.connector
from mysql.connector import errorcode

cnx = mysql.connector.connect(**config.config)
c = cnx.cursor()

c.execute("""DROP DATABASE IF EXISTS yelp""")

DB_NAME = 'yelp'

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



#returns true if all the necessary keys are in the restaurant dict
def all_rest_elements(restaurant):
    #this the way check if all vital keys in restaurant
    if all (k in restaurant for k in ("name","id", "rating", "price")):
        if len(restaurant['price']) > 0:
            return True
    return False


#same but for review
def all_review_elements(review):
    if all (k in review for k in ("id","restaurant_id", "rating", "time_created")):
        return True
    else:
        return False
    

cnx = mysql.connector.connect(**config.config)
c = cnx.cursor()

c.execute("""USE yelp""")
def populate_businesses(df):

#above should be an extra quote, jupyter broke
    for load in df:
        for restaurant in load:
            if all_rest_elements(restaurant):
                #assign ids before for formatting the sql statement
                rest_id = restaurant['id']
                name = restaurant['name']
                rating = restaurant['rating']
                price = restaurant['price']
                #sql for inserting this restaurants
                insert_str = f"""
                    INSERT INTO restaurants
                    (restaurant_id, name, rating, price) 
                    VALUES ("{rest_id}", 
                            "{name}", 
                            "{rating}", 
                            "{price}")
                    """    
                #execute and save    
                c.execute(insert_str)
                cnx.commit()
    #return dummy string
    return "Data Added"
    
populate_businesses(df)
cnx.close()

#use select to retrieve list of all business ids
def get_all_biz_ids():
    c, cnx = connect_to_yelp()
    select_ids = """SELECT restaurant_id
                    FROM restaurants"""
    c.execute(select_ids)
    all_ids = c.fetchall()
    return all_ids

def add_biz_id(data, biz_id):
    for review in data:
        review["restaurant_id"] = biz_id
    #data will be edited, so just return data
    return data

#retrieves all reviews and puts them into json format
def get_reviews(api_key):
    all_biz_ids = get_all_biz_ids()
    #initialize all_reviews for mapping
    all_reviews = []
    #go through each of the biz ids, get the data corresponding to it,
    #and add the biz id for foreign key use later
    for biz_id_tup in all_biz_ids:
        biz_id = biz_id_tup[0]
        #gets a dictionary from the API adding biz Id to the url
        response = yelp_review_call(api_key, biz_id)
        data = response.json()['reviews']
        #add restaurant_id
        data_with_biz = add_biz_id(data, biz_id)
        all_reviews.extend(data_with_biz)
    return all_reviews

reviews = get_reviews(config.api_key)

reviews[4]
#some review text has quotes and was causing conflicts when adding to db
#replace " with \"
def replace_quotes(text):
    return text.replace(r'"', r'\"')
c, cnx = connect_to_yelp()

def populate_reviews(df):

    for review in reviews:
        if all_review_elements(review):
            #assign ids before for formatting the sql statement
            rev_id = review['id']
            rest_id = review['restaurant_id']
            time_stamp = review['time_created']
            rev_rating = review['rating']
            rev_text = replace_quotes(review['text'])
            #sql for inserting this restaurants
            insert_str = f"""
                INSERT INTO reviews
                (review_id, time_created, review_text, user_rating, restaurant_id) 
                VALUES ("{rev_id}", 
                        "{time_stamp}",
                        "{rev_text}",
                        "{rev_rating}", 
                        "{rest_id}")
                """    
            #execute and save    
            c.execute(insert_str)
            cnx.commit()
    #return dummy string
    return "Data Added"
    
populate_reviews(reviews)
cnx.close()

c, cnx = connect_to_yelp()
def display_query(query):
    c.execute(query)
    df = pd.DataFrame(c.fetchall())
    df.columns = [column[0] for column in c.description]
    return df.head() 


def top_five():
    top_five_query = """SELECT name, rating
                    FROM restaurants
                    ORDER BY rating DESC
                    LIMIT 5"""
    return display_query(top_five_query)

top_five()
# Your code here; use a function or loop to retrieve all the results from your original request
def bottom_five():
    bottom_five_query = """SELECT name, rating
                    FROM restaurants
                    ORDER BY rating 
                    LIMIT 5"""
    return display_query(bottom_five_query)

bottom_five()
def rating_per_price():
    average_query = """SELECT price, ROUND(AVG(rating), 2) as average_rating
                    FROM restaurants
                    GROUP BY price"""
    return display_query(average_query)

rating_per_price()
def highly_rated_restaurants():
    top_query = """SELECT COUNT(*) AS top_rated
                    FROM restaurants
                    WHERE rating > 4.5"""
    return display_query(top_query)

highly_rated_restaurants()
def low_rated_restaurants():
    bottom_query = """SELECT COUNT(*) AS bottom_rated
                        FROM restaurants
                        WHERE rating < 3"""
    return display_query(bottom_query)

low_rated_restaurants()
def oldest_review():
    oldest_query = """SELECT review_text, time_created, user_rating
                        FROM reviews
                        ORDER BY time_created
                        LIMIT 1"""
    return display_query(oldest_query)

oldest_review()
def rating_for_old_review():
    old_rating_query = """SELECT name, time_created, rating
                        FROM restaurants 
                        JOIN reviews
                        ORDER BY time_created ASC
                        LIMIT 1"""
    return display_query(old_rating_query)

rating_for_old_review()
def newest_review_for_best():
    new_best_q = """SELECT review_text, user_rating, time_created
                    FROM reviews
                    WHERE restaurant_id = (SELECT restaurant_id
                                            FROM restaurants
                                            ORDER BY rating DESC
                                            LIMIT 1)                  
                    ORDER BY time_created DESC
                    LIMIT 1"""
    return display_query(new_best_q)
newest_review_for_best()
def oldest_review_for_worst():
    old_worst_q = """SELECT review_text, user_rating, time_created
                    FROM reviews
                    WHERE restaurant_id = (SELECT restaurant_id
                                            FROM restaurants
                                            ORDER BY rating
                                            LIMIT 1)                  
                    ORDER BY time_created
                    LIMIT 1"""
    return display_query(old_worst_q)
oldest_review_for_worst()
def graph_price_count():
    average_query = """SELECT price, COUNT(*) AS total
                    FROM restaurants
                    GROUP BY price"""
    return display_query(average_query)

graph_price_count()
#Your code here


