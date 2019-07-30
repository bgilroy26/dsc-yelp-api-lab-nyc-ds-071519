from mysql.connector import errorcode
import mysql.connector
import requests
import time
import sys

DB_NAME = 'yelp'

def yelp_call(url_params, api_key, session):
    url = 'https://api.yelp.com/v3/businesses/search'
    headers = {'Authorization': 'Bearer {}'.format(api_key)}
    response = session.get(url, headers=headers, params=url_params)
    return response

def pull_businesses(url_params, api_key, session):
    response = yelp_call(url_params, api_key, session)
    data = response.json()['businesses']
    return data

def get_businesses(url_params, api_key):
    s = requests.Session()
    response = yelp_call(url_params, api_key, s)
    num = response.json()['total']
    cur = 0
    results = []
    while cur < num and cur < 1000:
        url_params['offset'] = cur
        results.append(pull_businesses(url_params, api_key, s))
        time.sleep(.5) #Wait a second
        cur += 50
    print("Businesses pulled")
    return results

def drop_database(cursor):
    cursor.execute("""DROP DATABASE IF EXISTS yelp""")

def create_database(cursor):
    """Creates Database and Catch errors"""
    try:
        cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)
    print("Database created")

def create_schema(cursor, TABLES):
    for table_name in TABLES:
        create_table_script = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(create_table_script)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

def strip_businesses_ids(businesses):
    business_ids = []
    for business_list in businesses:
        for business in business_list:
            business_ids.append(business['id'])
    print('Business ids retrieved')
    return business_ids


#returns true if all the necessary keys are in the restaurant dict
def all_rest_elements(restaurant):
    #this the way check if all vital keys in restaurant
    if all (k in restaurant for k in ("name","id", "rating", "price")):
        if len(restaurant['price']) > 0:
            return True
    return False

def populate_businesses(businesses, cnx, cursor):

    for load in businesses:
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
                cursor.execute(insert_str)
                cnx.commit()
    print("Businesses populated")

def add_biz_id(data, biz_id):
    for review in data:
        review["restaurant_id"] = biz_id
    #data will be edited, so just return data
    return data

def yelp_review_call(api_key, biz_id, session):
    url = f'https://api.yelp.com/v3/businesses/{biz_id}/reviews'
    headers = {'Authorization': 'Bearer {}'.format(api_key)}
    response = session.get(url, headers=headers)
    return response

#retrieves all reviews and puts them into json format
def get_reviews(api_key, all_biz_ids):
    #initialize all_reviews for mapping
    all_reviews = []
    s = requests.Session()
    #go through each of the biz ids, get the data corresponding to it,
    #and add the biz id for foreign key use later
    for biz_id in all_biz_ids:
        #gets a dictionary from the API adding biz Id to the url
        response = yelp_review_call(api_key, biz_id, s)
        try:
            data = response.json()['reviews']
        except KeyError:
            print(biz_id)
        #add restaurant_id
        data_with_biz = add_biz_id(data, biz_id)
        all_reviews.extend(data_with_biz)
    print("Reviews pulled")
    return all_reviews

#same but for review
def all_review_elements(review):
    if all (k in review for k in ("id","restaurant_id", "rating", "text", "time_created")):
        return True
    else:
        return False

#some review text has quotes and was causing conflicts when adding to db
#replace " with \"
def replace_quotes(text):
    return text.replace(r'"', r'\"')

def populate_reviews(reviews, cnx, cursor):

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
            cursor.execute(insert_str)
            cnx.commit()
    print("Reviews populated")
