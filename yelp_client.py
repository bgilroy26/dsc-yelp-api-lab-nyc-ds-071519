import config
import mysql.connector
import hyelper

cnx = mysql.connector.connect(**config.config)
cursor = cnx.cursor()

term = 'bbq'
location = 'Nashville TN'

url_params = {  'term': term.replace(' ', '+'),
                'location': location.replace(' ', '+'),
                'limit' : 50
             }

api_key = config.api_key

businesses = hyelper.get_businesses(url_params, api_key)
hyelper.drop_database(cursor)
hyelper.create_database(cursor)

cnx.database = 'yelp'

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

hyelper.create_schema(cursor, TABLES)
business_ids = hyelper.strip_businesses_ids(businesses)
hyelper.populate_businesses(businesses, cnx, cursor)
reviews = hyelper.get_reviews(api_key, business_ids)
hyelper.populate_reviews(business_ids, cnx, cursor)
