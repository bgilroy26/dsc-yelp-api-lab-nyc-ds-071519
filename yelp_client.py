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
hyelper.create_tables()
business_ids = hyelper.strip_business_ids(businesses)
hyelper.populate_businesses(businesses, cnx, cursor)
reviews = hyelper.get_reviews(api_key)
hyelper.populate_reviews()


