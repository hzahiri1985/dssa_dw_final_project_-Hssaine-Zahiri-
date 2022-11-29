import pandas as pd
import psycopg2
from config import config
import numpy as np
from pypika import PostgreSQLQuery, Schema, Column


import pyarrow.parquet as pq

import pyarrow as pa


#creat table 



# The fact Table of our star schema
FACT_RENTAL = (
    Column('sk_customer', 'INT', False),
    Column('sk_date', 'INT', False),
    Column('sk_store', 'INT', False),
    Column('sk_film', 'INT', False),
    Column('sk_staff', 'INT', False),
    Column('count_rentals', 'INT', False)
)
# A dimension Table for staff
DIM_STAFF = (
    Column('sk_staff', 'INT', False),
    Column('name', 'VARCHAR(100)', False),
    Column('email', 'VARCHAR(100)', False)
)


# A dimension Table for customers
DIM_CUSTOMER = (
    Column('sk_customer', 'INT', False),
    Column('name', 'VARCHAR(100)', False),
    Column('email', 'VARCHAR(100)', False)
)


# A dimension table for dates
DIM_DATE = (
    Column('sk_date', 'INT', False),
    Column('date', 'TIMESTAMP', False),
    Column('quarter', 'INT', False),
    Column('year', 'INT', False),
    Column('month', 'INT', False),
    Column('day', 'INT', False),
)



# A dimension table for stores
DIM_STORE = (
    Column('sk_store', 'INT', False),
    Column('name', 'VARCHAR(100)', False),
    Column('address', 'VARCHAR(50)', False),
    Column('city', 'VARCHAR(50)', False),
    Column('state', 'VARCHAR(20)', False),
    Column('country', 'VARCHAR(50)', False)    
)

# A dimension table for films
DIM_FILM = (
    Column('sk_film', 'INT', False),
    Column('rating_code', 'VARCHAR(20)', False),
    Column('film_duration', 'INT', False),
    Column('rental_duration', 'INT', False),
    Column('language', 'CHAR(20)', False),
    Column('release_year', 'INT', False),
    Column('title', 'VARCHAR(255)', False)
)




def connect():
    connection = None
    try:
        params = config()
        print('Connecting to the postgreSQL database ...')
        connection = psycopg2.connect(**params)
        #######################################################
        df_staff = pd.read_sql_query(f"select  staff_id AS sk_staff , concat(first_name ,' ',  last_name) AS name, email FROM staff", connection)
        print(df_staff)

        df_customer = pd.read_sql_query(f"select  customer_id AS sk_customer, concat(first_name ,' ',  last_name) AS name, email FROM customer", connection)
        print(df_customer)


        df_date = pd.read_sql_query(f"select distinct rental.rental_date AS sk_date ,EXTRACT(QUARTER FROM rental_date) AS quarter, EXTRACT(YEAR FROM rental_date) AS year, EXTRACT(MONTH FROM rental_date) AS month, EXTRACT(day FROM rental_date) AS day FROM  rental", connection)
        print(df_date)

        df_store = pd.read_sql_query(f"select  store.store_id AS sk_store, concat(staff.first_name, ' ',staff.last_name) AS name, address.address, city.city, address.district  FROM  store, staff, address, city", connection)
        print(df_store)

        df_FILM = pd.read_sql_query(f"select  film.film_id AS sk_film, film.rating AS rating_code, film.length AS film_duration, film.rental_duration, language.name AS language, film.release_year, film.title   FROM  film, language", connection)
        print(df_FILM)


        df_fact = pd.read_sql_query(f"select  customer.customer_id AS sk_customer, rental.rental_date AS sk_date, store.store_id AS sk_store, Count(rental.rental_id) AS count_rentals FROM customer, rental, store GROUP BY customer.customer_id, rental.rental_date, store.store_id", connection)
        print(df_fact)
        
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            print('Database connection closed.')
            