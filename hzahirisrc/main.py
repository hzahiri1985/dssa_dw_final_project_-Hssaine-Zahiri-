# By Hssaine Zahiri 
# This script is to demonstrate ETL workflow
#ETL is a process that extracts, transforms, and loads data from multiple sources to a data warehouse or other unified data repository.
# Import all the libraries we need for this project 
import os
import pandas as pd
import psycopg2
import numpy as np
from pypika import PostgreSQLQuery, Schema, Column
import pandas.io.sql as psql
from sqlalchemy import create_engine
import pyarrow.parquet as pq
import pyarrow as pa
import pandas.io.sql as psql
from decouple import config

#*********************************************************************#
#*************************PARAMETERS**********************************#
#*********************************************************************#

#Use of os.environ to get access of environment variables
#Create environment variables & store them in the launch.json file
#Insert environment variables by adding "env" to the configuration json
#Note storing environment variables will require you to run scripts in debugger mode only.

host=os.environ["host"]
dbname=os.environ["dbname"]
user=os.environ["user"]
port=os.environ["port"]
password=os.environ["password"]
dbtype = "postgresql+psycopg2"

#Creating an engine

engine = f"{dbtype}://{user}:{password}@{host}:{port}/{dbname}"

# connecting python to SQLusing pyodbc
from config import config
# read connection parameters
params = config()
# connect to postgresql server
conn = psycopg2.connect(**params)
# open a cursor to perform database operations
cursor = conn.cursor()
# create DSSA schema if doesn't exist

cursor.execute('''
	      create schema IF NOT EXISTS dssa;''')

#********************************************************************#
#*************************CREATING TABLES****************************#
#********************************************************************#

# create FACT_RENTAL table
cursor.execute('''
		create table if not exists dssa.FACT_RENTAL (
            sk_customer INT NOT NULL,
            sk_date Date NOT NULL,
            sk_store INT NOT NULL,
            sk_film INT NOT NULL,
            sk_staff INT NOT NULL,
            count_rentals INT )
               ''')
# create STAFF table
cursor.execute('''
		create table if not exists dssa.STAFF (
            sk_staff INT NOT NULL,
            name varchar(100) NOT NULL,
            email varchar(100))
               ''')
# create CUSTOMER table
cursor.execute('''
		create table if not exists dssa.CUSTOMER (
            sk_customer INT NOT NULL,
            name varchar(100) NOT NULL,
            email varchar(100))
               ''')
# create DATE table
cursor.execute('''
		create table if not exists dssa.DATE (
            sk_date INT NOT NULL,
            quarter INT NOT NULL,
            year INT NOT NULL, 
            month INT NOT NULL,
            day INT NOT NULL) 
               ''')

# create STORE table
cursor.execute('''
		create table if not exists dssa.STORE(
            sk_store INT NOT NULL,
            name varchar(100) NOT NULL,
            address varchar(100) NOT NULL,
            city varchar(50) NOT NULL,
            state varchar(50) NOT NULL,
            country varchar(50) NOT NULL) 
               ''')

# create FILM table
cursor.execute('''
		create table if not exists dssa.FILM (
            sk_film INT NOT NULL,
            rating_code INT NOT NULL,
            film_duration INT NOT NULL,
            rental_duration INT NOT NULL,
            language VARCHAR(50) NOT NULL,
            release_year Date NOT NULL,
            title VARCHAR(100) NOT NULL) 
               ''')

conn.commit()

#******************************************************************#
#**********************Build STAR SCHEMA***************************#
#******************************************************************#

# I used SQl script to extract the data and do the transfomation needed.

# Fact Table: FACT_RENTAL
SQL_FACT_Rental= pd.read_sql_query('''
                              SELECT c.customer_id AS sk_customer,
                              r.rental_date AS sk_date, 
                              st.store_id AS sk_store,
                              f.film_id AS sk_film,
                              s.staff_id AS sk_staff, 
                              COUNT(rental_id) AS count_rentals
                              FROM customer AS c 
                              JOIN rental AS r ON c.customer_id = r.customer_id 
                              JOIN staff AS s ON r.staff_id = s.staff_id
                              JOIN store AS st ON s.store_id = st.store_id 
                              JOIN inventory AS i ON r.inventory_id = i.inventory_id
                              JOIN film AS f ON i.film_id = f.film_id 
                              GROUP BY sk_customer,sk_date,sk_store,sk_film,sk_staff ''', conn)
                              
df_FACT_Rental = pd.DataFrame(SQL_FACT_Rental, columns = ['sk_customer', 'sk_date','sk_store', 'sk_film','sk_staff','count_rentals'])
print(df_FACT_Rental)

# Dimension Table: STAFF

SQL_staff = pd.read_sql_query("SELECT DISTINCT  staff_id AS sk_staff , concat(first_name ,' ',  last_name) AS name, email FROM public.staff", conn)
df_staff = pd.DataFrame(SQL_staff, columns = ['sk_staff', 'name','email'])
print(df_staff)

# Dimension Table: CUSTOMER
SQL_customer= pd.read_sql_query("SELECT DISTINCT  customer_id AS sk_customer , concat(first_name ,' ',  last_name) AS name, email FROM  public.customer", conn)
df_customer = pd.DataFrame(SQL_customer, columns = ['sk_customer', 'name','email'])
print(df_customer)

# Dimension Tablw: DATE
SQL_Date = pd.read_sql_query("SELECT DISTINCT  rental_date AS sk_date , EXTRACT(QUARTER FROM rental_date) AS quarter, EXTRACT(YEAR FROM rental_date) AS year, EXTRACT(MONTH FROM rental_date) AS month, EXTRACT(day FROM rental_date) AS day FROM  rental", conn)
df_Date = pd.DataFrame(SQL_Date, columns = ['sk_date', 'quarter','year', 'month','day'])
print(df_Date)

# Dimesnion Table: STORE
SQL_Store = pd.read_sql_query('''
                              SELECT DISTINCT  sto.store_id AS sk_store , 
                              concat(first_name ,' ',  last_name) AS name,
                              address, 
                              city, 
                              district,
                              country
                              FROM store AS sto
                              JOIN staff AS sta on sto.store_id = sta.store_id
                              JOIN address AS ad on sta.address_id = ad.address_id
                              JOIN city AS ci on ad.city_id = ci.city_id
                              JOIN country AS co on ci.country_id = co.country_id
                              ''', conn)
df_Store = pd.DataFrame(SQL_Store, columns = ['sk_store', 'name','address', 'city','state','country'])
print(df_Store)

# Dimension Table: FILM

SQL_FILM = pd.read_sql_query('''
                              SELECT DISTINCT film_id AS sk_film , 
                              rating AS rating_code, 
                              length AS film_duration, 
                              rental_duration, 
                              name AS language, 
                              release_year,
                              title
                              FROM film AS f
                              JOIN language AS lg ON f.language_id = lg.language_id 
                              ''', conn)
df_FILM = pd.DataFrame(SQL_FILM, columns = ['sk_film', 'rating_code','film_duration', 'rental_duration','language','release_year', 'title'])
print(df_FILM)

#******************************************************************#
#**********************loading STAR SCHEMA*************************#
#******************************************************************#

#load Film data frame 

    
df_FILM.to_sql('film', engine, schema ='dssa' , if_exists ='replace')

#load store data frame 

df_Store.to_sql('store', engine, schema ='dssa' , if_exists ='replace')

#load date data frame 

df_Date.to_sql('date', engine, schema ='dssa' , if_exists ='replace')

#load customer data frame 

df_customer.to_sql('customer', engine, schema ='dssa' , if_exists ='replace')

#load staff data frame 

df_staff.to_sql('staff', engine, schema ='dssa' , if_exists ='replace')

#load rental data frame 

df_FACT_Rental.to_sql('fact_rental', engine, schema ='dssa' , if_exists ='replace')

#Lose the connection
cursor.close()
conn.close()



