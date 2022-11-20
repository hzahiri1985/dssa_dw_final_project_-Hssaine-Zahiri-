#import needed libraries
from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os
from config import config
import psycopg2



#get password from environmnet var

connection = psycopg2.connect(user="postgres",
                                  password="Tanila2019",
                                  host="127.0.0.1",
                                  port="1985",
                                  database="dvdrental")

#extract data from sql server
def extract():
    try:
        src_conn = pyodbc.connect("connection")
        src_cursor = src_conn.cursor()
        # execute query
        src_cursor.execute(""" select* from customer """)
        src_tables = src_cursor.fetchall()
        for tbl in src_tables:
            #query and load save data to dataframe
            df = pd.read_sql_query(f'select * FROM customer', src_conn)
            load(df, tbl[0])
    except Exception as e:
        print("Data extract error: " + str(e))
    finally:
        src_conn.close()

#load data to postgres
def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{uid}:{pwd}@{server}:5432/AdventureWorks')
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save df to postgres
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        # add elapsed time to final print out
        print("Data imported successful")
    except Exception as e:
        print("Data load error: " + str(e))

try:
    #call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))
   
