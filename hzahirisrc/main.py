import pandas as pd
import psycopg2
from config import config



def connect():
    connection = None
    try:
        params = config()
        print('Connecting to the postgreSQL database ...')
        connection = psycopg2.connect(**params)

        # create a cursor
        crsr = connection.cursor()
        print('PostgreSQL database version: ')
        
        # extract data from dvdrental 
        crsr.execute('SELECT * from customer')
       
        df_customer = pd.read_sql_query(f'select * FROM customer', connection)
        
        print(df_customer)
        

        
        
        crsr.execute('SELECT * from staff')
       
        df_staff = pd.read_sql_query(f'select * FROM staff', connection)
        print(df_staff)
        
        
        crsr.execute('SELECT * from FILM')
        
        df_FILM = pd.read_sql_query(f'select * FROM FILM', connection)
        print(df_FILM)
        
        #insert data to DSSA
       
        
        crsr.execute('SELECT * from store')

        df_store = pd.read_sql_query(f'select * FROM store', connection)
        print(df_store)
        
     
          
        
        
        
        
        crsr.close()
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            print('Database connection terminated.')
   #load data to postgres

    
        
        
if __name__ == "__main__":
    connect()