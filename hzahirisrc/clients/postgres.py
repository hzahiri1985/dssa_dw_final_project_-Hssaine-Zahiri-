import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="dvdrental",
    user="postgres",
    port= '1985',
    password="Tanila2019")