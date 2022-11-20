import psycopg2


try:
    connection = psycopg2.connect(user="postgres",
                                  password="Tanila2019",
                                  host="127.0.0.1",
                                  port="1985",
                                  database="dvdrental")

    cursor = connection.cursor()
    # Executing a SQL query to insert data into  table
    insert_query = """ """
    cursor.execute(insert_query)
    connection.commit()
    print("1 Record inserted successfully")
    # Fetch result
    cursor.execute("SELECT * from customer")
    record = cursor.fetchall()
    print("Result ", record)

    # Executing a SQL query to update table
    update_query = """Update """
    cursor.execute(update_query)
    connection.commit()
    count = cursor.rowcount
    print(count, "Record updated successfully ")
    # Fetch result
    cursor.execute("SELECT * from customer")
    print("Result ", cursor.fetchall())

    # Executing a SQL query to delete table
    delete_query = """Delete """
    cursor.execute(delete_query)
    connection.commit()
    count = cursor.rowcount
    print(count, "Record deleted successfully ")
    # Fetch result
    cursor.execute("SELECT * from customer")
    print("Result ", cursor.fetchall())


except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")