import psycopg2
from hzahirisrc.config import config
from pypika import PostgreSQLQuery, Schema, Column
import pandas as pd
from psycopg import Cursor
from hzahirisrc.clients.postgres import PostgresClient
from models.queues import QueueFactory



config = '.config/database.ini'
SECTION = 'postgresql'
DW = Schema('dssa')
DVD = Schema('public')
task_queue = QueueFactory.factory()

def connect_to_dvdrental(config:str, section:str='postgresql'):
    client = PostgresClient()
    cursor = client.connect_from_config(path=config, section=section)
    return cursor
def select_from_table(cursor, table_name:str, schema:str):
    cursor.execute(f"SET search path to {schema}, public;")
    result = cursor.execute(f"SELECT * FROM table_name").fetchall()
    return result
def convert_pandas(result):
    pd.DartaFrame(result)
    print(result) 
    
    
task_0=Task(setup_client)
task_1=Task(set_search_path)
tasl_2=Task(create_new_schema)
task_3=Task(read_table)
task_3=Task(to_pandas_df)
#######################################################
'''def read_table(cursor:Cursor, table_name:str, columns:tuple) -> pd.DataFrame:
    """Executes a query to selects Columns and rows from a Table using a cursor 

    Args:
        cursor (Cursor): A cursor instance
        table_name (str): name of the table to query
        columns (tuple): name of columns from the table to select

    Returns:
        pd.DataFrame: Returns results in a pandas dataframe
    """
    query = PostgreSQLQuery \
        .from_(table_name) \
        .select(*columns) \
        .get_sql()
        
    res = cursor.execute(query)
    data = res.fetchall()
    col_names = []
    for names in res.description:
        col_names.append(names[0])
    df = pd.DataFrame(data, columns=col_names)
    return df       
       
 # worker
def run(task_queue):

    result_from_last_task = tuple()
    while not task_queue.empty():
        
        # Get the activity from the queue to process
        task = task_queue.get()
        
        # Get inputs to use from dependencies
        inputs = result_from_last_task

        # Run the task with instructions
        result = task.run(inputs)
        
        # Put the results of the complete task in the result queue
        result_from_last_task = result

        # Activity is finished running
        task_queue.task_done() 
class Task:

    def __init__(self, func:Callable, kwargs:dict):
        self.func = func
        self.kwargs = kwargs
        self.result = None
        
    def run(self, *args):
        self.result = self.func(*args, **self.kwargs)
    
    
cur = Task(func=create_conn, kwargs={'path': DATABASE_CONFIG, 'section': SECTION})
extract_customer = Task(func=read_table, kwargs={'table_name':DVD.customer, 'columns': ('customer_id', 'first_name', 'last_name')})
transform_customer = Task(func=None, kwargs={})
load_customer = Task(func=None, kwargs={})

extract_film = Task(func=None, kwargs={})
transform_film = Task(func=None, kwargs={})
load_film = Task(func=None, kwargs={})

nodes = [
    (cur, extract_customer), (extract_customer, transform_customer), (transform_customer, load_customer),
    (cur, extract_film), (extract_film, transform_film), (transform_film, load_film)
    ]

DAG = nx.DiGraph(nodes)

print(DAG)

# Checks that our DAG is constructed properly
is_directed_acyclic_graph(DAG)
is_weakly_connected(DAG)
is_empty(DAG)


# This the topological order piece. 
for task_node in topological_sort(DAG):
    
    # For each node we get we want to put them in a Queue
    task_queue.put(task_node)

run(task_queue)'''
