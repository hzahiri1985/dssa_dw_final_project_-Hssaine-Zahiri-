import os
from sqlalchemy import create_engine  
from sqlalchemy.orm import sessionmaker


host = os.environ["localhost"]
port = os.environ["1985"]
user = os.environ["postgres"]
password = os.environ["Tanila2019"]
db = os.environ["dvdrental"]
dbtype = "postgresql+psycopg2"

SQLALCHEMY_DATABASE_URI = f"{dbtype}://{user}:{password}@{host}:{port}/{db}"

oltpEngine = create_engine(SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)
oltpSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=oltpEngine)
