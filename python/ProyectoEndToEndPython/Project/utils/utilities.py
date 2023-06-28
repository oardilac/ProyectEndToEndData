import os
import yaml
from io import StringIO
from google.cloud.storage import Client
from pymongo import MongoClient
import sqlalchemy as db

with open('/user/app/ProyectoEndToEndPython/Project/config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)

def get_client_cloud_storage():
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["cloud_storage"]["credentials"]
    client = Client()

    return client

def get_mongodb_client(db_name):
    
    CONNECTION_STRING = config["mongo_db"]["connection_string"]
    client = MongoClient(CONNECTION_STRING)
    dbname = client[db_name]

    return dbname

def get_mysql_client(db_name):
    
    host = config["mysql"]["host"]
    user = config["mysql"]["user"]
    password = config["mysql"]["pass"]
    port = config["mysql"]["port"]
    
    connection_string = f"mysql://{user}:{password}@{host}:{port}/{db_name}"
    engine = db.create_engine(connection_string)
    conn = engine.connect()
    
    return conn