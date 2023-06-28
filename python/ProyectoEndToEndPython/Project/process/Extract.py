from io import StringIO
import pandas as pd
import os
from sqlalchemy import text
import pandas as pd
from pandas import DataFrame
from google.cloud.storage import Client
from pymongo import MongoClient

from utils import utilities as u


class Extract():
    
    def __init__(self) -> None:
        self.process = "Extractprocess"

    def read_mysql(self, db_name, table_name):
        conn = u.get_mysql_client(db_name)
        df = pd.read_sql_query(text(f'SELECT * FROM {table_name}'), con=conn)
        return df

    def read_cloud_storage(self, bucket_name, folder_name, file_name):
        
        client = u.get_client_cloud_storage()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.get_blob(f'{folder_name}/{file_name}')
        data_downloaded = blob.download_as_text(encoding="utf-8")
        df = pd.read_csv(StringIO(data_downloaded), on_bad_lines='skip')
        return df
        

    def read_mongodb(self, db_name, col_name):
        dbname = u.get_mongodb_client(db_name)
        # Create a new collection
        collection_name = dbname[col_name]
        collection = collection_name.find({})
        df = DataFrame(collection)
        return df
