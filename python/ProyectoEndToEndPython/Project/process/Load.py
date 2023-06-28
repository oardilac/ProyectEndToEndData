import os
import pandas as pd
from google.cloud.storage import Client

from utils import utilities as u

class Load():
    def __init__(self) -> None:
        self.process = "Load Process"

    def load_to_cloud_storage (self, bucket_name, folder_name, file_name, df):
        client = u.get_client_cloud_storage()
        bucket = client.get_bucket(bucket_name)
        bucket.blob(f'{folder_name}/{file_name}').upload_from_string(df.to_csv(encoding = "utf-8", index=False), 'text/csv')

    def load_to_mysql(self, db_name, table_name, data):
        conn = u.get_mysql_client(db_name)
        df = pd.DataFrame(data)
        df.to_sql(name=table_name, con=conn, if_exists='append', index=False)