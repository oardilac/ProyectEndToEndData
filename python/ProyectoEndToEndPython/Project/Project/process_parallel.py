from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from azure.storage.blob import ContainerClient
import pandas as pd
from pandasql import sqldf
from io import StringIO
import io
from airflow.utils.task_group import TaskGroup
import sqlalchemy as db
from sqlalchemy import text

pysqldf = lambda q: sqldf(q, locals()) 

args = {"owner": "oardilac", "start_date":days_ago(1)}
dag = DAG(
    dag_id="process_parallel",
    default_args=args,
    schedule_interval='@once', # * * * * * *
)

def get_client_storage(container):
    
    conn_str = "BlobEndpoint=https://adlsdatapath.blob.core.windows.net/;QueueEndpoint=https://adlsdatapath.queue.core.windows.net/;FileEndpoint=https://adlsdatapath.file.core.windows.net/;TableEndpoint=https://adlsdatapath.table.core.windows.net/;SharedAccessSignature=sv=2021-12-02&ss=bfqt&srt=co&sp=rwdlacupyx&se=2023-05-02T10:20:36Z&st=2023-04-28T02:20:36Z&spr=https,http&sig=kXG3%2FeKwh0EnABHPGoee5AISKfH5CEhHsEUsKdgc0mM%3D"
    container_client = ContainerClient.from_connection_string(
        conn_str=conn_str, 
        container_name=container
    )

    return container_client

def read_data_adls(containerName, folderName, fileName):
    
    container = containerName
    container_client = get_client_storage(container)
    downloaded_blob = container_client.download_blob(f"{folderName}/{fileName}")
    df = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
    
    return df


def ingest_data(**kwargs):
    df = read_data_adls(kwargs['containerName'],kwargs['folderName'], kwargs['fileName'])
    load_data_adls(df, 'oardilac/landing', kwargs['fileName'])


def transform_retail(**kwargs):
  
    func_dict = {
                "enunciado1":enunciado1,
                "enunciado2":enunciado2
            }

    df_enunciado= func_dict[kwargs['enunciado']]()
    load_data_adls(df_enunciado, 'oardilac/gold', kwargs['enunciado'])


def load_data_adls(df, folderName, fileName):

    container = 'datalake'
    container_client = get_client_storage(container)
    output = io.StringIO()
    output = df.to_csv(encoding = "utf-8", index=False)
    container_client.upload_blob(f"{folderName}/{fileName}", output, overwrite=True, encoding='utf-8')

def load_data_mysql(**kwargs):
    engine = db.create_engine("mysql://root:root@192.168.244.77:3310/retail_db")
    conn = engine.connect()
    df = read_data_adls('datalake','oardilac/gold', kwargs['enunciado'])
    df.to_sql(kwargs['enunciado'], engine, index=False, if_exists='replace')


def enunciado1():
        df_customer = read_data_adls('datalake','oardilac/landing', 'customers')
        df_orders = read_data_adls('datalake','oardilac/landing', 'orders')
        df_items = read_data_adls('datalake','oardilac/landing', 'order_items')

        q = """
                SELECT
                    customer_id, customer_fname, customer_lname, customer_email, sum(order_item_quantity) as quantity_item_total, sum(order_item_subtotal)as total
                FROM
                    df_customer as c
                INNER JOIN
                    df_orders as o
                    ON c.customer_id = o.order_customer_id
                INNER JOIN
                    df_items as oi
                    ON o.order_id = oi.order_item_order_id
                WHERE order_status <> 'CANCELED'
                GROUP BY customer_id, customer_fname, customer_lname, customer_email
                ORDER BY  total DESC
                LIMIT 20
            """
        result = sqldf(q)

        return result
        

def enunciado2():

    df_products = read_data_adls('datalake','oardilac/landing', 'products')
    df_categories = read_data_adls('datalake','oardilac/landing', 'categories')
    df_items = read_data_adls('datalake','oardilac/landing', 'order_items')

    q = """
            SELECT
                ca.category_name, sum(order_item_quantity) as item_quantity, cast(sum(order_item_subtotal) AS INT )as total
            FROM df_items as oi
            INNER JOIN
                df_products as p
                ON oi.order_item_product_id = p.product_id
            INNER JOIN
                df_categories as ca
                ON p.product_category_id = ca.category_id
            GROUP BY ca.category_name
        """

    result = sqldf(q)

    return result


with dag:
    run_ingest_customer = PythonOperator(task_id="ingest_customers", python_callable=ingest_data, op_kwargs={"containerName": "source","folderName": "retail", "fileName": "customers"}, provide_context=True,)
    run_ingest_orders = PythonOperator(task_id="ingest_orders", python_callable=ingest_data, op_kwargs={"containerName": "source","folderName": "retail", "fileName": "orders"}, provide_context=True,)
    run_ingest_order_items = PythonOperator(task_id="ingest_order_items", python_callable=ingest_data, op_kwargs={"containerName": "source","folderName": "retail", "fileName": "order_items"}, provide_context=True,)
    run_ingest_categories = PythonOperator(task_id="ingest_categories", python_callable=ingest_data, op_kwargs={"containerName": "source","folderName": "retail", "fileName": "categories"}, provide_context=True,)
    run_ingest_products = PythonOperator(task_id="ingest_products", python_callable=ingest_data, op_kwargs={"containerName": "source","folderName": "retail", "fileName": "products"}, provide_context=True,)
    run_ingest_departments = PythonOperator(task_id="ingest_departments", python_callable=ingest_data, op_kwargs={"containerName": "source","folderName": "retail", "fileName": "departments"}, provide_context=True,)
    
    with TaskGroup(group_id="Transform") as transform:
        run_transform_enunciado1 = PythonOperator(task_id="transform_enunciado1", python_callable=transform_retail, op_kwargs={"enunciado": "enunciado1"}, provide_context=True,)
        run_transform_enunciado2 = PythonOperator(task_id="transform_enunciado2", python_callable=transform_retail, op_kwargs={"enunciado": "enunciado2"}, provide_context=True,)

    run_load_enunciado1 = PythonOperator(task_id="load_enunciado1", python_callable=load_data_mysql, op_kwargs={"enunciado": "enunciado1"}, provide_context=True,)
    run_load_enunciado2 = PythonOperator(task_id="load_enunciado2", python_callable=load_data_mysql, op_kwargs={"enunciado": "enunciado2"}, provide_context=True,)
    
    [run_ingest_customer, run_ingest_orders, run_ingest_order_items,run_ingest_categories,run_ingest_products,run_ingest_departments] >> transform  >> [run_load_enunciado1, run_load_enunciado2]
