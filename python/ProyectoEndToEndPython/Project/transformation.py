from process.Extract import Extract
from process.Transform import Transform
from process.Load import Load
import pandas as pd

extract = Extract()
transform = Transform()
load = Load()

print("INICIA LA EXTRACCIÓN DE DATOS DESDE LA CAPA LANDING")

df_customers = extract.read_mysql("retail_db","customers")
df_orders = extract.read_mysql("retail_db", "orders") 
df_order_items = extract.read_mysql("retail_db", "order_items") 
df_departments = extract.read_mysql("retail_db", "departments") 
df_categories = extract.read_mysql("retail_db","categories") 
df_products = extract.read_mysql("retail_db","products") 

print("TERMINA LA EXTRACCIÓN DE DATOS DESDE LA CAPA LANDING")

print("INICIA LA TRANSFORMACIÓN DE LOS DATOS")

enunciado1 = transform.enunciado1(df_customers, df_orders, df_order_items)
enunciado2 = transform.enunciado2(df_order_items, df_products,df_categories)
enunciado3 = transform.enunciado3(df_customers, df_orders, df_order_items, df_products, df_categories)

print("TERMINA LA TRANSFORMACIÓN DE LOS DATOS")

print("INICIA LA CARGA DE DATOS A LA CAPA GOLD")

load.load_to_cloud_storage('chatmine-388722', 'gold', 'e1_venta_mes', enunciado1)
load.load_to_cloud_storage('chatmine-388722', 'gold', 'e2_cant_vendida_productos', enunciado2)
load.load_to_cloud_storage('chatmine-388722', 'gold', 'e3_ventas_ciudades', enunciado3)


print("TERMINA LA CARGA DE DATOS A LA CAPA GOLD")

print("INICIA LA CARGA DE DATOS DE BI A MYSQL")

load.load_to_mysql('bi', 'enun1', enunciado1)
load.load_to_mysql('bi', 'enun2', enunciado2)
load.load_to_mysql('bi', 'enun3', enunciado3)

print("TERMINA LA CARGA DE DATOS DE BI A MYSQL")