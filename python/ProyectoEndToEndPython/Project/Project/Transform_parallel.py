from process.Extract import Extract
from process.Transform import Transform
from process.Load import Load
import pandas as pd
import sys

extract = Extract()
transform = Transform()
load = Load()

# Enunciado1
# extract customers, orders, order_items
# transf  enunciado1(customers, orders, order_items)
# load   lload_to_adls(df_enunciado)

enunciado = sys.argv[1]


parametros = {
'enunciado1':['customers', 'orders', 'order_items'],
'enunciado2':['order_items', 'products', 'categories'],
    
}


def extractor(file_name):
    df = extract.read_adls("datalake",f"landing/{file_name}")
    return df

def transform_enun1(file_names):
    df_customers = extractor(file_names[0])
    df_orders = extractor(file_names[1])
    df_order_items = extractor(file_names[2])
    df = transform.enunciado1(df_customers, df_orders, df_order_items)
    return df

def transform_enun2(file_names):
    df_order_items = extractor(file_names[0])
    df_products = extractor(file_names[1])
    df_categories = extractor(file_names[2])
    df = transform.enunciado2(df_order_items, df_products,df_categories)
    return df

def load_func(df, enun_name):
    load.load_to_adls(df,"datalake", f"gold/{enun_name}")

func_dict = {
                "enunciado1":transform_enun1,
                "enunciado2":transform_enun2
            }

# enunciado = enunciado1
# parametros[enunciado] = ['customers', 'orders', 'order_items']
# func_dict[enunciado](parametros[enunciado]) = transform_enun1(['customers', 'orders', 'order_items'])

df_enunciado = func_dict[enunciado](parametros[enunciado])
load_func(df_enunciado, enunciado)