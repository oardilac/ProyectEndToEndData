from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.utils.task_group import TaskGroup

args = {"owner": "oardilac", "start_date": days_ago(1)}
dag = DAG(
    dag_id="ingesta_parallel",
    default_args=args,
    schedule_interval='@once', # * * * * * *
)

bash_command_template = 'python "/user/app/ProyectoEndToEndPython/Project/ingest.py" "{}" "{}"'

with dag:
    ingest_tasks = []
    for source_sink in [("retail/customers", "landing/customers"), 
                        ("retail/orders", "landing/orders"),
                        ("retail/order_items", "landing/order_items"),
                        ("retail/categories", "landing/categories"),
                        ("retail/products", "landing/products"),
                        ("retail/departments", "landing/departments")]:
        source, sink = source_sink
        task = BashOperator(
            task_id=f'ingest_{source.replace("/", "_")}',
            bash_command=bash_command_template.format(source, sink)
        )
        ingest_tasks.append(task)

    with TaskGroup(group_id="Transform") as transform:
        run_script_enunciado1 = BashOperator(
            task_id='run_script_enunciado1',
            bash_command='python "/user/app/ProyectoEndToEndPython/Project/Transform_parallel.py" "enunciado1"'
        )

        run_script_enunciado2 = BashOperator(
            task_id='run_script_enunciado2',
            bash_command='python "/user/app/ProyectoEndToEndPython/Project/Transform_parallel.py" "enunciado2"'
        )

    for ingest_task in ingest_tasks:
        ingest_task >> transform
