from airflow import DAG

from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
import sys
import os
import yaml

from importlib.machinery import SourceFileLoader

pwd = os.path.dirname(os.path.realpath(__file__)) + "/otodom_filler.py"
P = SourceFileLoader("otodom_filler", pwd).load_module()


def fill_worker(start,chunk_size,end):
    size = min(chunk_size, end - start - 1)
    model = P.Filler(threads=1)
    model.update_chunk_rows(start,size)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 1),
}

with DAG(
    "filler_dag1",
    default_args=default_args,
    description="A simple async DAG",
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_tasks=4,
    concurrency=4,
) as dag:

    with open("fill_conf.yaml", "r") as file:
        conf = yaml.safe_load(file)

    tasks = []
    chunk_size = 10000
    for i in range(conf['start_id'], conf['end_id'], chunk_size):

        task = PythonOperator(
            task_id=f"fill_{i}_{i+chunk_size}",
            python_callable=fill_worker,
            op_args=[i+1,chunk_size,conf['end_id']],
            dag=dag,
            pool="async_pool",
            retries=3,
            retry_delay=timedelta(minutes=5),
        )
        tasks.append(task)

    tasks
