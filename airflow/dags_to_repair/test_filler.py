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


def fill_worker(start, chunk_size, end):
    size = min(chunk_size, end - start - 1)
    model = P.Filler(threads=1)
    model.update_chunk_rows(start, size)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 1),
}

with DAG(
    "test_filler",
    default_args=default_args,
    description="A simple async DAG",
    schedule_interval=None,
    catchup=False,
    max_active_tasks=1,
    concurrency=1,
) as dag:

    tasks = []
    chunk_size = 10
    start = 54567
    end = 54567 + 10
    for i in range(start, end, chunk_size):
        task = PythonOperator(
            task_id=f"fill_{start}_{end}",
            python_callable=fill_worker,
            op_args=[i + 1, chunk_size, end],
            dag=dag,
            pool="async_pool",
            retries=3,
            retry_delay=timedelta(minutes=5),
        )
        tasks.append(task)

    tasks
