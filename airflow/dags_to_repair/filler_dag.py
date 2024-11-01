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
    "filler_dag",
    default_args=default_args,
    description="A simple async DAG",
    schedule_interval=None,
    catchup=False,
    max_active_tasks=15,
    concurrency=15,
) as dag:

    with open("fill_conf.yml", "r") as file:
        conf = yaml.safe_load(file)

    tasks = []
    chunk_size = 10000
    len_numbers = len(str(conf["end_id"]))
    for i in range(conf["start_id"], conf["end_id"], chunk_size):
        if len(str(i)) < len_numbers:
            start_index = (len_numbers - len(str(i))) * "0" + str(i)
        else:
            start_index = i
        if len(str(i + chunk_size)) < len_numbers:
            end_index = (len_numbers - len(str(i + chunk_size))) * "0" + str(i)
        else:
            end_index = i
        task = PythonOperator(
            task_id=f"fill_{start_index}_{end_index}",
            python_callable=fill_worker,
            op_args=[i + 1, chunk_size, conf["end_id"]],
            dag=dag,
            pool="filler_pool",
            retries=3,
            retry_delay=timedelta(minutes=5),
        )
        tasks.append(task)

    tasks
