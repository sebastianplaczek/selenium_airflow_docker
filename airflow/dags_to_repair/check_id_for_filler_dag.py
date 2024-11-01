from airflow import DAG

from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
import sys
import os
import yaml

from importlib.machinery import SourceFileLoader

pwd = os.path.dirname(os.path.realpath(__file__)) + "/otodom_filler.py"
module = SourceFileLoader("otodom_filler", pwd).load_module()


def filler_conf():
    model = module.Filler()
    model.conf_for_filler(
        columns="id", from_table="offers", where_cond="where offer_loc_id is NULL"
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 1),
}

with DAG(
    "filler_conf_dag",
    default_args=default_args,
    description="Check start and end id from offers to fill",
    schedule_interval=None,
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id=f"filler_conf_task",
        python_callable=filler_conf,
        dag=dag,
    )
