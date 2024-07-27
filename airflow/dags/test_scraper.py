from airflow import DAG

from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
import sys
import os
import yaml

from importlib.machinery import SourceFileLoader

pwd = os.path.dirname(os.path.realpath(__file__)) + "/otodom_scraper.py"
module = SourceFileLoader("otodom_scraper", pwd).load_module()


def run():
    model = module.Scraper(save_to_db=False, save_to_csv=True, threads=1)
    model.test_run()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 1),
}

with DAG(
    "test_scraper",
    default_args=default_args,
    description="Check available pages",
    schedule_interval=None,
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id=f"check",
        python_callable=run,
        dag=dag,
    )
