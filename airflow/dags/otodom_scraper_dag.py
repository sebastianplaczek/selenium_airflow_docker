from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta
import os
import yaml

from importlib.machinery import SourceFileLoader

pwd = os.path.dirname(os.path.realpath(__file__)) + "/otodom_scraper.py"
P = SourceFileLoader("otodom_scraper", pwd).load_module()


def scrap_worker(start, chunk_size, n_pages, type,create_date):
    size = min(chunk_size, n_pages - start - 1)
    model = P.OtodomScraper(name='otodom',system='linux',database='gcp',create_date=create_date)
    model.scrap_chunk_pages(start, size,type)


def test_scraper():
    model = P.OtodomScraper(name='otodom',system='linux',database='gcp')
    model.run_tests()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 1),
}

with DAG(
    "scrap_otodom_dag",
    default_args=default_args,
    description="A simple async DAG",
    schedule_interval=None,
    catchup=False,
    max_active_tasks=2,
    concurrency=2,
) as dag:

    test_scraper_task = PythonOperator(
        task_id="tests",
        python_callable=test_scraper,
        dag=dag,
    )

    current_dir = os.path.dirname(os.path.abspath(__file__))
    conf_path = os.path.join(current_dir, "conf", "otodom_conf.yml")

    with open(conf_path, "r") as file:
        conf = yaml.safe_load(file)

    with TaskGroup(
        "scrap_task_group", tooltip="Task Group"
    ) as scrap_task_group:
        for type, n_pages in conf.items():
            chunk_size = 25
            for i in range(0, n_pages, chunk_size):
                task = PythonOperator(
                    task_id=f"scrapping_{type}_{i}",
                    python_callable=scrap_worker,
                    op_kwargs={
                        "start": i + 1,
                        "chunk_size": chunk_size,
                        "n_pages": n_pages,
                        "create_date": "{{ logical_date }}",
                        "type" : type,
                    },
                    dag=dag,
                    pool="async_pool",
                    retries=3,
                    retry_delay=timedelta(minutes=5),
                )

            print(f"{type} added to queue")

    test_scraper_task  >> scrap_task_group
