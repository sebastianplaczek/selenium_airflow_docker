from airflow import DAG

from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
import sys
import os
import yaml

from importlib.machinery import SourceFileLoader

pwd = os.path.dirname(os.path.realpath(__file__)) + "/pracuj_scraper.py"
P = SourceFileLoader("pracuj_scraper", pwd).load_module()


def scrap_worker(start, chunk_size, n_pages, type):
    size = min(chunk_size, n_pages - start - 1)
    model = P.Scraper(save_to_db=True, threads=5)
    model.scrap_pages(type, start, size)


def check_pages():
    model = P.Scraper(threads=5)
    model.check_pages()


def test_scraper():
    model = P.Scraper(save_to_db=False, test_run=True, threads=5)
    model.run_tests()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 1),
}

with DAG(
    "scrap_offers_dag",
    default_args=default_args,
    description="A simple async DAG",
    schedule_interval=None,
    catchup=False,
    max_active_tasks=4,
    concurrency=4,
) as dag:

    test_scraper_task = PythonOperator(
        task_id=f"tests",
        python_callable=test_scraper,
        dag=dag,
    )

    check_pages_task = PythonOperator(
        task_id=f"check_pages",
        python_callable=check_pages,
        dag=dag,
    )

    with open("scrap_conf.yaml", "r") as file:
        conf = yaml.safe_load(file)

    tasks = []
    for type, n_pages in conf.items():
        chunk_size = 25
        # if type in ['dzialki']:
        for i in range(0, n_pages, chunk_size):
            task = PythonOperator(
                task_id=f"scrapping_{type}_{i}",
                python_callable=scrap_worker,
                op_args=[i + 1, chunk_size, n_pages, type],
                dag=dag,
                pool="async_pool",
                retries=3,
                retry_delay=timedelta(minutes=5),
            )
            tasks.append(task)

        print(f"{type} added to queue")

    test_scraper_task >> check_pages_task >> tasks
