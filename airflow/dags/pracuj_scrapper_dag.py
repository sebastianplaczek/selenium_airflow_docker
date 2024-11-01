from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta
import os
import yaml

from importlib.machinery import SourceFileLoader

pwd = os.path.dirname(os.path.realpath(__file__)) + "/pracuj_scraper.py"
P = SourceFileLoader("pracuj_scraper", pwd).load_module()


def scrap_worker(start, chunk_size, n_pages, create_date,type):
    size = min(chunk_size, n_pages - start - 1)
    model = P.PracujScraper(name="pracuj",system="linux", database="gcp", create_date=create_date)
    model.scrap_chunk_pages(start, size,type)


# def test_scraper():
#     model = P.Scraper(save_to_db=False, test_run=True, threads=5)
#     model.run_tests()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 1),
}

with DAG(
    "scrap_pracuj_dag",
    default_args=default_args,
    description="A simple async DAG",
    schedule_interval=None,
    catchup=False,
    max_active_tasks=1,
    concurrency=1,
) as dag:

    # test_scraper_task = PythonOperator(
    #     task_id=f"tests",
    #     python_callable=test_scraper,
    #     dag=dag,
    # )

    current_dir = os.path.dirname(os.path.abspath(__file__))
    conf_path = os.path.join(current_dir, "conf", "pracuj_conf.yml")

    with open(conf_path, "r") as file:
        conf = yaml.safe_load(file)

    with TaskGroup(
        "scrap_task_group", tooltip="Task Group"
    ) as scrap_task_group:
        for type, n_pages in conf.items():
            chunk_size = 5
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
                    pool="async_pool",
                    retries=3,
                    retry_delay=timedelta(minutes=5),
                )
    # test_scraper_task >> check_pages_task >> tasks
    scrap_task_group
