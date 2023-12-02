from connect_settings import conn, engine
conn.autocommit = False
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging as log

from datetime import datetime, timedelta

import sys
import os
sys.path.insert(0, '/opt/airflow/dags/')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from raw.yandex.parser import YandJobParser, table_name
from variables_settings import variables, base_yand, profs
from parser_dags.base_dag import BaseDags


log.basicConfig(
    format='%(threadName)s %(name)s %(levelname)s: %(message)s',
    level=log.INFO
)

# Default dag arguments
default_args = {
    "owner": "admin_1T",
    'start_date': datetime(2023, 11, 26),
    'retry_delay': timedelta(minutes=5),
}


class Dags(BaseDags):
    def run_init_yandex_parser(self):
        """
        Основной вид задачи для запуска парсера для вакансий vacancy_url
        """
        log.info('Запуск парсера yandex')
        try:
            parser = YandJobParser(base_yand, profs, log, conn, table_name)
            parser.find_vacancies()
            parser.find_vacancies_description()
            parser.addapt_numpy_null()
            parser.save_df()
            parser.stop()
            log.info('Парсер yandex успешно провел работу')
            self.df = parser.df
        except Exception as e:
            log.error(f'Ошибка во время работы парсера yandex: {e}')

    def run_update_yandex(self):
        """
        Основной вид задачи для запуска парсера для вакансий yandex
        """
        log.info('Запуск парсера yandex')
        try:
            parser = YandJobParser(base_yand, profs, log, conn, table_name)
            parser.find_vacancies()
            parser.find_vacancies_description()
            parser.generating_dataframes()
            parser.addapt_numpy_null()
            parser.update_database_queries()
            log.info('Парсер yandex успешно провел работу')
            self.dataframe_to_update = parser.dataframe_to_update
            self.dataframe_to_closed = parser.dataframe_to_closed
        except Exception as e:
            log.error(f'Ошибка во время работы парсера yandex: {e}')

def init_call_all_func():
    worker = Dags()
    worker.run_init_yandex_parser()
    worker.update_dicts()
    worker.model(worker.df)
    worker.dml_core_init(worker.dfs)

def update_call_all_func():
    worker = Dags()
    worker.run_update_yandex()
    worker.update_dicts()
    worker.archiving(worker.dataframe_to_closed)
    worker.model(worker.dataframe_to_update)
    worker.dml_core_update(worker.dfs)


with DAG(
        dag_id="init_yandex_parser",
        schedule_interval=None, tags=['admin_1T'],
        default_args=default_args,
        catchup=False
) as dag_initial_yandex:
    parse_yandex_jobs = PythonOperator(
        task_id='init_yandex_task',
        python_callable=init_call_all_func,
        provide_context=True
    )

with DAG(
        dag_id="update_yandex_parser",
        schedule_interval=None, tags=['admin_1T'],
        default_args=default_args,
        catchup=False
) as yandex_update_dag:
    parse_delta_yandex_jobs = PythonOperator(
        task_id='update_yandex_task',
        python_callable=update_call_all_func,
        provide_context=True
    )