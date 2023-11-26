import json
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import logging as log
from logging import handlers
from airflow.models import Variable
from datetime import datetime, timedelta
import time
from airflow.utils.log.logging_mixin import LoggingMixin
import os
from sqlalchemy import create_engine
from core.ddl_core import DatabaseManager
from core.dml_core import DataManager
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from raw.connect_settings import conn, engine
from raw.habr_career import HabrJobParser, table_name
from raw.variables_settings import variables, base_habr
from core.model_spacy import Data_preprocessing


log.basicConfig(
    format='%(threadName)s %(name)s %(levelname)s: %(message)s',
    level=log.INFO
)

conn.autocommit = False

# Default dag arguments
default_args = {
    "owner": "admin_1T",
    'start_date': datetime(2023, 11, 26),
    'retry_delay': timedelta(minutes=5),
}

class Dags():
    def run_init_habrcareer_parser(self):
        """
        Основной вид задачи для запуска парсера для вакансий GetMatch
        """
        log.info('Запуск парсера HabrCareer')
        try:
            parser = HabrJobParser(base_habr, log, conn, table_name)
            parser.find_vacancies()
            parser.addapt_numpy_null()
            parser.save_df()
            log.info('Парсер HabrCareer успешно провел работу')
            self.df = parser.df
        except Exception as e:
            log.error(f'Ошибка во время работы парсера HabrCareer: {e}')


    def model(self, df):
        test = Data_preprocessing(df)
        test.call_all_functions()
        self.dfs = test.dict_all_data


    def ddl_core(self, conn):
        manager = DatabaseManager(conn)
        manager.db_creator()

    def dml_core(self, conn, engine, dfs):
        manager = DataManager(conn, engine, dfs, pd.DataFrame({'vacancy_url': ['https://rabota.sber.ru/search/4219605',
                                                                               'https://rabota.sber.ru/search/4221748']}))
        manager.init_load()


def call_all_func():
    worker = Dags()
    worker.run_init_habrcareer_parser()
    worker.ddl_core(conn)
    worker.model(worker.df)
    worker.dml_core(conn, engine, worker.dfs)


with DAG(
        dag_id="init_habrcareer_parser",
        schedule_interval=None, tags=['admin_1T'],
        default_args=default_args,
        catchup=False
    ) as habr_dag:

    parse_get_match_jobs = PythonOperator(
        task_id='init_habrcareer_task',
        python_callable=call_all_func,
        provide_context=True
    )
