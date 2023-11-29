from raw.connect_settings import conn, engine
conn.autocommit = False
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging as log
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from raw.hh_cut_parser import HHJobParser, table_name
from raw.variables_settings import variables, base_hh, profs
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

    def run_init_hh_parser(self):
        """
        Основной вид задачи для запуска парсера для вакансий HeadHunter
        """
        log.info('Запуск парсера HabrCareer')
        try:
            parser = HHJobParser(base_hh, profs, log, conn, table_name)
            parser.find_vacancies()
            parser.addapt_numpy_null()
            parser.save_df()
            log.info('Парсер HabrCareer успешно провел работу')
            self.df = parser.df
        except Exception as e:
            log.error(f'Ошибка во время работы парсера HeadHunter: {e}')

    def run_update_hh(self):
        """
        Основной вид задачи для запуска парсера для вакансий HeadHunter
        """
        log.info('Запуск парсера HabrCareer')
        try:
            parser = HHJobParser(base_hh, profs, log, conn, table_name)
            parser.find_vacancies()
            parser.generating_dataframes()
            parser.addapt_numpy_null()
            parser.update_database_queries()
            self.dataframe_to_update = parser.dataframe_to_update
            self.dataframe_to_closed = parser.dataframe_to_closed
        except Exception as e:
            log.error(f'Ошибка во время работы парсера HeadHunter: {e}')


def init_call_all_func():
    worker = Dags()
    worker.run_init_hh_parser()
    worker.update_dicts()
    worker.model(worker.df)
    worker.dml_core_init(worker.dfs)

def update_call_all_func():
    worker = Dags()
    worker.run_update_hh()
    worker.update_dicts()
    worker.model(worker.dataframe_to_update)
    worker.dml_core_update_and_archivate(worker.dfs, worker.dataframe_to_closed)


with DAG(
        dag_id="init_hh_parser",
        schedule_interval=None, tags=['admin_1T'],
        default_args=default_args,
        catchup=False
) as init_hh_dag:

    parse_hh_match_jobs = PythonOperator(
        task_id='init_hh_task',
        python_callable=init_call_all_func,
        provide_context=True
    )


with DAG(
        dag_id="update_hh_parser",
        schedule_interval=None, tags=['admin_1T'],
        default_args=default_args,
        catchup=False
) as hh_update_dag:

    parse_delta_hh_jobs = PythonOperator(
        task_id='update_hh_task',
        python_callable=update_call_all_func,
        provide_context=True
    )
