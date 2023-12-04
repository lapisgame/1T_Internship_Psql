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

from raw.vseti_parser import VsetiJobParser, table_name
from variables_settings import variables, base_vseti, profs
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
    """
    Custom class for handling VSeti parser DAGs.

    Inherits from the BaseDags class.

    Methods:
    - run_init_vseti_parser: Runs the initial VSeti parser workflow.
    - run_update_vseti: Runs the update VSeti parser workflow.
    """
    def run_init_vseti_parser(self):
        """
        Runs the initial VSeti parser workflow.

        This method initializes and runs the VsetiJobParser to find and process vacancies from VSeti.
        It handles the parsing, data manipulation, and saving of the parsed data.

        Raises:
        - Exception: If an error occurs during the VSeti parser workflow.
        """
        log.info('Starting VSeti parser')
        try:
            parser = VsetiJobParser(base_vseti, profs, log, conn, table_name)
            parser.find_vacancies()
            parser.calculate_currency_vacancies()
            parser.addapt_numpy_null()
            parser.save_df()
            log.info('VSeti parser successfully completed the job')
            self.df = parser.df
        except Exception as e:
            log.error(f'Error occurred during the VSeti parser workflow: {e}')


    def run_update_vseti(self):
        """
        Runs the update VSeti parser workflow.

        This method initializes and runs the VsetiJobParser to find and process updated vacancies from VSeti.
        It handles the parsing, data manipulation, and updating of the existing database with the updated data.

        Raises:
        - Exception: If an error occurs during the VSeti parser workflow.
        """
        log.info('Starting VSeti parser')
        try:
            parser = VsetiJobParser(base_vseti, profs, log, conn, table_name)
            parser.find_vacancies()
            parser.calculate_currency_vacancies()
            parser.generating_dataframes()
            parser.addapt_numpy_null()
            parser.update_database_queries()
            log.info('VSeti parser successfully completed the job')
            self.dataframe_to_update = parser.dataframe_to_update
            self.dataframe_to_closed = parser.dataframe_to_closed
        except Exception as e:
            log.error(f'Error occurred during the VSeti parser workflow: {e}')


def init_call_all_func():
    worker = Dags()
    worker.run_init_vseti_parser()
    worker.update_dicts()
    worker.model(worker.df)
    worker.dml_core_init(worker.dfs)

def update_call_all_func():
    worker = Dags()
    worker.run_update_vseti()
    worker.update_dicts()
    worker.archiving(worker.dataframe_to_closed)
    worker.model(worker.dataframe_to_update)
    worker.dml_core_update(worker.dfs)


with DAG(
        dag_id="init_vseti_parser",
        schedule_interval=None, tags=['admin_1T'],
        default_args=default_args,
        catchup=False
) as vseti_dag:

    parse_vseti_jobs = PythonOperator(
        task_id='init_vseti_task',
        python_callable=init_call_all_func,
        provide_context=True
    )

with DAG(
        dag_id="update_vseti_parser",
        schedule_interval=None, tags=['admin_1T'],
        default_args=default_args,
        catchup=False
) as vseti_update_dag:

    parse_delta_vseti_jobs = PythonOperator(
        task_id='update_vseti_task',
        python_callable=update_call_all_func,
        provide_context=True
    )

