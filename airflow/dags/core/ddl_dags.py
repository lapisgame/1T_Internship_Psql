from connect_settings import conn, engine

conn.autocommit = False
from core.ddl_core import DatabaseManager
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

default_args = {
    "owner": "admin_1T",
    'start_date': datetime(2023, 11, 26),
    'retry_delay': timedelta(minutes=5),
}


def ddl_core():
    manager = DatabaseManager(conn)
    manager.db_creator()


with DAG(
        dag_id="ddl_core_tables",
        schedule_interval=None,
        tags=['admin_1T'],
        default_args=default_args,
        catchup=False
) as dag_ddl_core:
    create_core_tables = PythonOperator(
        task_id='create_core_tables',
        python_callable=ddl_core,
        provide_context=True
    )

