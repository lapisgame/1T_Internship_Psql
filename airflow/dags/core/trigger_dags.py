from connect_settings import conn
conn.autocommit = False
from core.trigger_create import TriggerCreator
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


def trigger_core():
    manager = TriggerCreator(conn)
    manager.create_meta_update_trigger()


with DAG(
        dag_id="meta_trigger_update",
        schedule_interval=None,
        tags=['admin_1T'],
        default_args=default_args,
        catchup=False
) as dag_trigger_core:
    create_core_tables = PythonOperator(
        task_id='create_meta_trigger',
        python_callable=trigger_core,
        provide_context=True
    )
