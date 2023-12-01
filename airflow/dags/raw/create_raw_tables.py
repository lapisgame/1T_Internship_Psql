from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.log.logging_mixin import LoggingMixin

import sys
import os
sys.path.insert(0, '/opt/airflow/dags/')
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from raw.connect_settings import conn
from raw.variables_settings import raw_tables, schemes

conn.autocommit = False

class DatabaseManager:
    """
    Class for creating the schema "raw" and tables
    """
    def __init__(self, conn):
        self.conn = conn
        self.cur = conn.cursor()
        self.log = LoggingMixin().log
        self.schema = schemes['raw']
        self.raw_tables = raw_tables

    def create_raw_schema(self):
        try:
            schema_query = f"CREATE SCHEMA IF NOT EXISTS {self.schema}"
            self.cur.execute(schema_query)
            self.log.info(f'Создана схема {self.schema}')
            self.conn.commit()
        except Exception as e:
            self.log.error(f'Ошибка при создании схемы {self.schema}: {e}')
            self.conn.rollback()

    def create_raw_tables(self):
        try:
            for raw_table in self.raw_tables:
                table_name = raw_table['raw_tables_name']
                drop_table_query = f"DROP TABLE IF EXISTS {self.schema}.{table_name};"
                self.cur.execute(drop_table_query)
                self.log.info(f'Удалена таблица {table_name}')
                create_table_query = f"""                
                CREATE TABLE IF NOT EXISTS {self.schema}.{table_name}(
                   vacancy_url VARCHAR(2083) NOT NULL,
                   vacancy_name VARCHAR(255),
                   towns VARCHAR(255),
                   level VARCHAR(255),
                   company VARCHAR(255),
                   salary_from DECIMAL(10, 2),
                   salary_to DECIMAL(10, 2),
                   currency_id VARCHAR(5),
                   сurr_salary_from DECIMAL(10, 2),
                   сurr_salary_to DECIMAL(10, 2),
                   exp_from DECIMAL(2, 1),
                   exp_to DECIMAL(2, 1),
                   description TEXT,
                   job_type VARCHAR(255),
                   job_format VARCHAR(255),
                   languages VARCHAR(255),
                   skills VARCHAR(511),
                   source_vac SMALLINT,
                   date_created DATE,
                   date_of_download DATE NOT NULL, 
                   status VARCHAR(32),
                   date_closed DATE,
                   version_vac INTEGER NOT NULL,
                   actual SMALLINT,
                   PRIMARY KEY(vacancy_url, version_vac)
                );
                """
                self.cur.execute(create_table_query)
                self.log.info(f'Таблица {self.schema}.{table_name} создана в базе данных.')
                self.conn.commit()
        except Exception as e:
            self.log.error(f'Ошибка при создании таблицы {self.schema}.{table_name}: {e}')
            self.conn.rollback()

    def create_currency_directory(self):
        try:
            table_name = 'currency_directory'
            drop_table_query = f"DROP TABLE IF EXISTS {self.schema}.{table_name};"
            self.cur.execute(drop_table_query)
            self.log.info(f'Удалена таблица {table_name}')
            create_table_query = f"""                
            CREATE TABLE IF NOT EXISTS {self.schema}.{table_name}(
            exchange_rate_date DATE NOT NULL,
            usd_rate DECIMAL(4, 4),
            eur_rate DECIMAL(4, 4),
            kzt_rate DECIMAL(4, 4), 
            PRIMARY KEY(exchange_rate_date)
            );
            """
            self.cur.execute(create_table_query)
            self.log.info(f'Таблица {self.schema}.{table_name} создана в базе данных.')
            self.conn.commit()
        except Exception as e:
            self.log.error(f'Ошибка при создании таблицы {self.schema}.{table_name}: {e}')
            self.conn.rollback()
        finally:
            # закрываем курсор и соединение с базой данных
            self.cur.close()
            self.conn.close()

db_manager = DatabaseManager(conn=conn)

# Define the tasks and DAG
start_date = datetime(2023, 12, 2)

default_dag_args = {
    "owner": "admin_1T",
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ddl_raw_tables',
    default_args=default_dag_args,
    start_date=start_date,
    schedule_interval=None,
) as dag:
    create_raw_schema = PythonOperator(
        task_id='create_raw_schema',
        python_callable=db_manager.create_raw_schema,
        provide_context=True,
    )

    create_raw_tables = PythonOperator(
        task_id='create_raw_tables',
        python_callable=db_manager.create_raw_tables,
        provide_context=True,
    )

    create_currency_directory = PythonOperator(
        task_id='create_currency_directory',
        python_callable=db_manager.create_currency_directory,
        provide_context=True,
    )

    end_task = DummyOperator(
        task_id="end_task",
    )

    create_raw_schema >> create_raw_tables >> create_currency_directory >> end_task
