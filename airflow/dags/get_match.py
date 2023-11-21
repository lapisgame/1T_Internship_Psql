import dateparser
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv, json
import psycopg2
from airflow import settings
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs
from airflow import settings
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from typing import Callable
from airflow.utils.task_group import TaskGroup
import logging
from logging import handlers
from airflow import models
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import requests
from bs4 import BeautifulSoup
from airflow.utils.dates import days_ago
import re


# Connections settings
# Загружаем данные подключений из JSON файла
with open('/opt/airflow/dags/config_connections.json', 'r') as conn_file:
    connections_config = json.load(conn_file)

# Получаем данные конфигурации подключения и создаем конфиг для клиента
conn_config = connections_config['psql_connect']

config = {
    'database': conn_config['database'],
    'user': conn_config['user'],
    'password': conn_config['password'],
    'host': conn_config['host'],
    'port': conn_config['port']
}

conn = psycopg2.connect(**config)

raw_tables = ['raw_get_match']

# Параметры по умолчанию
default_args = {
    "owner": "admin_1T",
    'start_date': days_ago(1)
}


logging_level = os.environ.get('LOGGING_LEVEL', 'DEBUG').upper()
logging.basicConfig(level=logging_level)
log = logging.getLogger(__name__)
log_handler = handlers.RotatingFileHandler('/opt/airflow/logs/airflow.log',
                                           maxBytes=5000,
                                           backupCount=5)

log_handler.setLevel(logging.DEBUG)
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_handler.setFormatter(log_formatter)
log.addHandler(log_handler)

class DatabaseManager:
    def __init__(self, conn):
        self.conn = conn
        self.cur = conn.cursor()
        self.raw_tables = raw_tables
        self.log = LoggingMixin().log

    def create_raw_tables(self):     
        table_name = 'raw_get_match'
        try:
            drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
            self.cur.execute(drop_table_query)
            self.log.info(f'Удалена таблица {table_name}')

            create_table_query = f"""                
            CREATE TABLE {table_name}(
               vacancy_id VARCHAR(2083),
               vacancy_name VARCHAR(255),
               towns VARCHAR(255),
               level VARCHAR(255),
               company VARCHAR(255),
               salary_from BIGINT,
               salary_to BIGINT,
               exp_from SMALLINT,
               exp_to SMALLINT,
               description TEXT,
               job_type VARCHAR(255),
               job_format VARCHAR(255),
               languages VARCHAR(255),
               skills VARCHAR(511),
               source_vac VARCHAR(255),
               date_created DATE,
               date_of_download DATE, 
               status VARCHAR(32),
               date_closed DATE,
               version_vac INTEGER,
               actual SMALLINT,
               PRIMARY KEY(vacancy_id)
            );
            """
            self.cur.execute(create_table_query)
            self.log.info(f'Таблица {table_name} создана в базе данных.')
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            self.log.error(f'Ошибка при создании таблицы {table_name}: {e}')



class BaseJobParser:
    def __init__(self, conn, log):
        self.conn = conn
        self.log = log

    def find_vacancies(self, conn):
        """
        Метод для парсинга вакансий, должен быть переопределен в наследниках
        """
        raise NotImplementedError("Вы должны определить метод find_vacancies")

    def save_df(self):
        """
        Метод для сохранения данных из pandas DataFrame
        """
        raise NotImplementedError("Вы должны определить метод save_df")
   


class GetMatchJobParser(BaseJobParser):

    def __init__(self, conn, log):
        super().__init__(conn, log)
        self.items = []
        
    def find_vacancies(self):
        BASE_URL = 'https://getmatch.ru/vacancies?p=1&sa=150000&pa=all&s=landing_ca_header'
        url = 'https://getmatch.ru/vacancies?p={i}&sa=150000&pa=all&s=landing_ca_header'
        HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0",
}
        self.log.info(f'Создаем пустой список')
        self.items = []
        self.unique_items = []
        seen_ids = set()
        self.all_links = []  
        self.log.info(f'Парсим данные')
        # Парсим линки вакансий с каждой страницы (на сайте примерно 50страниц, 100 - это с избытком)     
        
        for i in range(1, 100):
            response = requests.get(url.format(i=i), headers=HEADERS)
            soup = BeautifulSoup(response.content, 'html.parser')
            divs = soup.find_all('div', class_='b-vacancy-card-title')
            for div in divs:
                vacancy_url = 'https://getmatch.ru/' + div.find('a').get('href')
                self.all_links.append(vacancy_url)
    
        for link in self.all_links:                
            resp = requests.get(link, HEADERS)
            vac = BeautifulSoup(resp.content, 'lxml')

            try:
                # парсим грейд вакансии 
                term_element = vac.find('div', {'class': 'col b-term'}, text='Уровень')                        
                level = term_element.find_next('div', {'class': 'col b-value'}).text.strip() if term_element else None

                # Парсим иностранные языки
                lang = vac.find('div', {'class': 'col b-term'}, text='Английский')                        
                if lang is not None:                           
                    level_lang= lang.find_next('span', {'class': 'b-language-description d-md-none'}).text.strip()
                    lang=lang.text.strip()
                    language = f"{lang}: {level_lang}"
                    if level==level_lang:
                        level=None
                else:
                    language=None
                            
                # Парсим навыки
                stack_container = vac.find('div', class_='b-vacancy-stack-container')
                if stack_container is not None:
                    labels = stack_container.find_all('span', class_='g-label')
                    page_stacks = ', '.join([label.text.strip('][') for label in labels])
                else:
                    page_stacks=None
                    
                # Парсим описание вакансий
                description_element = vac.find('section', class_='b-vacancy-description')
                description_lines = description_element.stripped_strings
                description = '\n'.join(description_lines)
                
                # Парсим и распаршиваем зарплаты
                salary_text = vac.find('h3').text.strip()
                salary_text = salary_text.replace('\u200d', '-').replace('—', '-')
                if '₽/мес на руки' in vac.find('h3').text:
                    salary_parts = list(map(str.strip, salary_text.split('-')))
                    salary_from = salary_parts[0]
                    if len(salary_parts) == 1:
                        salary_to = None if 'от' in vac.find('h3').text else salary_parts[0]
                    elif len(salary_parts) > 1:
                        salary_to = salary_parts[2]
                else:
                    salary_from = None
                    salary_to = None
                    
                # Приводим зарплаты к числовому формату 
                if salary_from is not None:
                    numbers = re.findall(r'\d+', salary_from)
                    combined_number = ''.join(numbers)
                    salary_from = int(combined_number) if combined_number else None
                if salary_to is not None:
                    numbers = re.findall(r'\d+', salary_to)
                    combined_number = ''.join(numbers)
                    salary_to = int(combined_number) if combined_number else None 
                           
                # Парсим формат работы
                job_form_classes = ['g-label-linen', 'g-label-zanah', 'ng-star-inserted']
                job_form = vac.find('span', class_=job_form_classes)
                job_format = job_form.get_text(strip=True) if job_form is not None else None
                
                # Получаем другие переменные 
                date_created = date_of_download = datetime.now().date()
                status ='existing'
                version_vac=1
                actual=1
                                         
                item = {
                    "company": vac.find('h2').text.replace('\xa0', ' ').strip('в'),
                    "vacancy_name": vac.find('h1').text,
                    "skills": page_stacks,
                    "towns": ', '.join([span.get_text(strip=True).replace('📍', '') for span in vac.find_all('span', class_='g-label-secondary')]),
                    "vacancy_id": link,
                    "description": description,
                    "job_format": job_format,
                    "level": level,
                    "salary_from": salary_from,
                    "salary_to": salary_to,
                    "date_created": date_created,
                    "date_of_download": date_of_download,
                    "source_vac": BASE_URL,
                    "status": status,
                    "version_vac": version_vac,
                    "actual": actual,
                    "languages":language,
        }

                print(f"Adding item: {item}")
                self.items.append(item)
                time.sleep(3)

            except AttributeError as e:
                        print(f"Error processing link {link}: {e}")
        # Удаляем дубликаты
        for item in self.items:
            vacancy_id = item["vacancy_id"]
            if vacancy_id not in seen_ids:
                seen_ids.add(vacancy_id)
                self.unique_items.append(item)

        self.log.info("В список добавлены данные")
        return self.unique_items


    def save_df(self, cursor, connection):
        self.log.info(f"Запрос вставки данных")
        try:
            for item in self.unique_items:
                company = item["company"]
                vacancy_title = item["vacancy_name"]
                skills = item["skills"]
                meta = item["towns"]
                link_vacancy = item["vacancy_id"]
                date_created=item["date_created"]
                date_of_download=item["date_of_download"]
                description=item["description"]
                source_vac=item["source_vac"]
                status=item["status"]
                version_vac=item["version_vac"]
                actual=item["actual"]
                level=item["level"]
                salary_from=item["salary_from"]
                salary_to=item["salary_to"]
                job_format=item["job_format"]
                language= item["languages"]

            # SQL-запрос для вставки данных
                sql = """INSERT INTO public.raw_get_match (vacancy_id, company, vacancy_name, skills, towns, description, date_created,
                                                 date_of_download, source_vac, status, version_vac, actual, level, salary_from, salary_to, job_format, languages)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
                values = (link_vacancy, company, vacancy_title, skills, meta, description, date_created, 
                      date_of_download, source_vac, status, version_vac, actual, level, salary_from, salary_to, job_format, language)
                cursor.execute(sql, values)

            connection.commit()
            cursor.close()
            connection.close()
            print("Data successfully inserted into the database.")
            self.log.info(f"Данные успешно добавлены")
            print(f"Inserted data: {self.items}")
        
            self.items = []

        except Exception as e:
            print(f"Error during data insertion: {e}")
            self.log.info(f"Ошибка добавления данных")
            connection.rollback()
            cursor.close()
            connection.close()

# Создаем объект DatabaseManager
db_manager = DatabaseManager(conn=conn)

# Создаем объект GetMatchJobParser
get_match_parser = GetMatchJobParser(conn=conn, log=log)

def init_run_get_match_parser(**context):
    log.info('Запуск парсера Get Match')
    parser = GetMatchJobParser(conn, log)
    items = parser.find_vacancies()
    log.info('Парсер Get Match успешно провел работу')
    parser.save_df(cursor=conn.cursor(), connection=conn)


with DAG(dag_id = "parse_get_match_jobs", schedule_interval = None, tags=['admin_1T'],
    default_args = default_args, catchup = False) as dag:


# Определение задачи
        create_raw_tables = PythonOperator(
            task_id='create_raw_tables',
            python_callable=db_manager.create_raw_tables,
            provide_context=True
)

        parse_get_match_jobs = PythonOperator(
            task_id='parse_get_match_jobs',
            python_callable=init_run_get_match_parser,
             provide_context=True)

create_raw_tables >> parse_get_match_jobs