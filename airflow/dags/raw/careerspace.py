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
from airflow.utils.log.logging_mixin import LoggingMixin
from typing import Callable
from airflow.utils.task_group import TaskGroup
import logging
from logging import handlers
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import requests
from bs4 import BeautifulSoup
from airflow.utils.dates import days_ago
import re


import sys
import os
sys.path.insert(0, '/opt/airflow/dags/')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from raw.variables_settings import variables, base_careerspace

table_name = variables['raw_tables'][9]['raw_tables_name']


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
   


class CareerspaceJobParser(BaseJobParser):

    def __init__(self, conn, log):
        super().__init__(conn, log)
        
        
    def find_vacancies(self):
        HEADERS = {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0",}       
        url_template = 'https://careerspace.app/api/v2/jobs/filters?skip={i}&take=8&sortBy=new-desc&functions=8%2C13%2C14%2C9&jobLevel%5B0%5D={o}&currencyCode=RUR' 
        # i - это номер страницы, o - это уровень опыта
        on = ['intern', 'specialist', 'manager', 'head', 'director']
    
        self.log.info(f'Создаем пустой список')
        self.all_items = []
        self.unique_items = []
        self.items = []
        seen_ids = set()
        self.log.info(f'Парсим данные')
        
        # Парсим линки вакансий с каждой страницы    
    
        for o in on:
            page = 1
            # Пока страницы есть
            while True:
                i = page * 8  # 8 - это кол-во вакансий на странице (по-умолчанию)
                url = url_template.format(i=i, o=o)  # Обновляем URL на каждой итерации
                r = requests.get(url)

                # Проверяем успешность запроса
                if r.status_code == 200:
                    # Парсим JSON-ответ
                    data = r.json()
                    # Извлекаем ссылки из JSON
                    for job in data.get('jobs', []):
                        full_url = 'https://careerspace.app/job/' + str(job.get('job_id'))
                        
                        # получаем зарплату из JSON 
                        if job.get('job_salary_currency') == 'RUB':
                            salary_from = job.get('job_salary_from')
                            salary_to = job.get('job_salary_to') 
                        else: 
                            salary_from = salary_to = None
                            
                        # получаем города и страны из JSON 
                        locations = job.get("locations", {})
                        cities_list = locations.get("cities", [])
                        countries_list = locations.get("countries", [])

                        # Проверяем наличие городов в JSON перед итерацией
                        towns = ", ".join(city["name"] for city in cities_list) if isinstance(cities_list, list) else None

                        # Проверяем наличие стран в JSON перед итерацией
                        countries = ", ".join(country["name"] for country in countries_list) if isinstance(countries_list, list) else None

                        # Объединяем города и страны в одну строку через запятую
                        towns = ", ".join(filter(None, [towns, countries]))
                        
                        # Gолучаем формат работы из JSON 
                        formatted_job_format = job.get('job_format')
                        job_format = ", ".join(map(str, formatted_job_format)) if formatted_job_format else None
                        
                        # Gереходим на страницу вакансии и парсим описание вакансии 
                        resp = requests.get(full_url, headers=HEADERS)
                        vac = BeautifulSoup(resp.content, 'lxml')
                        description = vac.find('div', {'class': 'j-d-desc'}).text.strip()
                        
                        date_created = date_of_download = datetime.now().date()   
                        url_source='https://careerspace.app/jobs?isPreFilter=true'
                        
                        # Задаем значения для уровня опыта
                        if o == 'intern':
                            level = 'Менее 1 года'
                        elif o == 'specialist':
                            level = '1-3 года'
                        elif o == 'manager':
                            level = '3-5 лет'    
                        elif o == 'head':
                            level = '5-10 лет'    
                        else:
                            level = 'Более 10 лет'            

                        item = {
                                "company": job.get('company', {}).get('company_name'),
                                "vacancy_name": job.get('job_name'),
                                "vacancy_id": full_url,
                                "job_format": job_format,
                                "salary_from": salary_from,
                                "salary_to": salary_to,
                                "date_created": date_created,
                                "date_of_download": date_of_download,
                                "source_vac": url_source,
                                "status": 'existing',
                                "version_vac": '1',
                                "actual": '1',
                                "description": description,
                                "towns": towns,
                                "level": level,
                                }
                        print(f"Adding item: {item}")
                        self.items.append(item)

                   # Проверяем, есть ли следующая страница
                    if not data.get('jobs'):
                        break  # Если следующая страница пустая, выходим из цикла

                    page += 1  # Переходим на следующую страницу
                else:
                    print(f"Failed to fetch data for {o}. Status code: {r.status_code}")
                    break  # Прерываем цикл при ошибке запрос
            self.all_items.extend(self.items)

        self.log.info("В список добавлены данные")
        return self.all_items


    def save_df(self, cursor, connection):
        self.log.info(f"Inserting data into the database")
        try:
            for item in self.all_items:
                company = item["company"]
                vacancy_title = item["vacancy_name"]
                meta = item["towns"]
                link_vacancy = item["vacancy_id"]
                date_created=item["date_created"]
                date_of_download=item["date_of_download"]
                description=item["description"]
                source_vac=item["source_vac"]
                status=item["status"]
                version_vac=item["version_vac"]
                actual=item["actual"]
                salary_from=item["salary_from"]
                salary_to=item["salary_to"]
                job_format=item["job_format"]
                level=item["level"]

                # SQL-запрос для вставки данных
                sql = """INSERT INTO public.raw_careerspace (vacancy_id, company, vacancy_name,  towns, description, date_created,
                                                 date_of_download, source_vac, status, level, version_vac, actual, salary_from, salary_to, job_format)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (vacancy_id) DO NOTHING;"""
                values = (link_vacancy, company, vacancy_title, meta, description, date_created, 
                      date_of_download, source_vac, status, level, version_vac, actual, salary_from, salary_to, job_format)
                cursor.execute(sql, values)

            connection.commit()
            cursor.close()
            connection.close()
            print("Data successfully inserted into the database.")
            self.log.info("Data successfully inserted")
            print(f"Inserted data: {self.items}")

        except Exception as e:
            print(f"Error during data insertion: {e}")
            self.log.info(f"Ошибка добавления данных")
            connection.rollback()
            cursor.close()
            connection.close()

# Создаем объект DatabaseManager
db_manager = DatabaseManager(conn=conn)

# Создаем объект CareerspaceJobParser
get_match_parser = CareerspaceJobParser(conn=conn, log=log)

def init_run_careerspace_parser(**context):
    log.info('Запуск парсера Get Match')
    parser = CareerspaceJobParser(conn, log)
    items = parser.find_vacancies()
    log.info('Парсер Get Match успешно провел работу')
    parser.save_df(cursor=conn.cursor(), connection=conn)


with DAG(dag_id = "parse_careerspace_jobs", schedule_interval = '@daily', tags=['admin_1T'],
    default_args = default_args, catchup = False) as dag:


# Определение задачи
        create_raw_tables = PythonOperator(
            task_id='create_raw_tables',
            python_callable=db_manager.create_raw_tables,
            provide_context=True
)

        parse_careerspace_jobs = PythonOperator(
            task_id='parse_careerspace_jobs',
            python_callable=init_run_careerspace_parser,
             provide_context=True)

create_raw_tables >> parse_careerspace_jobs