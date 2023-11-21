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
from selenium.common.exceptions import NoSuchElementException
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

raw_tables = ['raw_habr']

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
        table_name = 'raw_habr'
        try:
            drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
            self.cur.execute(drop_table_query)
            self.log.info(f'Удалена таблица {table_name}')

            create_table_query = f"""                
            CREATE TABLE {table_name}(
               vacancy_id VARCHAR(2083) NOT NULL,
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
               date_of_download DATE NOT NULL, 
               status VARCHAR(32),
               date_closed DATE,
               version_vac INTEGER NOT NULL,
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
   


class HabrJobParser(BaseJobParser):

    def __init__(self, conn, log):
        super().__init__(conn, log)
        self.items = []
        
    def find_vacancies(self):
        BASE_URL = "https://career.habr.com/vacancies"
        HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0",}
        
        #Значения - грейды/ опыт
        qid_values = [1, 3, 4, 5, 6]
        
        #Поиск у Хабр карьеры работает плохо, поэтому для поиска специализаций использовался внутренний фильтр Хабр.Карьеры, который классно работает 
        # и не подает в разделе для Дата инженеров вакансии с инженерами
        s_values = [41, 42, 43, 95, 34, 3, 2, 4, 82, 23, 12, 10, 76, 44, 22]
        s_value_descriptions = {
        41: "Системный аналитик",
        42: "Бизнес аналитик",
        43: "Аналитик данных",
        95: "Менеджер проектов",
        34: "Менеджер продуктов",
        3: "Фронт-разработчик",
        2: "Бэк-разработчик",        
        4: "Full-stack разработчик",
        82: "Веб дизайнер",
        23: "UI/UX специалист",
        12: "Инженер по тестированию",
        10: "Инженер по автоматизированному тестированию",      
        76: "Data инженер", 
        44: "Data scientist",
        22: "DevOps инженер"}
        self.items = []
        self.log.info("Создан пустой список")
        self.log.info(f"Парсим вакансии")
        
        # Для разных комбинаций грейда и специализации ищутся страницы, вакансии и данные вакансии
        for qid in qid_values:
            
          for s_value in s_values:
           url = f"{BASE_URL}?qid={qid}&s[]={s_value}&type=all"
           r = requests.get(url, headers=HEADERS)
           html = r.text if r.status_code == 200 else None
           if html:
            soup = BeautifulSoup(html, "html.parser")
            pagination = soup.find("div", class_="pagination")
            if pagination:
                pages = pagination.find_all("a")
                if pages:
                    last_page = pages[-2].text
                    total_pages = int(last_page)
                else:
                    total_pages = 1
            else:
                total_pages = 1
            print(f"Found {total_pages} pages for qid={qid}, s[]={s_value}")
            
            for page in range(1, total_pages + 1):
                url = f"{BASE_URL}?qid={qid}&s[]={s_value}&type=all&page={page}"
                r = requests.get(url, headers=HEADERS)
                html = r.text if r.status_code == 200 else None
                if html:
                    soup = BeautifulSoup(html, "html.parser")
                    vacancy_cards = soup.find_all("div", class_="vacancy-card")
                    
                    for card in vacancy_cards:
                       # На хабр.карьере в зарплате передается строка "от N до N валюта", которую нужно распарсить            
                        salary_find = card.find("div", class_="basic-salary").text.strip()
                        salary_from = salary_to = None  # Инициализация переменных

                        if '₽' in salary_find:
                            # Используем регулярное выражение для поиска чисел в строке
                            salary_find = salary_find.replace(' ', '')  # Удаление пробелов
                            if 'от' in salary_find and 'до' in salary_find:
                                match = re.search(r'от(\d+)до(\d+)', salary_find)
                                if match:
                                    salary_from = int(match.group(1))
                                    salary_to = int(match.group(2))

                            elif 'от' in salary_find:
                                match = re.search(r'от(\d+)', salary_find)
                                if match:
                                    salary_from = int(match.group(1))

                            elif 'до' in salary_find:
                                match = re.search(r'до(\d+)', salary_find)
                                if match:
                                    salary_to = int(match.group(1))
                              
                       # Парсим описание вакансии    
                        description_url = "https://career.habr.com" + card.find("a", class_="vacancy-card__title-link").get("href")
                        description_html = requests.get(description_url, headers=HEADERS).text
                        description_soup = BeautifulSoup(description_html, 'lxml')
                        description_text = description_soup.find("div", class_="vacancy-description__text")
                        description = ' '.join(description_text.stripped_strings) if description_text else ""
                        date_created=datetime.now().date()
                        date_of_download=datetime.now().date()
                        status ='existing'
                        version_vac=1
                        actual=1
                       # Значения грейдов
                        if qid==1:
                            level='Intern'
                        elif qid==3:
                            level='Junior'
                        elif qid==4:
                            level='Middle'    
                        elif qid==5:
                            level='Senior'    
                        else:
                            level='Lead'
                       # Создаем список со спаршенными данными по каждой вакансии     
                        item = {
                            "company": card.find("div", class_="vacancy-card__company-title").get_text(strip=True),
                            "vacancy_name": card.find("a", class_="vacancy-card__title-link").get_text(strip=False),
                            "skills": card.find("div", class_="vacancy-card__skills").get_text(strip=False),
                            "towns": card.find("div", class_="vacancy-card__meta").get_text(strip=False),
                            "vacancy_id": "https://career.habr.com/" + card.find("a", class_="vacancy-card__title-link").get("href"),
                            "description": description,
                            "date_created": date_created,
                            "date_of_download": date_of_download,
                            "source_vac": BASE_URL,
                            "status": status,
                            "version_vac": version_vac,
                            "actual": actual,
                            "level": level,
                            "salary_from": salary_from,
                            "salary_to": salary_to}            
                        print(f"Adding item: {item}")
                        self.items.append(item)
        self.log.info("В список добавлены данные")
        return self.items
        pass


    def save_df(self, cursor, connection):
        self.log.info(f"Запрос вставки данных")
        try:
            for item in self.items:
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

            # SQL-запрос для вставки данных
                sql = """INSERT INTO public.raw_habr (vacancy_id, company, vacancy_name, skills, towns, description, date_created,
                                                 date_of_download, source_vac, status, version_vac, actual, level, salary_from, salary_to)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
                values = (link_vacancy, company, vacancy_title, skills, meta, description, date_created, 
                      date_of_download, source_vac, status, version_vac, actual, level, salary_from, salary_to)
                cursor.execute(sql, values)

            connection.commit()
            cursor.close()
            connection.close()
            print("Data successfully inserted into the database.")
            self.log.info(f"Данные успешно добавлены")
            print(f"Inserted data: {self.items}")
        
        # Clear the items list after successful insertion
            self.items = []

        except Exception as e:
            print(f"Error during data insertion: {e}")
            self.log.info(f"Ошибка добавления данных")
            connection.rollback()
            cursor.close()
            connection.close()

# Создаем объект DatabaseManager
db_manager = DatabaseManager(conn=conn)

# Создаем объект HabrJobParser
habr_parser = HabrJobParser(conn=conn, log=log)

def init_run_habr_parser(**context):
    log.info('Запуск парсера Хабр. Карьера')
    parser = HabrJobParser(conn, log)
    items = parser.find_vacancies()
    log.info('Парсер Хабр. Карьера успешно провел работу')
    parser.save_df(cursor=conn.cursor(), connection=conn)


with DAG(dag_id = "parse_habrjobs", schedule_interval = None, tags=['admin_1T'],
    default_args = default_args, catchup = False) as dag:


# Определение задачи
        create_raw_tables = PythonOperator(
            task_id='create_raw_tables',
            python_callable=db_manager.create_raw_tables,
            provide_context=True
)

        parse_habrjobs = PythonOperator(
            task_id='parse_habrjobs',
            python_callable=init_run_habr_parser,
             provide_context=True)

create_raw_tables >> parse_habrjobs
