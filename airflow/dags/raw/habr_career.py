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
import logging as log
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

from raw.connect_settings import conn
from raw.variables_settings import variables, base_habr
from raw.base_job_parser import BaseJobParser

table_name = variables['raw_tables'][5]['raw_tables_name']

# Параметры по умолчанию
default_args = {
    "owner": "admin_1T",
    'start_date': days_ago(1)
}

class HabrJobParser(BaseJobParser):
    def find_vacancies(self):
        self.items = []
        # BASE_URL = "https://career.habr.com/vacancies"
        HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0",}
        
        #Значения - грейды/ опыт
        qid_values = [1, 3, 4, 5, 6]
        
        #Поиск у Хабр карьеры работает плохо, поэтому для поиска специализаций использовался внутренний фильтр Хабр.Карьеры, который классно работает 
        # и не подает в разделе для Дата инженеров вакансии с инженерами
        # s_values = [41, 42, 43, 95, 34, 3, 2, 4, 82, 23, 12, 10, 76, 44, 22]
        s_values = [41]
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
           url = f"{base_habr}?qid={qid}&s[]={s_value}&type=all"
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
                url = f"{base_habr}?qid={qid}&s[]={s_value}&type=all&page={page}"
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
                        date_of_download=datetime.now().date()
                        date_string = card.find("time", class_="basic-date").text.strip()

# Маппинг месяцев для русского языка
                        months_mapping = {
                                            'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
                                            'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
                                            }

# Разбираем строку и получаем объект datetime
                        date_created = datetime.now().date()
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
                            "vacancy_url": "https://career.habr.com/" + card.find("a", class_="vacancy-card__title-link").get("href"),
                            "description": description,
                            "date_created": date_created,
                            "date_of_download": date_of_download,
                            "source_vac": 2,
                            "status": status,
                            "version_vac": version_vac,
                            "actual": actual,
                            "level": level,
                            "salary_from": salary_from,
                            "salary_to": salary_to}            
                        print(f"Adding item: {item}")
                        self.df = pd.concat([self.df, pd.DataFrame(item, index=[0])], ignore_index=True)
                        time.sleep(3)
        self.df = self.df.drop_duplicates()
        self.log.info("Общее количество найденных вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")


# # Создаем объект HabrJobParser
# def init_run_habr_parser():
#     log.info('Запуск парсера Хабр. Карьера')
#     parser = HabrJobParser(base_habr, log, conn, table_name)
#     parser.find_vacancies()
#     parser.save_df()
#     log.info('Парсер Хабр. Карьера успешно провел работу')
#
#
# with DAG(
#         dag_id = "init_habrcareer_parser",
#         schedule_interval = None,
#         tags=['admin_1T'],
#         default_args = default_args,
#         catchup = False) as habr_dag:
#
#
# # Определение задачи
#
#         parse_habrjobs = PythonOperator(
#                 task_id='init_habrcareer_task',
#                 python_callable=init_run_habr_parser,
#                 provide_context=True
#                 )
#
# parse_habrjobs
