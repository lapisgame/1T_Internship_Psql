from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
import logging
from logging import handlers
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import time

import sys
import os
sys.path.insert(0, '/opt/airflow/dags/')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from raw.connect_settings import conn
from raw.variables_settings import variables, base_careerspace
from raw.base_job_parser import BaseJobParser

table_name = variables['raw_tables'][9]['raw_tables_name']

logging.basicConfig(
    format='%(threadName)s %(name)s %(levelname)s: %(message)s',
    level=logging.INFO
)

log = logging

# Параметры по умолчанию
default_args = {
    "owner": "admin_1T",
    'start_date': days_ago(1)
}

class CareerspaceJobParser(BaseJobParser):
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
                url = base_careerspace.format(i=i, o=o)  # Обновляем URL на каждой итерации
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
                                "vacancy_url": full_url,
                                "job_format": job_format,
                                "salary_from": salary_from,
                                "salary_to": salary_to,
                                "date_created": date_created,
                                "date_of_download": date_of_download,
                                "source_vac": 3,
                                "status": 'existing',
                                "version_vac": '1',
                                "actual": '1',
                                "description": description,
                                "towns": towns,
                                "level": level,
                                }
                        print(f"Adding item: {item}")
                        self.df = pd.concat([self.df, pd.DataFrame(item, index=[0])], ignore_index=True)
                        time.sleep(3)

                   # Проверяем, есть ли следующая страница
                    if not data.get('jobs'):
                        break  # Если следующая страница пустая, выходим из цикла

                    page += 1  # Переходим на следующую страницу
                else:
                    print(f"Failed to fetch data for {o}. Status code: {r.status_code}")
                    break  # Прерываем цикл при ошибке запрос
            self.all_items.extend(self.items)

        self.df = self.df.drop_duplicates()
        self.log.info("Общее количество найденных вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")


# # Создаем объект CareerspaceJobParser
# get_match_parser = CareerspaceJobParser(conn=conn, log=log)
#
# def init_run_careerspace_parser():
#     log.info('Запуск парсера Careerspace')
#     parser = CareerspaceJobParser(conn, log)
#     items = parser.find_vacancies()
#     log.info('Парсер Get Match успешно провел работу')
#     parser.save_df(cursor=conn.cursor(), connection=conn)
#
#
# with DAG(dag_id = "parse_careerspace_jobs", schedule_interval = '@daily', tags=['admin_1T'],
#     default_args = default_args, catchup = False) as dag:
#
#
#         parse_careerspace_jobs = PythonOperator(
#             task_id='parse_careerspace_jobs',
#             python_callable=init_run_careerspace_parser,
#              provide_context=True)
#
#
# parse_careerspace_jobs