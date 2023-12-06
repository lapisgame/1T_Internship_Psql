from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
import logging
from airflow.utils.task_group import TaskGroup
import logging
import time
from datetime import datetime
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.utils.dates import days_ago
from psycopg2.extras import execute_values
import re

import sys
import os
sys.path.insert(0, '/opt/airflow/dags/')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from variables_settings import variables, base_getmatch
from raw.base_job_parser import BaseJobParser

table_name = variables['raw_tables'][6]['raw_tables_name']
url = base_getmatch

logging.basicConfig(
    format='%(threadName)s %(name)s %(levelname)s: %(message)s',
    level=logging.INFO
)

log = logging


class GetMatchJobParser(BaseJobParser):
    def find_vacancies(self):
        self.items = []
        HEADERS = {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0",
                }
        self.log.info(f'Создаем пустой список')
        self.items = []
        # seen_ids = set()
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

        # vacancy_count = 0
        for link in self.all_links:
            # if vacancy_count < 5:
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
                currency_symbols = {'₽': 'RUR', '€': 'EUR', '$': 'USD', '₸': 'KZT'}

                if any(symbol in vac.find('h3').text for symbol in currency_symbols):
                    currency_id = next(
                        (currency_symbols[symbol] for symbol in currency_symbols if symbol in vac.find('h3').text),
                        None)

                    salary_parts = list(map(str.strip, salary_text.split('-')))
                    curr_salary_from = salary_parts[0]

                    if len(salary_parts) == 1:
                        curr_salary_to = None if 'от' in vac.find('h3').text else salary_parts[0]
                    elif len(salary_parts) > 1:
                        curr_salary_to = salary_parts[2]

                # Приводим зарплаты к числовому формату
                if curr_salary_from is not None:
                    numbers = re.findall(r'\d+', curr_salary_from)
                    combined_number = ''.join(numbers)
                    curr_salary_from = int(combined_number) if combined_number else None
                if curr_salary_to is not None:
                    numbers = re.findall(r'\d+', curr_salary_to)
                    combined_number = ''.join(numbers)
                    curr_salary_to = int(combined_number) if combined_number else None

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
                    "towns": ', '.join([span.get_text(strip=True).replace('📍', '') for span in
                                        vac.find_all('span', class_='g-label-secondary')]),
                    "vacancy_url": link,
                    "description": description,
                    "job_format": job_format,
                    "level": level,
                    "currancy_id": currency_id,
                    "curr_salary_from": curr_salary_from,
                    "curr_salary_to": curr_salary_to,
                    "date_created": date_created,
                    "date_of_download": date_of_download,
                    "source_vac": 1,
                    "status": status,
                    "version_vac": version_vac,
                    "actual": actual,
                    "languages":language,
                    }
                print(f"Page: {link}: Adding item: {item}")
                item_df = pd.DataFrame([item])
                self.df = pd.concat([self.df, item_df], ignore_index=True)
                time.sleep(3)
                # vacancy_count += 1
            except AttributeError as e:
                print(f"Error processing link {link}: {e}")
            # else:
            #     break
        self.df = self.df.drop_duplicates()
        self.log.info("Общее количество найденных вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")
