from airflow.utils.task_group import TaskGroup
import logging
import time
from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.utils.dates import days_ago
import re

import sys
import os
sys.path.insert(0, '/opt/airflow/dags/')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from variables_settings import variables, base_getmatch
from raw.base_job_parser import BaseJobParser

table_name = variables['raw_tables'][6]['raw_tables_name']
BASE_URL = base_getmatch

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

class GetMatchJobParser(BaseJobParser):
    def find_vacancies(self):
        """
        This method parses job vacancies from the GetMatch website.
        It retrieves the vacancy details such as company name, vacancy name, skills required, location, job format,
        salary range, date created, and other relevant information.
        The parsed data is stored in a DataFrame for further processing.
        """
        # i = 1
        # # BASE_URL = 'https://getmatch.ru/api/offers?sa=150000&p={i}&pa=all&s=landing_ca_header&offset=20&limit=20'
        HEADERS = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0",
        }
        self.log.info(f'Создаем пустой список')
        self.items = []
        self.all_links = []
        self.log.info(f'Парсим данные')
        try:
            # Парсим линки вакансий с каждой страницы (на сайте примерно 50страниц, 100 - это с избытком)
            for i in range(1, 100):
                url = BASE_URL.format(i=i)  # Обновляем URL на каждой итерации
                r = requests.get(url)
                if r.status_code == 200:
                    # Парсим JSON-ответ
                    data = r.json()
                    # Извлекаем ссылки из JSON
                    for job in data.get('offers', []):
                        # Получаем дату размещения из JSON
                        date_created = job.get('published_at')
                        # Получаем название вакансии из JSON
                        vacancy_name = job.get('position')
                        # Получаем зарплату из JSON
                        сurr_salary_from = сurr_salary_to = salary_from = salary_to = currency_id = None
                        salary = job.get('salary_description')
                        if salary:
                            salary_text = salary.replace('\u200d', '-').replace('—', '-')
                            salary_parts = list(map(str.strip, salary_text.split('-')))
                            # Если зарплата рублевая
                            if '₽' in salary or '€' in salary or '$' in salary or '₸' in salary:
                                сurr_salary_from = salary_parts[0]
                                if len(salary_parts) == 1:
                                    сurr_salary_to = None if 'от' in salary else salary_parts[0]
                                elif len(salary_parts) > 1:
                                    сurr_salary_to = salary_parts[2]
                            if '€' in salary:
                                currency_id = 'EUR'
                            elif '$' in salary:
                                currency_id = 'USD'
                            elif '₸' in salary:
                                currency_id = 'KZT'
                            elif '₽' in salary:
                                currency_id = 'KZT'
                            # Приводим зарплаты к числовому формату
                            #     if salary_from is not None:
                            #         numbers = re.findall(r'\d+', salary_from)
                            #         combined_number = ''.join(numbers)
                            #         salary_from = int(combined_number) if combined_number else None
                            #     if salary_to is not None:
                            #         numbers = re.findall(r'\d+', salary_to)
                            #         combined_number = ''.join(numbers)
                            #         salary_to = int(combined_number) if combined_number else None
                            if сurr_salary_from is not None:
                                numbers = re.findall(r'\d+', сurr_salary_from)
                                combined_number = ''.join(numbers)
                                сurr_salary_from = int(combined_number) if combined_number else None
                            if сurr_salary_to is not None:
                                numbers = re.findall(r'\d+', сurr_salary_to)
                                combined_number = ''.join(numbers)
                                сurr_salary_to = int(combined_number) if combined_number else None
                        # Получаем описание вакансии из JSON
                        description_text = job.get('offer_description')
                        description = BeautifulSoup(description_text, 'html.parser').get_text()

                        # Получаем ссылку на вакансию из JSON
                        full_url = 'https://getmatch.ru' + str(job.get('url'))

                        # Переходим по ссылке, чтобы спарсить оставшиеся элементы
                        resp = requests.get(full_url, HEADERS)
                        vac = BeautifulSoup(resp.content, 'lxml')
                        try:
                            # Парсим уровень
                            term_element = vac.find('div', {'class': 'col b-term'}, text='Уровень')
                            level = term_element.find_next('div', {
                                'class': 'col b-value'}).text.strip() if term_element else None
                            # Парсим знание языков, если есть
                            lang = None
                            try:
                                lang = vac.find('div', {'class': 'col b-term'}, text='Английский')
                                if lang is not None:
                                    level_lang = lang.find_next('span', {
                                        'class': 'b-language-description d-md-none'}).text.strip()
                                    lang = lang.text.strip()
                                    language = f"{lang}: {level_lang}"
                                    if level == level_lang:
                                        level = None
                                else:
                                    language = None
                            except:
                                pass
                            # Парсим формат работы
                            job_form_classes = ['g-label-linen', 'g-label-zanah', 'ng-star-inserted']
                            job_form = vac.find('span', class_=job_form_classes)
                            job_format = job_form.get_text(strip=True) if job_form is not None else None

                            # Парсим скиллы
                            stack_container = vac.find('div', class_='b-vacancy-stack-container')
                            if stack_container is not None:
                                labels = stack_container.find_all('span', class_='g-label')
                                skills = ', '.join([label.text.strip('][') for label in labels])
                            else:
                                skills = None
                            date_of_download = datetime.now().date()
                        except:
                            pass
                        item = {
                            "company": job.get('company', {}).get('name'),
                            "vacancy_name": vacancy_name,
                            "skills": skills,
                            "towns": ', '.join([span.get_text(strip=True).replace('📍', '') for span in
                                                vac.find_all('span', class_='g-label-secondary')]),
                            "vacancy_url": full_url,
                            "description": description,
                            "job_format": job_format,
                            "level": level,
                            "salary_from": salary_from,
                            "salary_to": salary_to,
                            "date_created": date_created,
                            "date_of_download": date_of_download,
                            "source_vac": 1,
                            "status": 'existing',
                            "version_vac": 1,
                            "actual": 1,
                            "languages": language,
                            "сurr_salary_from": сurr_salary_from,
                            "сurr_salary_to": сurr_salary_to,
                            "currency_id": currency_id
                        }
                        print(f"Adding item: {item}")
                        item_df = pd.DataFrame([item])
                        self.df = pd.concat([self.df, item_df], ignore_index=True)
                        time.sleep(3)
                else:
                    print(f"Failed to fetch data for {url}. Status code: {r.status_code}")
                    continue  # Прерываем цикл при ошибке запроса
                # i += 1
            self.log.info("В список добавлены данные")

        except AttributeError as e:
            self.log.error(f"Error processing link {url}: {e}")

        self.df = self.df.drop_duplicates()
        self.log.info("Total number of found vacancies after removing duplicates: " + str(len(self.df)) + "\n")
