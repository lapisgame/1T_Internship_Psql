import dateparser
import time
from datetime import datetime, timedelta
import logging as log
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import sys
import os
sys.path.insert(0, '/opt/airflow/dags/')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from variables_settings import variables, base_vseti
from raw.base_job_parser import BaseJobParser

table_name = variables['raw_tables'][10]['raw_tables_name']


class VsetiJobParser(BaseJobParser):

    def find_vacancies(self):
        HEADERS = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:98.0) Gecko/20100101 Firefox/98.0", }
        #
        # base_url = 'https://www.vseti.app/jobs?1cd4ffac_page={i}'
        i = 1

        self.log.info(f'Создаем пустой список')
        self.all_items = []
        self.items = []
        self.log.info(f'Парсим данные')

        # Пока есть страницы
        while True:
            url = self.url.format(i=i)
            response = requests.get(url)

            # Проверяем успешность запроса
            if response.status_code == 200:
                # Создаем объект BeautifulSoup для парсинга HTML-кода
                soup = BeautifulSoup(response.content, 'html.parser')
                vacancy_cards = soup.find_all("div", class_="collection-item w-dyn-item")

                if not vacancy_cards:
                    # Если нет карточек вакансий, завершаем цикл
                    break

                for card in vacancy_cards:
                    # Парсим инфо о вакансии
                    vacancy_url = card.find('a').get('href')
                    response_vacancy = requests.get(vacancy_url, )

                    if response_vacancy.status_code == 200:
                        soup_vacancy = BeautifulSoup(response_vacancy.content, 'html.parser')

                        # Парсим наименование работодателя
                        employer_info = soup_vacancy.find('div', class_='div-block-230')
                        if employer_info:
                            employer_name = employer_info.find('p', class_='paragraph-23').text.strip()

                        # Парсим данные из заголовка
                        vacancy_info = soup_vacancy.find('div', class_='header_vacancy_div')
                        if vacancy_info:
                            # наименование вакансии
                            job_title = vacancy_info.find('h1', class_='heading-9').text.strip()
                            # зарплата
                            сurr_salary_from = сurr_salary_to = salary_from = salary_to = currency_id = None
                            salary = vacancy_info.find('p', class_='paragraph-22').text.strip()
                            if salary:
                                salary_find = salary.replace('\u200d', '-').replace('—', '-')
                                salary_find = salary_find.replace(' ', '')

                                if ('₽' or '€' or '$' or '₸') in salary_find:
                                    if 'от' in salary_find and 'до' in salary_find:
                                        match = re.search(r'от(\d+)до(\d+)', salary_find)
                                        if match:
                                            сurr_salary_from = int(match.group(1)) if int(match.group(1)) < \
                                                                                      99999999 else None
                                            сurr_salary_to = int(match.group(2)) if int(match.group(2)) < \
                                                                                    99999999 else None
                                    elif 'от' in salary_find:
                                        match = re.search(r'от(\d+)', salary_find)
                                        if match:
                                            сurr_salary_from = int(match.group(1)) if int(match.group(1)) < \
                                                                                      99999999 else None
                                    elif 'до' in salary_find:
                                        match = re.search(r'до(\d+)', salary_find)
                                        if match:
                                            сurr_salary_to = int(match.group(1)) if int(match.group(1)) < \
                                                                                    99999999 else None

                                else:
                                    self.log.info(f"A new currency has been found: "
                                                  f"{salary_find}")
                                    currency_id = salary_find
                                    сurr_salary_from = None
                                    сurr_salary_to = None

                                if сurr_salary_from is not None or сurr_salary_to is not None:
                                    if '₽' in salary:
                                        currency_id = 'RUR'
                                    elif '€' in salary:
                                        currency_id = 'EUR'
                                    elif '$' in salary:
                                        currency_id = 'USD'
                                    elif '₸' in salary:
                                        currency_id = 'KZT'

                            # города и формат работы
                            towns_info = vacancy_info.find('div', class_='margin-16')
                            towns_text = towns_info.find_next('p',
                                                              class_='paragraph-21').text.strip() if towns_info else None
                            towns_list = towns_text.split(', ') if towns_text else []
                            locations = ['можно удаленно', 'удаленно', 'удалённо', 'гибрид', 'офис', 'парт-тайм',
                                         'можно удалённо', 'удалённо (рф)', 'удаленно (рф)', 'удаленно (европа)',
                                         'удаленно (снг)',
                                         'удаленно (можно из любой точки мира)', 'удалённо (можно из любой точки мира)',
                                         'удалённо (европа)', 'удалённо (снг)', 'удалённо или гибрид',
                                         'офис или удалённо (любая точка мира)',
                                         'офис или удалённо', 'офис или удаленно (любая точка мира)',
                                         'офис или удаленно', 'офис/гибрид',
                                         'гибрид или полностью удалённо', 'удаленно (из любой точки мира)',
                                         'удалённо (мск +2)', 'офис и удалённо (рф)',
                                         'гибрид или удалённо (рф)', 'удаленно (по всему миру)']

                            towns = [location for location in towns_list if location.lower() not in locations]
                            towns = ', '.join(towns)

                            job_format = [location.capitalize() for location in towns_list if
                                          location.lower() in locations]
                            job_format = ', '.join(job_format)

                            # грейд
                            level_info = vacancy_info.find_all('div', class_='div-block-54')
                            level_values = []
                            for level_elem in level_info:
                                levels = level_elem.find_all('p', class_='paragraph-9')
                                level_values.extend(level.text.strip() for level in levels)
                            level_value = (val.capitalize() for val in level_values if
                                           val in ['СТАЖЕР', 'ДЖУН', 'МИДДЛ', 'СЕНЬОР', 'ЛИД'])
                            level = ', '.join(level_value)

                            # описание вакансии
                            richtext_block = soup_vacancy.find("div", class_="rich-text-block w-richtext")
                            paragraphs = richtext_block.find_all('p')
                            description = '\n'.join(paragraph.get_text(strip=True) for paragraph in paragraphs)

                            # другие параметры
                            date_str = vacancy_info.find('div', class_='div-block-234').text.strip()
                            # date_created = datetime.strptime(date_str, '%d%B%Y').strftime('%d %B %Y')
                            date_created = datetime.strptime(date_str, '%d%B%Y').strftime('%Y-%m-%d')

                            date_of_download = datetime.now().date()

                            item = {
                                "company": employer_name,
                                "vacancy_name": job_title,
                                "vacancy_url": vacancy_url,
                                "job_format": job_format,
                                "salary_from": salary_from,
                                "salary_to": salary_to,
                                "сurr_salary_from": сurr_salary_from,
                                "сurr_salary_to": сurr_salary_to,
                                "currency_id": currency_id,
                                "date_created": date_created,
                                "date_of_download": date_of_download,
                                "source_vac": '11',
                                "status": 'existing',
                                "version_vac": '1',
                                "actual": '1',
                                "description": description,
                                "level": level,
                                "towns": towns,
                            }

                            print(f"Adding item: {item}")
                            self.df = pd.concat([self.df, pd.DataFrame(item, index=[0])], ignore_index=True)
                            time.sleep(3)

                        else:
                            print(
                                f'Ошибка при загрузке страницы вакансии {vacancy_url}: {response_vacancy.status_code}')
            else:
                print(f'Ошибка при загрузке страницы {url}: {response.status_code}')

            i += 1  # Переходим на следующую страницу

        self.df = self.df.drop_duplicates()
        # self.df.to_csv('/opt/airflow/files/vseti.csv', index=False)
        self.log.info("Общее количество найденных вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")
