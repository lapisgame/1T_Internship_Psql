from raw.base_job_parser import BaseJobParser
from variables_settings import variables, base_zarplata, profs

import re
import pandas as pd
import time
import requests
from datetime import date, datetime
from airflow.utils.dates import days_ago
import logging

import sys
import os
sys.path.insert(0, '/opt/airflow/dags/')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

table_name = variables['raw_tables'][7]['raw_tables_name']
url = base_zarplata

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

class HHJobParser(BaseJobParser):
    def find_vacancies(self):
        currencies = {}
        dictionaries = requests.get('https://api.hh.ru/dictionaries').json()
        for currency in dictionaries['currency']:
            currencies[currency['code']] = (1/currency['rate'])

        self.max_page_count = 10
        self.re_html_tag_remove = r'<[^>]+>'

        for index, vac_name in enumerate(self.profs):
            for page_number in range(self.max_page_count):
                params = {
                    'text': f'{vac_name}',
                    'page': page_number,
                    'per_page': 20,
                    'area': '113',
                    'negotiations_order': 'updated_at',
                    'vacancy_search_order': 'publication_time'
                }

                try:
                    self.log.info(f'get 1.{page_number} {index}/{len(self.profs)} - {vac_name}')
                    req = requests.get(f'{base_zarplata}', params=params).json()

                    if 'items' in req.keys():
                        for item in req['items']:
                            item = requests.get(f'{base_zarplata}/{item["id"]}').json()
                            res = {}
                            try:
                                res['vacancy_url'] = f'https://www.zarplata.ru/vacancy/card/id{item["id"]}'
                                res['vacancy_name'] = item['name']
                                res['towns'] = item['area']['name']
                                res['level'] = ''
                                res['company'] = item['employer']['name']

                                if item['salary'] != None:
                                    if item['salary']['from'] != None:
                                        res['salary_from'] = int(item['salary']['from']) * currencies[item['currency']]
                                    else:
                                        res['salary_from'] = None
                                    
                                    if item['salary']['to'] != None:
                                        res['salary_to'] = int(item['salary']['to']) * currencies[item['currency']]
                                    else:
                                        res['salary_to'] = None
                                else:
                                    item['salary_form'] = None
                                    item['salary_to'] = None

                                if item['experience']['id'] == 'noExperience':
                                    res['exp_from'] = '0'
                                    res['level'] = 'Junior'
                                elif item['experience']['id'] == 'between1And3':
                                    res['exp_from'] = '1'
                                    res['exp_to'] = '3'
                                    res['level'] = 'Middle'
                                elif item['experience']['id'] == 'between3And6':
                                    res['exp_from'] = '3'
                                    res['exp_to'] = '6'
                                    res['level'] = 'Senior'
                                else:
                                    res['exp_from'] = '6'
                                    res['level'] = 'Lead'

                                res['description'] = re.sub(self.re_html_tag_remove, '', item['description'])

                                res['job_type'] = item['employment']['name']
                                res['job_format'] = item['schedule']['name']


                                res['skills'] = ' '.join(skill['name'] for skill in item['key_skills'])

                                res['source_vac'] = 4

                                res['date_created'] = item['published_at']

                                res['date_of_download'] = datetime.now().date()
                                res['status'] = 'existing'

                                res['version_vac'] = 1 
                                res['actual'] = 1   

                                self.df = pd.concat([self.df, pd.DataFrame(pd.json_normalize(res))], ignore_index=True)

                            except Exception as exc:
                                self.log.error(f'В процессе парсинга вакансии https://www.zarplata.ru/vacancy/card/id{item["id"]} произошла ошибка {exc} \n\n')
                    else:
                        self.log.info(req)

                    time.sleep(5)

                except Exception as exp:
                    self.log.error(f'ERROR {vac_name} {exp}')
                    time.sleep(5)
                    continue

        self.log.info(f'Общее количество найденных вакансий после удаления дубликатов: {len(self.df)}')