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

from raw.variables_settings import variables, base_hh, profs
from raw.base_job_parser import BaseJobParser

table_name = variables['raw_tables'][8]['raw_tables_name']
url = base_hh

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

        self.max_page_count = 5

        # self.vac_name_list = [] #ВСТАВИТЬ СПИСОК ВАКАНСИЙ

        #& Регулярное выражение для удаление HTML тегов из описания
        self.re_html_tag_remove = r'<[^>]+>'

        # for vac_name in self.vac_name_list:
        for self.vac_name in self.profs:
            self.pars_vac(self.vac_name, index=self.profs.index(self.vac_name) + 1)
            time.sleep(5)
        
        self.log.info('ПАРСИНГ ЗАВЕРШЕН')

    #* Добавление в new_df всех вакансий которые возможно получить по названию vac_name
    def pars_vac(self, vac_name:str, index:int):
        for page_number in range(self.max_page_count):
            params = {
                'text': f'{self.vac_name}',
                'page': page_number,
                'per_page': 20,
                'area': '113',
                # 'only_with_salary': 'true',
                'negotiations_order': 'updated_at',
                'vacancy_search_order': 'publication_time'
            }

            try:
                self.log.info(f'get 1.{page_number} {index}/{len(self.profs)} - {self.vac_name}')
                req = requests.get(f'{base_hh}', params=params).json()
                time.sleep(5)
                
                if 'items' in req.keys():
                    for item in req['items']:
                        item = requests.get(f'{base_hh}/{item["id"]}').json()
                        res = {}
                        try:
                            res['vacancy_url'] = f'https://hh.ru/vacancy/{item["id"]}'
                            res['vacancy_name'] = item['name']
                            res['towns'] = item['area']['name']
                            res['level'] = ''
                            res['company'] = item['employer']['name']

                            # if item['salary'] == None:
                                # res['salary_from'] = 0
                                # res['salary_to'] = 999999999
                            if item['salary'] != None:
                                if item['salary']['from'] != None:
                                    res['salary_from'] = int(item['salary']['from'])
                                # else:
                                #     res['salary_from'] = 0
                                if item['salary']['to'] != None:
                                    res['salary_to'] = int(item['salary']['to'])
                                # else:
                                #     res['salary_to'] = 999999999

                            # if item['experience'] == None:
                            #     res['exp_from'] = '0'
                            #     res['exp_to'] = '100'
                            if item['experience']['id'] == 'noExperience':
                                res['exp_from'] = '0'
                                # res['exp_to'] = '0'
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
                                # res['exp_to'] = '100'
                                res['level'] = 'Lead'

                            res['description'] = re.sub(self.re_html_tag_remove, '', item['description'])

                            res['job_type'] = item['employment']['name']
                            res['job_format'] = item['schedule']['name']

                            # res['languages'] = 'Russian'

                            res['skills'] = ' '.join(skill['name'] for skill in item['key_skills'])

                            res['source_vac'] = 4

                            res['date_created'] = item['published_at']

                            res['date_of_download'] = datetime.now().date()
                            # res['status'] = item['type']['name']
                            res['status'] = 'existing'

                            # res['date_closed'] = date(2025, 1, 1)

                            res['version_vac'] = 1 
                            res['actual'] = 1   

                            self.df = pd.concat([self.df, pd.DataFrame(pd.json_normalize(res))], ignore_index=True)
                            self.log.info(self.df)

                        except Exception as exc:
                            self.log.error(f'В процессе парсинга вакансии https://hh.ru/vacancy/{item["id"]} '
                                  f'произошла ошибка {exc} \n\n')

                else:
                    self.log.info(req)

            except Exception as e:
                self.log.error(f'ERROR {self.vac_name} {e}')
                time.sleep(5)
                continue

        self.df = self.df.drop_duplicates()
        self.log.info("Общее количество найденных вакансий после удаления дубликатов: " + str(len(self.df)) + "\n")

