import re
import pandas as pd
import time
import requests
from datetime import date, datetime

class hh_parser:
    def __init__(self, max_page_count=5) -> None:
        self.table_name = 'hh_row'
        self.max_page_count = max_page_count

        self.vac_name_list = [] #ВСТАВИТЬ СПИСОК ВАКАНСИЙ

        #& Регулярное выражение для удаление HTML тегов из описания
        self.re_html_tag_remove = r'<[^>]+>'

        self.new_df = pd.DataFrame(columns=['vacancy_id', 'vacancy_name', 'towns', 
                                'level', 'company', 'salary_from', 'salary_to',
                                'exp_from', 'exp_to', 'description', 
                                'job_type', 'job_format', 'languages', 
                                'skills', 'source_vac', 
                                'date_created', 'date_of_download', 
                                'status', 'date_closed',
                                'version_vac', 'actual'])

    #* Основная функция парсинга
    def topars(self):
        connection = self.pg_hook.get_conn()
        cur = connection.cursor()

        for vac_name in self.vac_name_list:
            self.pars_vac(vac_name, index=self.vac_name_list.index(vac_name)+1)
            time.sleep(5)
        
        print('ПАРСИНГ ЗАВЕРШЕН')

    #* Добавление в new_df всех вакансий которые возможно получить по названию vac_name
    def pars_vac(self, vac_name:str, index:int):
        for page_number in range(self.max_page_count):
            params = {
                'text': f'{vac_name}',
                'page': page_number,
                'per_page': 20,
                'area': '113',
                'only_with_salary': 'true',
                'negotiations_order': 'updated_at',
                'vacancy_search_order': 'publication_time'
            }

            try:
                print(f'get 1.{page_number} {index}/{len(self.vac_name_list)} - {vac_name}')
                req = requests.get('https://api.hh.ru/vacancies', params=params).json()
                time.sleep(5)
                
                if 'items' in req.keys():
                    for item in req['items']:
                        item = requests.get(f'https://api.hh.ru/vacancies/{item["id"]}').json()
                        res = {}
                        try:
                            res['vacancy_id'] = f'https://hh.ru/vacancy/{item["id"]}'
                            res['vacancy_name'] = item['name']
                            res['towns'] = item['area']['name']
                            res['level'] = ''
                            res['company'] = item['employer']['name']

                            if item['salary'] == None:
                                res['salary_from'] = 0
                                res['salary_to'] = 999999999
                            else:
                                if item['salary']['from'] != None:
                                    res['salary_from'] = int(item['salary']['from'])
                                else:
                                    res['salary_from'] = 0
                                    
                                if item['salary']['to'] != None:
                                    res['salary_to'] = int(item['salary']['to'])
                                else:
                                    res['salary_to'] = 999999999

                            if item['experience'] == None:
                                res['exp_from'] = '0'
                                res['exp_to'] = '100'
                            elif item['experience']['id'] == 'noExperience':
                                res['exp_from'] = '0'
                                res['exp_to'] = '0'
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
                                res['exp_to'] = '100'
                                res['level'] = 'Lead'

                            res['description'] = re.sub(self.re_html_tag_remove, '', item['description'])

                            res['job_type'] = item['employment']['name']
                            res['job_format'] = item['schedule']['name']

                            res['languages'] = 'Russian'

                            res['skills'] = ' '.join(skill['name'] for skill in item['key_skills'])

                            res['source_vac'] = 'hh.ru'

                            res['date_created'] = item['published_at']

                            res['date_of_download'] = datetime.now()
                            res['status'] = item['type']['name']

                            res['date_closed'] = date(2025, 1, 1)

                            res['version_vac'] = 1 
                            res['actual'] = 1   

                            self.new_df = pd.concat([self.new_df, pd.DataFrame(pd.json_normalize(res))], ignore_index=True)
                        except Exception as exc:
                            print(f'В процессе парсинга вакансии https://hh.ru/vacancy/{item["id"]} произошла ошибка {exc} \n\n')

                else:
                    print(req)

            except Exception as e:
                print(f'ERROR {vac_name} {e}')
                time.sleep(5)
                continue