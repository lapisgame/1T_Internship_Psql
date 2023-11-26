import pandas as pd
import numpy as np
import re
import spacy
import psycopg2

from datetime import datetime
from spacy.matcher import Matcher
from spacy.lang.en import English

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.patterns_all import patterns_town, patterns_skill, patterns_jformat, patterns_jtype
from core.dict_for_model import dict_i_jformat, dict_job_types, all_skill_dict, dict_all_spec


pd.DataFrame.iteritems = pd.DataFrame.items
#Отображение колонок и строк в VScode
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# Загрузка словарей, потом заменяется на SQL / Python запросы к БД
job_formats_dict = pd.read_csv("/opt/airflow/dags/core/for_de/dict/job_formats.csv")
languages_dict = pd.read_csv("/opt/airflow/dags/core/for_de/dict/languages.csv")
skills_dict = pd.read_csv("/opt/airflow/dags/core/for_de/dict/skills.csv")
companies_dict = pd.read_csv("/opt/airflow/dags/core/for_de/dict/companies.csv")
job_types_dict = pd.read_csv("/opt/airflow/dags/core/for_de/dict/job_types.csv")
specialities_dict = pd.read_csv("/opt/airflow/dags/core/for_de/dict/specialities.csv")
towns_dict = pd.read_csv("/opt/airflow/dags/core/for_de/dict/towns.csv")
sources_dict = pd.read_csv("/opt/airflow/dags/core/for_de/dict/sources.csv")



# raw_sber = pd.read_csv("data/raw_sber_202311111958.csv")



class Data_preprocessing():

    def __init__(self, dataframe):
        '''
        Initializing models spacy, connecting to the database, creating empty tables for core: vacancies, job_formats_vacancies,
        languages_vacancies, skills_vacancies, job_types_vacancies, specialities_vacancies, towns_vacancies, ds_search, experience_vacancies,
        specialities_skills.
        Adding post id from core
        '''
        # # Connecting to the database
        # conn = psycopg2.connect(dbname='vacancy', user='admin',
        #                 password='password', host='146.120.224.155', port='10131')
        
        # Loading dataframe from raw
        self.dataframe = dataframe
        self.dataframe['url'] = self.dataframe['vacancy_url']
        self.dataframe['vacancy_id'] = range(1, (self.dataframe.shape[0] + 1))
        # Добавить id с core!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        

        # Initializing models for each column on core
        self.nlp = spacy.load('ru_core_news_lg')
        self.nlp_lem = spacy.load('ru_core_news_lg', disable=['parser', 'ner'])
        self.matcher_town = Matcher(self.nlp.vocab)
        self.matcher_skill = Matcher(self.nlp.vocab)
        self.matcher_jformat = Matcher(self.nlp.vocab)
        self.matcher_jtype = Matcher(self.nlp.vocab)

        # Creating the vacancies table
        self.vacancies = pd.DataFrame(columns = ['id', 'version', 'url', 'title', 'salary_from', 'salary_to', 
                                                 'experience_from', 'experience_to', 'description', 'company_id',
                                                 'source_id', 'publicated_at'])

        # Creating id-id relationship tables 
        self.job_formats_vacancies = pd.DataFrame(columns = ['vacancy_id', 'job_format_id'])
        self.experience_vacancies = pd.DataFrame(columns = ['vacancy_id', 'experience_id'])
        self.languages_vacancies = pd.DataFrame(columns = ['vacancy_id', 'language_id'])
        self.skills_vacancies = pd.DataFrame(columns = ['vacancy_id', 'skill_id'])
        self.specialities_skills = pd.DataFrame(columns = ['spec_id', 'skill_id'])
        self.job_types_vacancies = pd.DataFrame(columns = ['vacancy_id', 'job_type_id'])
        self.specialities_vacancies = pd.DataFrame(columns = ['vacancy_id', 'spec_id', 'concurrence_percent'])
        self.towns_vacancies = pd.DataFrame(columns = ['vacancy_id', 'town_id'])
        self.ds_search = pd.DataFrame(columns = ['id', 'vector'])


    def description_lemmatization(self, text):
        '''
        Lemmatization function. Returns completely cleared text
        '''
        text = re.sub(r'[^\w\s]', ' ', text)
        doc = self.nlp_lem(text)
        processed = " ".join([token.lemma_ for token in doc])

        return processed


    def description_lemmatization_add(self):
        '''
        Adds lemmatization to the dataframe in the all_search cell
        '''
        # We combine all search strings into one
        self.dataframe['all_search'] = self.dataframe['towns'].astype(str) + ' ' + self.dataframe['description'].astype(str) + ' ' + self.dataframe['job_type'].astype(str) + ' ' + self.dataframe['job_format'].astype(str) + ' ' + self.dataframe['skills'].astype(str)
        self.dataframe['all_search'] = self.dataframe['all_search'].apply(self.description_lemmatization)


    def description_processing_town(self, pat_town, towns_dict):
        '''
        Loading the dataframe and dictionary patterns_town
        '''
        matcher_town = self.matcher_town
        matcher_town.add("TOWN_PATTERNS", pat_town)
        self.dataframe['town_search'] = self.dataframe['towns'].astype(str) + ' ' + self.dataframe['skills'].astype(str)
        
        for i_town in range(self.dataframe.shape[0]):
            self.dataframe.loc[i_town, 'town_search'] = re.sub(r'[^\w\s]', ' ', self.dataframe.loc[i_town, 'town_search'])
            doc = self.nlp(self.dataframe.loc[i_town, 'town_search'])
            matches = matcher_town(doc)

            list_town = []
            for match_id, start, end in matches:
                span = str(doc[start:end])
                list_town.append(span)
            fin_town = list(set(list_town))
            
            for element in fin_town:
                index = int(towns_dict.loc[towns_dict['clear_title'] == element.lower(), 'id'].iloc[-1]) # Можно заменить SQL!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                self.towns_vacancies.loc[len(self.towns_vacancies.index)] = [self.dataframe.loc[i_town, 'vacancy_id'], index]

        self.towns_vacancies.drop_duplicates(inplace = True)


    def description_processing_skill(self, pat_skill, all_skill_d, skills_dict):
        '''
        Loading the dataframe and dictionary patterns_skill, all_skill_dict
        '''
        matcher_skill = self.matcher_skill
        matcher_skill.add("SKILL_PATTERNS", pat_skill)
        
        for i_skill in range(self.dataframe.shape[0]):
            doc = self.nlp(self.dataframe.loc[i_skill, 'all_search'])
            matches = matcher_skill(doc)
            list_skill = []
            for match_id, start, end in matches:
                span = str(doc[start:end])
                list_skill.append(span)
            fin_skill = list(set(list_skill))
            if not fin_skill: 
                fin_skill = ['не указан']
            

            for key, vals in all_skill_d.items():
                for val in vals:
                    for i in range(len(fin_skill)):
                        if fin_skill[i] == fin_skill[i]:
                            fin_skill[i] = fin_skill[i].replace(val, key)
                        else: 
                            fin_skill.remove(fin_skill[i])

            for element in fin_skill:
                try:
                    index = int(skills_dict.loc[skills_dict['title'] == element.lower(), 'id'].iloc[-1]) # Можно заменить SQL!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                    self.skills_vacancies.loc[len(self.skills_vacancies.index)] = [self.dataframe.loc[i_skill, 'vacancy_id'], index]
                except: pass

           
            # For the model, save it in a dataframe
            fin_str_skill = ''
            for el in fin_skill:
                fin_str_skill += str.lower(el)
                fin_str_skill += ','
            self.dataframe.loc[i_skill, 'skill_clean'] = fin_str_skill
        

        self.skills_vacancies.drop_duplicates(inplace = True)

        
    def description_processing_jformat(self, pat_jformat, d_i_jformat, job_form_dict):
        '''
        Loading dataframe and dictionary patterns_jformat, dict_i_jformat
        '''
        matcher_jformat = self.matcher_jformat
        matcher_jformat.add("JFORMAT_PATTERNS", pat_jformat)

        for i_jformat in range(self.dataframe.shape[0]):
            doc = self.nlp(self.dataframe.loc[i_jformat, 'all_search'])
            matches = matcher_jformat(doc)
            list_jformat = []
            for match_id, start, end in matches:
                span = str(doc[start:end])
                list_jformat.append(span)
            fin_jformat = list(set(list_jformat))
            if not fin_jformat: 
                fin_jformat = ['не указан']

            for key, vals in d_i_jformat.items():
                for val in vals:
                    for i in range(len(fin_jformat)):
                        fin_jformat[i] = fin_jformat[i].replace(val, key)

            for element in fin_jformat:
                index = int(job_form_dict.loc[job_form_dict['title'] == element.lower(), 'id'].iloc[-1]) # Можно заменить SQL!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                self.job_formats_vacancies.loc[len(self.job_formats_vacancies.index)] = [self.dataframe.loc[i_jformat, 'vacancy_id'], index]


    def description_processing_jtype(self, pat_jtype, job_type_dict, d_job_types):
        '''
        Loading a dataframe and dictionary patterns_jtype, dict_job_types
        '''
        matcher_jtype = self.matcher_jtype
        matcher_jtype.add("JTYPE_PATTERNS", pat_jtype)

        for i_jtype in range(self.dataframe.shape[0]):
            doc = self.nlp(self.dataframe.loc[i_jtype, 'all_search'])
            matches = matcher_jtype(doc)
            list_jtype = []
            for match_id, start, end in matches:
                span = str(doc[start:end])
                list_jtype.append(span)
            fin_jtype = list(set(list_jtype))

            if not fin_jtype: 
                fin_jtype = ['не указан']

            for key, vals in d_job_types.items():
                for val in vals:
                    for i in range(len(fin_jtype)):
                        fin_jtype[i] = fin_jtype[i].replace(val, key)

            for element in fin_jtype:
                index = int(job_type_dict.loc[job_type_dict['title'] == element.lower(), 'id'].iloc[-1]) # Можно заменить SQL!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                self.job_types_vacancies.loc[len(self.job_types_vacancies.index)] = [self.dataframe.loc[i_jtype, 'vacancy_id'], index]


    # Experience and salary. There is no final solution yet


    def clustering_specialties():
        '''
        Clustering by specialty
        '''
        pass


    def save_dataframe(self):
        '''
        Saving a dataframe vacancies
        '''
        for num in range(self.dataframe.shape[0]):
            str_for_vacancies = {'id': (self.dataframe.loc[num, 'vacancy_id']),
                                 'version': (self.dataframe.loc[num, 'version_vac']),
                                 'url': (self.dataframe.loc[num, 'vacancy_url']),
                                 'title': (self.dataframe.loc[num, 'vacancy_name']),
                                 'salary_from': (self.dataframe.loc[num, 'salary_from']),
                                 'salary_to': (self.dataframe.loc[num, 'salary_to']),
                                 'experience_from': (self.dataframe.loc[num, 'exp_from']),
                                 'experience_to': (self.dataframe.loc[num, 'exp_to']),
                                 'publicated_at': self.dataframe.loc[num, 'date_created']}

            self.vacancies = self.vacancies._append(str_for_vacancies, ignore_index=True)


    def call_all_functions(self):
        '''
        General function call method
        '''
        self.description_lemmatization_add()
        self.description_processing_town(patterns_town, towns_dict)
        self.description_processing_skill(patterns_skill, all_skill_dict, skills_dict)
        self.description_processing_jformat(patterns_jformat, dict_i_jformat, job_formats_dict)
        self.description_processing_jtype(patterns_jtype, job_types_dict, dict_job_types)
        self.save_dataframe()

        self.dict_all_data = {
                'vacancies': self.vacancies,
                'job_formats_vacancies': self.job_formats_vacancies,
                'languages_vacancies': self.languages_vacancies,
                "skills_vacancies": self.skills_vacancies,
                'job_types_vacancies': self.job_types_vacancies,
                'specialities_vacancies': self.specialities_vacancies,
                'towns_vacancies': self.towns_vacancies, 
                'ds_search': self.ds_search,
                'experience_vacancies': self.experience_vacancies,
                'specialities_skills':self.specialities_skills    }






        

# test = Data_preprocessing(raw_sber)
# test.call_all_functions()

