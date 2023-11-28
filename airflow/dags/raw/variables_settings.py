import sys
import os
sys.path.insert(0, '/opt/airflow/dags/')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

variables = {
    "conn_id": "psql_connect",
    "psql_path": "/docker-entrypoint-initdb.d/raw_data/",
    "schemes": {
        "raw": "raw_scheme",
        "core": "core_scheme"
    },
    "base_sber": "https://rabota.sber.ru/search",
    "base_yand": "https://yandex.ru/jobs/vacancies/",
    "base_vk": "https://team.vk.company/vacancy/",
    "base_tin": "https://www.tinkoff.ru/career/it/",
    "base_remote": "https://remote-job.ru/search?search%5Bquery%5D=&search%5BsearchType%5D=vacancy",
    "base_habr": "https://career.habr.com/vacancies",
    "base_getmatch": "https://getmatch.ru/vacancies?p={i}&sa=150000&pa=all&s=landing_ca_header",
    "base_careerspace": "https://careerspace.app/api/v2/jobs/filters?skip={i}&take=8&sortBy=new-desc&functions=8%2C13%2C14%2C9&jobLevel%5B0%5D={o}&currencyCode=RUR",
    "raw_tables": [
        {"raw_tables_name": "raw_vk"},
        {"raw_tables_name": "raw_sber"},
        {"raw_tables_name": "raw_tinkoff"},
        {"raw_tables_name": "raw_yandex"},
        {"raw_tables_name": "raw_remote"},
        {"raw_tables_name": "raw_habr"},
        {"raw_tables_name": "raw_getmatch"},
        {"raw_tables_name": "raw_zarplata"},
        {"raw_tables_name": "raw_hh"},
        {"raw_tables_name": "raw_careerspace"},
    ],
    "professions": [
        {"fullName": "Data engineer"}
    ]
}

profs = variables.get('professions')
schemes = variables.get('schemes')
raw_tables = variables.get('raw_tables')

# URLs
base_sber = variables.get('base_sber')
base_yand = variables.get('base_yand')
base_vk = variables.get('base_vk')
base_tin = variables.get('base_tin')
base_remote = variables.get('base_remote')
base_getmatch = variables.get('base_getmatch')
base_habr = variables.get('base_habr')



