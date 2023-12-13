import json
from connect_settings import conn
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


schema = 'core_schema'
db = 'vacancy'


def update_meta():
    cur = conn.cur
    update_query = f"""SELECT table_name, column_name, data_type
                       FROM information_schema.columns
                       WHERE table_schema = 'core_schema';"""
    cur.execute(update_query)
    rows = cur.fetchall()
    rows.sort()
    dictionary = {}
    with open('/opt/airflow/dags/config_meta.json', 'w') as file:
        for row in rows:
            dictionary = {db: {'table': row[0], 'column': row[1], 'type': row[2]}}
            json.dump(dictionary, file, indent=4)


update_meta()
