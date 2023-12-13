import logging
import psycopg2
from connect_settings import conn
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    format='%(threadName)s %(name)s %(levelname)s: %(message)s',
    level=logging.INFO
)


class TriggerCreator:
    def __init__(self, conn):
        self.conn = conn
        self.cur = conn.cursor()
        self.front_schema = 'core_schema'
        self.path = '/opt/airflow/dags/core/update_meta.py'

    def create_meta_update_trigger(self):
        try:
            func_create = f"""
            CREATE OR REPLACE FUNCTION {self.front_schema}.core_schema_trigger()
            RETURNS event_trigger AS $$
            BEGIN
                EXECUTE 'python {self.path}';
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """
            self.cur.execute(func_create)
            trigger_create = f"""
            CREATE EVENT TRIGGER core_schema_event_trigger
            ON ddl_command_end
            EXECUTE FUNCTION {self.front_schema}.core_schema_trigger();
            """
            self.cur.execute(trigger_create)
            logging.info("function and trigger created successfully")
            self.conn.commit()
            self.conn.close()
        except Exception as e:
            logging.error(f"Error while creating function or trigger to update meta: {e}")
            self.conn.rollback()
            self.conn.close()
