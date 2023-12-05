from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from typing import Callable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import sys
import os
sys.path.insert(0, '/opt/airflow/dags/parser_dags')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dag_careerspace, dag_getmatch, dag_hh, dag_habrcareer, dag_vseti, dag_zarplata
import dag_remotejob, dag_sber, dag_tinkoff, dag_vk, dag_yandex
from raw.currency_directory import exchange_rates

start_date = datetime(2023, 12, 6)

def generate_parser_task(task_id: str, run_parser: Callable, trigger_rule='all_done'):
    """
    Function to generate a PythonOperator for running a parser.
    """
    return PythonOperator(
        task_id=task_id,
        python_callable=run_parser,
        provide_context=True,
        trigger_rule=trigger_rule
    )

def generate_parsing_dag(dag_id: str, task_id: str, run_parser: Callable, start_date):
    """
    Function to generate a DAG for parsing tasks.
    """
    dag = DAG(
        dag_id=dag_id,
        default_args={
            "owner": "admin_1T",
            'retry_delay': timedelta(minutes=5),
        },
        start_date=start_date,
        schedule_interval=None,
    )

    with dag:
        hello_bash_task = BashOperator(
            task_id='hello_task',
            bash_command='echo "Wishing you successful parsing! May the reliable internet be with us!"'
        )

        parsed_task = generate_parser_task(task_id=task_id, run_parser=run_parser, trigger_rule='all_done')

        end_task = DummyOperator(
            task_id="end_task"
        )

        hello_bash_task >> parsed_task >> end_task

    return dag


upd_common_dag_id = 'UPDATING_COMMON_PARSING_DAG'

with DAG(
    dag_id=upd_common_dag_id,
    default_args={
        "owner": "admin_1T",
        'retry_delay': timedelta(minutes=5),
    },
    start_date=start_date,
    schedule_interval='@daily',
    ) as updating_common_dag:

    with TaskGroup('updating_parsers') as parsers:

        hello_bash_task = BashOperator(
            task_id='hello_task',
            bash_command='echo "Wishing you successful parsing! May the reliable internet be with us!"',
        )

        update_currency_task = PythonOperator(
            task_id='update_currency_task',
            python_callable=exchange_rates,
            trigger_rule='all_done',
        )

        end_task = DummyOperator(
            task_id="end_task",
        )


        with TaskGroup('parsers_group') as parsers_group:

            update_careerspace_task = generate_parser_task('update_careerspace_task', dag_careerspace.update_call_all_func,
                                                         trigger_rule='all_done')
            update_getmatch_task = generate_parser_task('update_getmatch_task', dag_getmatch.update_call_all_func,
                                                         trigger_rule='all_done')
            update_habrcareer_task = generate_parser_task('update_habrcareer_task', dag_habrcareer.update_call_all_func,
                                                         trigger_rule='all_done')
            update_headhunter_task = generate_parser_task('update_headhunter_task', dag_hh.update_call_all_func,
                                                         trigger_rule='all_done')
            update_vseti_task = generate_parser_task('update_vseti_task', dag_vseti.update_call_all_func,
                                                         trigger_rule='all_done')
            update_zarplata_task = generate_parser_task('update_zarplata_task', dag_zarplata.update_call_all_func,
                                                         trigger_rule='all_done')
            update_vkjob_task = generate_parser_task('update_vkjob_task', dag_vk.update_call_all_func,
                                                         trigger_rule='all_done')
            update_sber_task = generate_parser_task('update_sber_task', dag_sber.update_call_all_func,
                                                         trigger_rule='all_done')
            update_tinkoff_task = generate_parser_task('update_tinkoff_task', dag_tinkoff.update_call_all_func,
                                                         trigger_rule='all_done')
            update_yandex_task = generate_parser_task('update_yandex_task', dag_yandex.update_call_all_func,
                                                         trigger_rule='all_done')
            update_remotejob_task = generate_parser_task('update_remotejob_task', dag_remotejob.update_call_all_func,
                                                         trigger_rule='all_done')

            # Define the execution order of tasks within the task group
            update_careerspace_task >> update_getmatch_task >> update_habrcareer_task >> \
            update_headhunter_task >> update_vseti_task >> update_zarplata_task >> update_vkjob_task \
            >> update_sber_task >> update_tinkoff_task >> update_yandex_task >> update_remotejob_task

        hello_bash_task >> update_currency_task >> parsers_group >> end_task

# Create separate DAGs for each parsing task
update_currency_dag = generate_parsing_dag('update_currency_dag', 'update_currency',
                                      exchange_rates, start_date)
update_careerspace_dag = generate_parsing_dag('update_careerspace_dag', 'update_careerspace',
                                      dag_careerspace.update_call_all_func, start_date)
update_getmatch_dag = generate_parsing_dag('update_getmatch_dag', 'update_getmatch',
                                      dag_getmatch.update_call_all_func, start_date)
update_habrcareer_dag = generate_parsing_dag('update_habrcareer_dag', 'update_habrcareer',
                                      dag_habrcareer.update_call_all_func, start_date)
update_headhunter_dag = generate_parsing_dag('update_headhunter_dag', 'update_headhunter',
                                      dag_hh.update_call_all_func, start_date)
update_vseti_dag = generate_parsing_dag('update_vseti_dag', 'update_vseti',
                                      dag_vseti.update_call_all_func, start_date)
update_zarplata_dag = generate_parsing_dag('update_zarplata_dag', 'update_zarplata',
                                      dag_zarplata.update_call_all_func, start_date)
update_vk_dag = generate_parsing_dag('update_vk_dag', 'update_vk',
                                      dag_vk.update_call_all_func, start_date)
update_sber_dag = generate_parsing_dag('update_sber_dag', 'update_sber',
                                      dag_sber.update_call_all_func, start_date)
update_tinkoff_dag = generate_parsing_dag('update_tinkoff_dag', 'update_tinkoff',
                                      dag_tinkoff.update_call_all_func, start_date)
update_yandex_dag = generate_parsing_dag('update_yandex_dag', 'update_yandex',
                                      dag_yandex.update_call_all_func, start_date)
update_remotejob_dag = generate_parsing_dag('update_remotejob_dag', 'update_remotejob',
                                      dag_remotejob.update_call_all_func, start_date)

# Make DAGs globally accessible
globals()[upd_common_dag_id] = updating_common_dag
globals()[update_currency_dag.dag_id] = update_currency_dag
globals()[update_careerspace_dag.dag_id] = update_careerspace_dag
globals()[update_getmatch_dag.dag_id] = update_getmatch_dag
globals()[update_habrcareer_dag.dag_id] = update_habrcareer_dag
globals()[update_headhunter_dag.dag_id] = update_headhunter_dag
globals()[update_vseti_dag.dag_id] = update_vseti_dag
globals()[update_zarplata_dag.dag_id] = update_zarplata_dag
globals()[update_vk_dag.dag_id] = update_vk_dag
globals()[update_sber_dag.dag_id] = update_sber_dag
globals()[update_tinkoff_dag.dag_id] = update_tinkoff_dag
globals()[update_yandex_dag.dag_id] = update_yandex_dag
globals()[update_remotejob_dag.dag_id] = update_remotejob_dag

