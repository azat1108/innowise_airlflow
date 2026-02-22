from airflow.sdk import DAG, Asset
from airflow.models import Variable
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup
from datetime import datetime
import os
import pandas as pd
import re


file_path_to_listen = Variable.get("file_path_to_listen")
processed_dataset_path = Variable.get("processed_dataset_path")
processed_asset = Asset(processed_dataset_path)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 2, 17),
    "retries": 1,
}

def check_file_empty(**context):
    if os.path.getsize(file_path_to_listen) == 0:
        return "file_empty_task"
    else:
        return "data_processing_group.replace_nulls"

def replace_nulls_func():
    df = pd.read_csv(file_path_to_listen)
    df = df.fillna("-")
    df.to_csv(file_path_to_listen, index=False)

def sort_by_date_func():
    df = pd.read_csv(file_path_to_listen, parse_dates=["at"])
    df = df.sort_values(by="at")
    df.to_csv(file_path_to_listen, index=False)

def clean_content_func():
    df = pd.read_csv(file_path_to_listen)
    df["content"] = df["content"].apply(lambda x: re.sub(r"[^\w\s\.,!?;:]", "", str(x)))
    df.to_csv(processed_dataset_path, index=False)

with DAG(
    dag_id="process_data",
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_data_file",
        filepath=file_path_to_listen,
        poke_interval=120,
        mode="reschedule",
    )

    branch_task = BranchPythonOperator(
        task_id="check_if_file_empty",
        python_callable=check_file_empty,
    )

    file_empty_task = BashOperator(
        task_id="file_empty_task",
        bash_command='echo "File is empty."',
    )

    with TaskGroup("data_processing_group") as data_processing_group:
        replace_nulls = PythonOperator(task_id="replace_nulls", python_callable=replace_nulls_func)
        sort_by_date = PythonOperator(task_id="sort_by_date", python_callable=sort_by_date_func)
        clean_content = PythonOperator(task_id="clean_content", python_callable=clean_content_func, outlets=[processed_asset])

        replace_nulls >> sort_by_date >> clean_content

    wait_for_file >> branch_task
    branch_task >> [file_empty_task, data_processing_group]
