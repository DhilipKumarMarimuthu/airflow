from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.contrib.hooks.fs_hook import FSHook
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.utils.dates import days_ago, timedelta
from airflow.models import Variable
from mongo_plugin.operators.csv_to_mongo_operator import CsvToMongoOperator
from utils import utils


from datetime import datetime
import pyodbc
import pandas as pd
import numpy as np
import os

yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

default_args = {
    'owner':'airflow',
    'start_date': datetime(2021, 3, 9),    
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

with DAG('csv_data_ingestion_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    check_file_exists=FileSensor(task_id='check_file_exists', filepath='{{var.value.downhole_equipment_filename}}', fs_conn_id='fs_custom', poke_interval=5, timeout=30)

    clean_raw_csv = PythonOperator(task_id='clean_raw_csv', python_callable=utils.filter_csv_data, op_kwargs={'filepath': '{{var.value.downhole_equipment_filepath}}', 'filterpath': '{{var.value.downhole_equipment_filterpath}}', 'filter_columns': '{{var.value.de_filter_columns}}'})

    transfer_data =  CsvToMongoOperator(task_id='transfer_data', fs_conn_id='fs_custom', mongo_conn_id='mongo_default', mongo_collection='WellboreEquipment')

    rename_raw_csv = BashOperator(task_id='rename_raw_csv', bash_command='mv {{var.value.downhole_equipment_filepath}} {{var.value.downhole_equipment_rename_path}}_%s.csv' % yesterday_date)

    check_file_exists >> clean_raw_csv >> transfer_data >> rename_raw_csv
