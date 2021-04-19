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

with DAG('flowback_data_ingestion_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    check_file_exists=FileSensor(task_id='check_file_exists', filepath='{{var.value.flowback_data_source_filename}}', fs_conn_id='fs_custom', poke_interval=5, timeout=30)

    clean_raw_data = PythonOperator(task_id='clean_raw_data', python_callable=utils.filter_flowback_data, op_kwargs={'filepath': '{{var.value.flowback_data_source_filepath}}', 'filterpath': '{{var.value.flowback_data_filepath}}', 'filter_columns': '{{var.value.de_filter_columns}}'})

    validate_raw_data = PythonOperator(task_id='validate_raw_data', python_callable=utils.validate_flowback_data, op_kwargs={'filepath': '{{var.value.flowback_data_filepath}}', 'filterpath': '{{var.value.flowback_data_filterpath}}', 'filter_columns': '{{var.value.de_filter_columns}}'})
    
    process_raw_data = PythonOperator(task_id='process_raw_data', python_callable=utils.process_flowback_data, op_kwargs={'filepath': '{{var.value.flowback_data_filterpath}}', 'filterpath': '{{var.value.flowback_data_filterpath}}', 'filter_columns': '{{var.value.de_filter_columns}}'})

    transfer_data =  CsvToMongoOperator(task_id='transfer_data', fs_conn_id='fs_custom', mongo_conn_id='mongo_default', mongo_collection='FlowBackData')

    # rename_source_file = BashOperator(task_id='rename_source_file', bash_command='mv {{var.value.flowback_data_source_filepath}} {{var.value.flowback_data_rename_path}}_%s.xlsm' % yesterday_date)
    
    check_file_exists >> clean_raw_data >> validate_raw_data >> process_raw_data >> transfer_data
    
    
