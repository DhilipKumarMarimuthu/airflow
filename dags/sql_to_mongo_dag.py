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
from airflow.operators.postgres_operator import PostgresOperator
from mongo_plugin.operators.mongo_data_process_operator import MongoDataProcessOperator
from mongo_plugin.operators.sql_to_mongo_operator import SqlToMongoOperator
from mongo_plugin.operators.sql_to_postgres_operator import SqlToPostgresOperator
from utils import utils


from datetime import datetime
import pyodbc
import pandas as pd
import numpy as np
import os

yesterday_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

default_args = {
    'owner':'airflow',
    'start_date': datetime(2021, 3, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=10)
}

def check_datasource_connection():
    conn = OdbcHook().get_conn()
    cursor = conn.cursor()
    cursor.close()
    conn.close()

with DAG('sql_data_ingestion_dag', default_args=default_args, schedule_interval='@daily', template_searchpath=['/c/users/mdkum/airflow/dags/sql_files'], catchup=False) as dag:
    
    is_connection_available = PythonOperator(
        task_id='is_connection_available',
        python_callable=check_datasource_connection
    )
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_default',
        sql='create_well_table.sql'
    )    
    prepare_data = SqlToPostgresOperator(
        task_id='prepare_data',
        odbc_conn_id='odbc_default',
        odbc_sql='select_daily_data.sql',
        postgres_conn_id='postgres_default',
        postgres_sql='select_well_data.sql',
        postgres_insert_sql='insert_well_data.sql',
        mongo_conn_id='mongo_default',
        mongo_collection='DailyProduction'           
    )    
    transfer_data = SqlToMongoOperator(
        task_id='transfer_data',
        odbc_conn_id='odbc_default',
        postgres_conn_id='postgres_default',
        mongo_conn_id='mongo_default',
        mongo_collection='DailyProduction',
        postgres_sql='select_well_data.sql'        
    )    
    process_data = MongoDataProcessOperator(
        task_id='process_data',
        postgres_conn_id='postgres_default',
        mongo_conn_id='mongo_default',
        mongo_collection='DailyProduction',
        postgres_sql='select_well_data.sql'   
    )            
    update_table = PostgresOperator(
        task_id='update_table',
        postgres_conn_id='postgres_default',
        sql='update_well_data.sql'
    )
    
    is_connection_available >> create_table >> prepare_data >> transfer_data >> process_data >> update_table

