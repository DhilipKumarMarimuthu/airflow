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
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

def check_datasource_connection():
    conn = OdbcHook().get_conn()
    cursor = conn.cursor()
    cursor.close()
    conn.close()
     
    
def extract_most_recent_date(ti):
    try:
        
        conn = OdbcHook().get_conn()
        cursor = conn.cursor()
        most_recent_date = ti.xcom_pull(key='most_recent_date', task_ids=['catchup_date'])
        print(most_recent_date)
        sql = None
        if not np.array(most_recent_date) or not most_recent_date or most_recent_date is None:
            sql = 'SELECT MAX(minDate.Date) AS Date FROM(SELECT TOP 10 *  FROM [dbo].[MV_Amarok_DailyProdWellData] ORDER BY Date) AS minDate'
            cursor.execute(sql)
        else:
            sql = 'SELECT MAX(minDate.Date) AS Date FROM(SELECT TOP 10 * FROM [dbo].[MV_Amarok_DailyProdWellData] where Date > ? ORDER BY Date) AS minDate'
            cursor.execute(sql, most_recent_date)
        
        print(sql)
        row = cursor.fetchone()
        if row is not None:            
            ti.xcom_push(key='most_recent_date', value=row.Date.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
        print(row.Date)
        cursor.close()
        
    except pyodbc.Error as err:
        print('Failed to read data from table', err)
    finally:
        if conn:
            conn.close()
            print('ODBC connection is closed')
    
    
def extract_data_source(ti):
    try:
        
        conn = OdbcHook().get_conn()
        most_recent_date = ti.xcom_pull(key='most_recent_date', task_ids=['catchup_date'])
        print('most_recent_date')
        print(most_recent_date)
        sql = None
        if not np.array(most_recent_date) or not most_recent_date or most_recent_date is None:
            print('run without xcom value')
            sql = 'SELECT TOP 10 *  FROM [dbo].[MV_Amarok_DailyProdWellData]'
            df = pd.read_sql(sql, conn)
        else:
            print('run with xcom value')
            sql = 'SELECT TOP 10 *  FROM [dbo].[MV_Amarok_DailyProdWellData] where Date > ?'
            df = pd.read_sql(sql, conn, params=[most_recent_date])
        print(sql)                                
        df.to_json ('/home/mdkumarmca/data/daily_production_data.json',orient='records')
        
    except pyodbc.Error as err:
        print('Failed to read data from table', err)
    finally:
        if conn:
            conn.close()
            print('ODBC connection is closed')
    
    
def process_source_data():
    fileHook = FSHook('fs_custom')
    mongoHook = MongoHook()
    path = os.path.join(fileHook.get_path(), 'daily_production_data.json')
    
    df = pd.read_json(path)
    water_cut_calc = []
    gor_calc = []
    
    for index, row in df.iterrows():
        water_cut_calc.append(utils.calc_watercut(row['OIL_bopd'], row['WATER_bwpd']))
        gor_calc.append(utils.calc_gor(row['OIL_bopd'], row['GAS_mscfd']))
    
    df = df.assign(**{'water_cut_calc': water_cut_calc, 'gor_calc': gor_calc})
    
    data_dict = df.to_dict("records")    
    mongoHook.insert_many('DailyProduction',data_dict,'fusion_dev_db')
    
    os.remove(path)
    

with DAG('realtime_data_ingestion_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    is_connection_available = PythonOperator(
        task_id='is_connection_available',
        python_callable=check_datasource_connection
    )
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_source
    )
    sensing_file = FileSensor(
        task_id='sensing_file',
        filepath='daily_production_data.json',
        fs_conn_id='fs_custom'
    )
    process_data = PythonOperator(
        task_id='process_data',
        python_callable=process_source_data
    )    
    catchup_date = PythonOperator(
        task_id='catchup_date',
        python_callable=extract_most_recent_date
    )
    

    is_connection_available >> extract_data >> sensing_file >> process_data >> catchup_date

