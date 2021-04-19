from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable



import os
import logging as log
import pandas as pd
from datetime import datetime
import pyodbc
from contextlib import closing

class SqlToMongoOperator(BaseOperator):
    
    template_fields = ('postgres_sql',)
    template_ext = ('.sql',)
    
    @apply_defaults
    def __init__(self, odbc_conn_id, postgres_conn_id, mongo_conn_id, mongo_collection, postgres_sql, mongo_db='', *args, **kwargs):
        self.odbc_conn_id = odbc_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.mongo_conn_id = mongo_conn_id        
        self.mongo_collection = mongo_collection
        self.postgres_sql = postgres_sql
        # self.prev_exec_date = prev_exec_date
        super().__init__(*args, **kwargs)

    def execute(self, context):
        mongoHook = MongoHook(conn_id=self.mongo_conn_id)
        self.mongo_db = mongoHook.connection.schema
        
        log.info('odbc_conn_id: %s', self.odbc_conn_id)
        log.info('postgres_conn_id: %s', self.postgres_conn_id)
        log.info('mongo_conn_id: %s', self.mongo_conn_id)
        log.info('postgres_sql: %s', self.postgres_sql)
        # log.info('prev_exec_date: %s', self.prev_exec_date)
        log.info('mongo_db: %s', self.mongo_db)
        log.info('mongo_collection: %s', self.mongo_collection)
        
        odbcHook = OdbcHook(self.odbc_conn_id)
        
        well_data = self.get_data()
        log.info('postgres well data: %s', well_data)
        most_recent_date = self.get_most_recent_date(mongoHook)
        print(most_recent_date)
        if most_recent_date:
            print('store most recent date inside airflow variable')
            Variable.set("most_recent_date", most_recent_date)
        
        with closing(odbcHook.get_conn()) as conn:
            for index, well in well_data.iterrows():
                print(well['well_name'],well['is_newly_added'])
                if well is not None and well['is_newly_added']:
                    sql = 'SELECT *  FROM [dbo].[MV_Amarok_DailyProdWellDemoData] where Name = ?'
                    df = pd.read_sql(sql, conn, params=[well['well_name']])
                else:
                    sql = 'SELECT *  FROM [dbo].[MV_Amarok_DailyProdWellDemoData] where Name = ? and Date > ?'
                    df = pd.read_sql(sql, conn, params=[well['well_name'], most_recent_date])

                if not df.empty:
                    data_dict = df.to_dict("records")
                    self.insert_records(mongoHook, data_dict)
                    # mongoHook.insert_many(self.mongo_collection, data_dict, self.mongo_db)
                    
    
    def get_data(self):
        pgHook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        with closing(pgHook.get_conn()) as conn:
            df = pd.read_sql(self.postgres_sql, conn)
            return df
            
    def insert_records(self, mongoHook, docs):
        log.info('inserting mongo db')
        if len(docs) == 1:
            mongoHook.insert_one(self.mongo_collection,
                             docs[0],
                             mongo_db=self.mongo_db)
        else:
            mongoHook.insert_many(self.mongo_collection,
                              docs,
                              mongo_db=self.mongo_db)
            
    def get_most_recent_date(self, mongoHook):
        print('most recent date function started ')
        # most_recent_cursor = mongoHook.find().sort([("Date", -1)]).limit(1)
        filter_query = [{ "$group": {  "_id": 'null', "Date": { "$max": "$Date" }   }}]
        most_recent_cursor = mongoHook.aggregate(self.mongo_collection, filter_query, self.mongo_db )
        most_recent_doc = [doc for doc in most_recent_cursor]
        most_recent_date = None
        for doc in most_recent_doc:
            most_recent_date = doc.get('Date').strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        log.info('inside most recent date function: %s', most_recent_date)
        return most_recent_date
        