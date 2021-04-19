from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



import os
import logging as log
import pandas as pd
from contextlib import closing

class SqlToPostgresOperator(BaseOperator):
    
    template_fields = ('odbc_sql','postgres_sql','postgres_insert_sql')
    template_ext = ('.sql',)
    
    @apply_defaults
    def __init__(self, odbc_conn_id, odbc_sql, postgres_conn_id, postgres_sql, postgres_insert_sql, mongo_conn_id, mongo_collection, *args, **kwargs):
        self.odbc_conn_id = odbc_conn_id
        self.odbc_sql = odbc_sql
        self.postgres_conn_id = postgres_conn_id
        self.postgres_sql = postgres_sql
        self.postgres_insert_sql = postgres_insert_sql
        self.mongo_conn_id = mongo_conn_id        
        self.mongo_collection = mongo_collection
        super().__init__(*args, **kwargs)

    def execute(self, context):
        
        
        mongoHook = MongoHook(conn_id=self.mongo_conn_id)
        
        log.info('odbc_conn_id: %s', self.odbc_conn_id)
        log.info('postgres_conn_id: %s', self.postgres_conn_id)
        log.info('mongo_conn_id: %s', self.mongo_conn_id)
        log.info('mongo_db: %s', mongoHook.connection.schema)
        log.info('mongo_collection: %s', self.mongo_collection)
        log.info('odbc_sql: %s', self.odbc_sql)
        log.info('postgres_sql: %s', self.postgres_sql)
        log.info('postgres_insert_sql: %s', self.postgres_insert_sql)
        
        mongo_well_list = mongoHook.get_collection(self.mongo_collection).distinct("Name")
        log.info('mongo well list: %s', mongo_well_list)
        odbc_well_list = self.get_data()
        log.info('odbc well list: %s', odbc_well_list)
        final_well_list = []
        if not mongo_well_list and len(mongo_well_list) == 0:
            final_well_list = self.prepare_well_list(odbc_well_list, True)
        else:
            mongo_filtered_well_list = self.prepare_well_list(mongo_well_list, False)
            new_well_list = list(set(odbc_well_list)-set(mongo_well_list))
            log.info('new well list: %s', new_well_list)
            new_well_list = self.prepare_well_list(new_well_list, True)
            postgres_well_list = self.get_well_data()
            if  postgres_well_list.empty == True:
                for item in new_well_list:
                    final_well_list.append(item)
            else:
                final_well_list = new_well_list
            
        log.info('final well list for insert: %s', final_well_list)
        if final_well_list and len(final_well_list) > 0:
            self.insert_data(final_well_list)
                    
    def prepare_well_list(self, well_list, is_newly_added):
        log.info('prepare_well_list method started')
        final_well_list = []
        for item in well_list:
            final_well_list.append([item, is_newly_added])
        log.info('prepare_well_list: %s', final_well_list)
        log.info('prepare_well_list method completed')
        return final_well_list
        
    
    def get_data(self):
        odbcHook = OdbcHook(conn_id=self.odbc_conn_id)
        with closing(odbcHook.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                cur.execute(self.odbc_sql)
                rows = cur.fetchall()
                rows = [row[0] for row in rows]
                return rows
            
    def insert_data(self, well_list):        
        pgHook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        with closing(pgHook.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                cur.executemany(self.postgres_insert_sql, well_list)
                conn.commit()
                
    def get_well_data(self):
        pgHook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        with closing(pgHook.get_conn()) as conn:
            df = pd.read_sql(self.postgres_sql, conn)
            return df
                