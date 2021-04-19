from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from utils import utils

import os
import logging as log
import pandas as pd
from contextlib import closing
from datetime import datetime

class MongoDataProcessOperator(BaseOperator):
    
    template_fields = ('postgres_sql',)
    template_ext = ('.sql',)
    
    @apply_defaults
    def __init__(self, postgres_conn_id, mongo_conn_id, mongo_collection, postgres_sql, mongo_db='', *args, **kwargs):
        self.postgres_conn_id = postgres_conn_id
        self.mongo_conn_id = mongo_conn_id        
        self.mongo_collection = mongo_collection
        self.postgres_sql = postgres_sql
        # self.prev_exec_date = prev_exec_date
        super().__init__(*args, **kwargs)

    def execute(self, context):
        mongoHook = MongoHook(conn_id=self.mongo_conn_id)
        self.mongo_db = mongoHook.connection.schema
        log.info('postgres_conn_id: %s', self.postgres_conn_id)
        log.info('mongo_conn_id: %s', self.mongo_conn_id)
        log.info('postgres_sql: %s', self.postgres_sql)
        # log.info('prev_exec_date: %s', self.prev_exec_date)
        log.info('mongo_db: %s', self.mongo_db)
        log.info('mongo_collection: %s', self.mongo_collection)
        
        well_data = self.get_data()
        most_recent_date = Variable.get("most_recent_date")
        print(most_recent_date)
        filter_query = None
        for index, well in well_data.iterrows():
            if well is not None and well['is_newly_added']:
                print('newly added')
                filter_query = { "Name" : { "$eq" : well['well_name']} }                
            else:
                print('old well')
                filter_query = {"$and" : [{ "Name" : { "$eq" : well['well_name']} },{ "Date" : { "$gt" : most_recent_date} }]}
                # filter_query = { "Date" : { "$gt" : most_recent_date } }
            
            log.info('mongo filter query: %s', filter_query)
            mongo_well_list = self.transform(mongoHook.get_collection(self.mongo_collection).find(filter_query))
            print(len(mongo_well_list))
            if len(mongo_well_list) > 0:                
                for doc in mongo_well_list:
                    doc["water_cut_calc"] = utils.calc_watercut(doc['OIL_bopd'], doc['WATER_bwpd'])
                    doc["gor_calc"] = utils.calc_gor(doc['OIL_bopd'], doc['GAS_mscfd'])
                               
                self.update_records(mongoHook, filter_query, mongo_well_list)
                    
    
    def get_data(self):
        pgHook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        with closing(pgHook.get_conn()) as conn:
            df = pd.read_sql(self.postgres_sql, conn)
            return df
            
    def update_records(self, mongoHook, filterDoc, updateDocs):
        log.info('updating mongo db')
        if len(updateDocs) == 1:
            mongoHook.replace_one(self.mongo_collection,
                                updateDocs[0],
                                None,
                                mongo_db=self.mongo_db)
        else:
            mongoHook.replace_many(self.mongo_collection,
                                updateDocs,
                                None,
                                mongo_db=self.mongo_db)
            
    def transform(self, docs):
        """
        docs is a pyMongo cursor of documents and cursor just needs to be
            converted into an array.
        """
        return [doc for doc in docs]