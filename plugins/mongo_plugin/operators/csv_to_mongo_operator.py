from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.contrib.hooks.fs_hook import FSHook
from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults

import os
import logging as log
import pandas as pd

class CsvToMongoOperator(BaseOperator):
    @apply_defaults
    def __init__(self, fs_conn_id, mongo_conn_id, mongo_collection, mongo_db='', *args, **kwargs):
        self.fs_conn_id = fs_conn_id
        self.mongo_conn_id = mongo_conn_id        
        self.mongo_collection = mongo_collection        
        super().__init__(*args, **kwargs)

    def execute(self, context):
        fileHook = FSHook(conn_id=self.fs_conn_id)
        mongoHook = MongoHook(conn_id=self.mongo_conn_id)
        self.mongo_db = mongoHook.connection.schema
        # file_name = Variable.get("downhole_equipment_filter_filename")
        file_name = Variable.get("flowback_data_filter_filename")
        
        print(file_name)
        path = os.path.join(fileHook.get_path(), file_name)
        
        log.info('source_file_path: %s', path)
        log.info('mongo_db: %s', self.mongo_db)
        log.info('mongo_collection: %s', self.mongo_collection)
        df = pd.read_csv(path)
        
        data_dict = df.to_dict("records")
        print(data_dict)
        # mongoHook.insert_many(self.mongo_collection, data_dict, self.mongo_db)
        self.insert_records(mongoHook, data_dict)
        mongoHook.close_conn()
        
    def insert_records(self, mongoHook, docs):
        if len(docs) == 1:
            mongoHook.insert_one(self.mongo_collection,
                             docs[0],
                             mongo_db=self.mongo_db)
        else:
            mongoHook.insert_many(self.mongo_collection,
                              docs,
                              mongo_db=self.mongo_db)