from ssl import CERT_NONE
from types import TracebackType
from typing import List, Optional, Type

import pymongo
from pymongo import MongoClient, ReplaceOne

from airflow.hooks.base import BaseHook


class MongoHook(BaseHook):

    conn_name_attr = 'conn_id'
    default_conn_name = 'mongo_default'
    conn_type = 'mongo'
    hook_name = 'MongoDB'

    def __init__(self, conn_id: str = default_conn_name, *args, **kwargs) -> None:

        super().__init__()
        self.mongo_conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson.copy()
        self.client = None

        srv = self.extras.pop('srv', False)
        scheme = 'mongodb+srv' if srv else 'mongodb'

        self.uri = '{scheme}://{creds}{host}{port}/{database}'.format(
            scheme=scheme,
            creds=f'{self.connection.login}:{self.connection.password}@' if self.connection.login else '',
            host=self.connection.host,
            port='' if self.connection.port is None else f':{self.connection.port}',
            database=self.connection.schema,
        )

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if self.client is not None:
            self.close_conn()

    def get_conn(self) -> MongoClient:
        """Fetches PyMongo Client"""
        if self.client is not None:
            return self.client

        # Mongo Connection Options dict that is unpacked when passed to MongoClient
        options = self.extras

        # If we are using SSL disable requiring certs from specific hostname
        if options.get('ssl', False):
            options.update({'ssl_cert_reqs': CERT_NONE})

        self.client = MongoClient(self.uri, **options)

        return self.client

    def close_conn(self) -> None:
        """Closes connection"""
        client = self.client
        if client is not None:
            client.close()
            self.client = None

    def get_collection(
        self, mongo_collection: str, mongo_db: Optional[str] = None
    ) -> pymongo.collection.Collection:
        """
        Fetches a mongo collection object for querying.

        Uses connection schema as DB unless specified.
        """
        mongo_db = mongo_db if mongo_db is not None else self.connection.schema
        mongo_conn: MongoClient = self.get_conn()

        return mongo_conn.get_database(mongo_db).get_collection(mongo_collection)

    def aggregate(
        self, mongo_collection: str, aggregate_query: list, mongo_db: Optional[str] = None, **kwargs
    ) -> pymongo.command_cursor.CommandCursor:
        """
        Runs an aggregation pipeline and returns the results
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.aggregate(aggregate_query, **kwargs)

    def find(
        self,
        mongo_collection: str,
        query: dict,
        find_one: bool = False,
        mongo_db: Optional[str] = None,
        **kwargs,
    ) -> pymongo.cursor.Cursor:
        """
        Runs a mongo find query and returns the results
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        if find_one:
            return collection.find_one(query, **kwargs)
        else:
            return collection.find(query, **kwargs)

    def insert_one(
        self, mongo_collection: str, doc: dict, mongo_db: Optional[str] = None, **kwargs
    ) -> pymongo.results.InsertOneResult:
        """
        Inserts a single document into a mongo collection
        """
        print(mongo_collection)
        print(mongo_db)
        
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.insert_one(doc, **kwargs)

    def insert_many(
        self, mongo_collection: str, docs: dict, mongo_db: Optional[str] = None, **kwargs
    ) -> pymongo.results.InsertManyResult:
        """
        Inserts many docs into a mongo collection.
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)
        print(mongo_collection)
        print(mongo_db)
        print(docs)

        return collection.insert_many(docs, **kwargs)

    def update_one(
        self,
        mongo_collection: str,
        filter_doc: dict,
        update_doc: dict,
        mongo_db: Optional[str] = None,
        **kwargs,
    ) -> pymongo.results.UpdateResult:
        """
        Updates a single document in a mongo collection.
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.update_one(filter_doc, update_doc, **kwargs)

    def update_many(
        self,
        mongo_collection: str,
        filter_doc: dict,
        update_doc: dict,
        mongo_db: Optional[str] = None,
        **kwargs,
    ) -> pymongo.results.UpdateResult:
        """
        Updates one or more documents in a mongo collection.
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.update_many(filter_doc, update_doc, **kwargs)

    def replace_one(
        self,
        mongo_collection: str,
        doc: dict,
        filter_doc: Optional[dict] = None,
        mongo_db: Optional[str] = None,
        **kwargs,
    ) -> pymongo.results.UpdateResult:
        """
        Replaces a single document in a mongo collection.
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        if not filter_doc:
            filter_doc = {'_id': doc['_id']}

        return collection.replace_one(filter_doc, doc, **kwargs)

    def replace_many(
        self,
        mongo_collection: str,
        docs: List[dict],
        filter_docs: Optional[List[dict]] = None,
        mongo_db: Optional[str] = None,
        upsert: bool = False,
        collation: Optional[pymongo.collation.Collation] = None,
        **kwargs,
    ) -> pymongo.results.BulkWriteResult:
        """
        Replaces many documents in a mongo collection.
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        if not filter_docs:
            filter_docs = [{'_id': doc['_id']} for doc in docs]

        requests = [
            ReplaceOne(filter_docs[i], docs[i], upsert=upsert, collation=collation) for i in range(len(docs))
        ]

        return collection.bulk_write(requests, **kwargs)

    def delete_one(
        self, mongo_collection: str, filter_doc: dict, mongo_db: Optional[str] = None, **kwargs
    ) -> pymongo.results.DeleteResult:
        """
        Deletes a single document in a mongo collection.
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.delete_one(filter_doc, **kwargs)

    def delete_many(
        self, mongo_collection: str, filter_doc: dict, mongo_db: Optional[str] = None, **kwargs
    ) -> pymongo.results.DeleteResult:
        """
        Deletes one or more documents in a mongo collection.
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.delete_many(filter_doc, **kwargs)
