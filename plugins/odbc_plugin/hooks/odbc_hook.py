from typing import Any, Optional
from urllib.parse import quote_plus

import pyodbc

from airflow.hooks.dbapi import DbApiHook
from airflow.utils.helpers import merge_dicts


class OdbcHook(DbApiHook):
    DEFAULT_SQLALCHEMY_SCHEME = 'mssql+pyodbc'
    conn_name_attr = 'odbc_conn_id'
    default_conn_name = 'odbc_default'
    conn_type = 'odbc'
    hook_name = 'ODBC'
    supports_autocommit = True

    def __init__(
        self,
        *args,
        database: Optional[str] = None,
        driver: Optional[str] = None,
        dsn: Optional[str] = None,
        connect_kwargs: Optional[dict] = None,
        sqlalchemy_scheme: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._database = database
        self._driver = driver
        self._dsn = dsn
        self._conn_str = None
        self._sqlalchemy_scheme = sqlalchemy_scheme
        self._connection = None
        self._connect_kwargs = connect_kwargs

    @property
    def connection(self):
        if not self._connection:
            self._connection = self.get_connection(getattr(self, self.conn_name_attr))
        return self._connection

    @property
    def database(self) -> Optional[str]:
        return self._database or self.connection.schema

    @property
    def sqlalchemy_scheme(self) -> Optional[str]:
        return (
            self._sqlalchemy_scheme
            or self.connection_extra_lower.get('sqlalchemy_scheme')
            or self.DEFAULT_SQLALCHEMY_SCHEME
        )

    @property
    def connection_extra_lower(self) -> dict:
        return {k.lower(): v for k, v in self.connection.extra_dejson.items()}

    @property
    def driver(self) -> Optional[str]:
        if not self._driver:
            driver = self.connection_extra_lower.get('driver')
            if driver:
                self._driver = driver
        return self._driver and self._driver.strip().lstrip('{').rstrip('}').strip()

    @property
    def dsn(self) -> Optional[str]:
        if not self._dsn:
            dsn = self.connection_extra_lower.get('dsn')
            if dsn:
                self._dsn = dsn.strip()
        return self._dsn

    @property
    def odbc_connection_string(self):
        if not self._conn_str:
            conn_str = ''
            if self.driver:
                conn_str += f"DRIVER={{{self.driver}}};"
            if self.dsn:
                conn_str += f"DSN={self.dsn};"
            if self.connection.host:
                conn_str += f"SERVER={self.connection.host};"
            database = self.database or self.connection.schema
            if database:
                conn_str += f"DATABASE={database};"
            if self.connection.login:
                conn_str += f"UID={self.connection.login};"
            if self.connection.password:
                conn_str += f"PWD={self.connection.password};"
            if self.connection.port:
                f"PORT={self.connection.port};"

            extra_exclude = {'driver', 'dsn', 'connect_kwargs', 'sqlalchemy_scheme'}
            extra_params = {
                k: v for k, v in self.connection.extra_dejson.items() if not k.lower() in extra_exclude
            }
            for k, v in extra_params.items():
                conn_str += f"{k}={v};"

            self._conn_str = conn_str
            print(self._conn_str)
        return self._conn_str

    @property
    def connect_kwargs(self) -> dict:

        def clean_bool(val):  # pylint: disable=inconsistent-return-statements
            if hasattr(val, 'lower'):
                if val.lower() == 'true':
                    return True
                elif val.lower() == 'false':
                    return False
            else:
                return val

        conn_connect_kwargs = self.connection_extra_lower.get('connect_kwargs', {})
        hook_connect_kwargs = self._connect_kwargs or {}
        merged_connect_kwargs = merge_dicts(conn_connect_kwargs, hook_connect_kwargs)

        if 'attrs_before' in merged_connect_kwargs:
            merged_connect_kwargs['attrs_before'] = {
                int(k): int(v) for k, v in merged_connect_kwargs['attrs_before'].items()
            }

        return {k: clean_bool(v) for k, v in merged_connect_kwargs.items()}

    def get_conn(self) -> pyodbc.Connection:
        """Returns a pyodbc connection object."""
        conn = pyodbc.connect(self.odbc_connection_string, **self.connect_kwargs)
        return conn

    def get_uri(self) -> str:
        """URI invoked in :py:meth:`~airflow.hooks.dbapi.DbApiHook.get_sqlalchemy_engine` method"""
        quoted_conn_str = quote_plus(self.odbc_connection_string)
        uri = f"{self.sqlalchemy_scheme}:///?odbc_connect={quoted_conn_str}"
        return uri

    def get_sqlalchemy_connection(
        self, connect_kwargs: Optional[dict] = None, engine_kwargs: Optional[dict] = None
    ) -> Any:
        """Sqlalchemy connection object"""
        engine = self.get_sqlalchemy_engine(engine_kwargs=engine_kwargs)
        cnx = engine.connect(**(connect_kwargs or {}))
        return cnx
