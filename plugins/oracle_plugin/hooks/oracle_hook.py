from datetime import datetime
from typing import List, Optional

import cx_Oracle
import numpy

from airflow.hooks.dbapi import DbApiHook


class OracleHook(DbApiHook):
    """Interact with Oracle SQL."""

    conn_name_attr = 'oracle_conn_id'
    default_conn_name = 'oracle_default'
    conn_type = 'oracle'
    hook_name = 'Oracle'

    supports_autocommit = False

    # pylint: disable=c-extension-no-member
    def get_conn(self) -> 'OracleHook':
        """
        Returns a oracle connection object
        """
        conn = self.get_connection(
            self.oracle_conn_id  # type: ignore[attr-defined]  # pylint: disable=no-member
        )
        conn_config = {'user': conn.login, 'password': conn.password}
        dsn = conn.extra_dejson.get('dsn')
        sid = conn.extra_dejson.get('sid')
        mod = conn.extra_dejson.get('module')

        service_name = conn.extra_dejson.get('service_name')
        port = conn.port if conn.port else 1521
        if dsn and sid and not service_name:
            conn_config['dsn'] = cx_Oracle.makedsn(dsn, port, sid)
        elif dsn and service_name and not sid:
            conn_config['dsn'] = cx_Oracle.makedsn(dsn, port, service_name=service_name)
        else:
            conn_config['dsn'] = conn.host

        if 'encoding' in conn.extra_dejson:
            conn_config['encoding'] = conn.extra_dejson.get('encoding')
            # if `encoding` is specific but `nencoding` is not
            # `nencoding` should use same values as `encoding` to set encoding, inspired by
            # https://github.com/oracle/python-cx_Oracle/issues/157#issuecomment-371877993
            if 'nencoding' not in conn.extra_dejson:
                conn_config['nencoding'] = conn.extra_dejson.get('encoding')
        if 'nencoding' in conn.extra_dejson:
            conn_config['nencoding'] = conn.extra_dejson.get('nencoding')
        if 'threaded' in conn.extra_dejson:
            conn_config['threaded'] = conn.extra_dejson.get('threaded')
        if 'events' in conn.extra_dejson:
            conn_config['events'] = conn.extra_dejson.get('events')

        mode = conn.extra_dejson.get('mode', '').lower()
        if mode == 'sysdba':
            conn_config['mode'] = cx_Oracle.SYSDBA
        elif mode == 'sysasm':
            conn_config['mode'] = cx_Oracle.SYSASM
        elif mode == 'sysoper':
            conn_config['mode'] = cx_Oracle.SYSOPER
        elif mode == 'sysbkp':
            conn_config['mode'] = cx_Oracle.SYSBKP
        elif mode == 'sysdgd':
            conn_config['mode'] = cx_Oracle.SYSDGD
        elif mode == 'syskmt':
            conn_config['mode'] = cx_Oracle.SYSKMT
        elif mode == 'sysrac':
            conn_config['mode'] = cx_Oracle.SYSRAC

        purity = conn.extra_dejson.get('purity', '').lower()
        if purity == 'new':
            conn_config['purity'] = cx_Oracle.ATTR_PURITY_NEW
        elif purity == 'self':
            conn_config['purity'] = cx_Oracle.ATTR_PURITY_SELF
        elif purity == 'default':
            conn_config['purity'] = cx_Oracle.ATTR_PURITY_DEFAULT

        conn = cx_Oracle.connect(**conn_config)
        if mod is not None:
            conn.module = mod

        return conn

    def insert_rows(
        self,
        table: str,
        rows: List[tuple],
        target_fields=None,
        commit_every: int = 1000,
        replace: Optional[bool] = False,
        **kwargs,
    ) -> None:
        """
        A generic way to insert a set of tuples into a table,
        the whole set of inserts is treated as one transaction
        Changes from standard DbApiHook implementation:
        """
        if target_fields:
            target_fields = ', '.join(target_fields)
            target_fields = f'({target_fields})'
        else:
            target_fields = ''
        conn = self.get_conn()
        cur = conn.cursor()  # type: ignore[attr-defined]
        if self.supports_autocommit:
            cur.execute('SET autocommit = 0')
        conn.commit()  # type: ignore[attr-defined]
        i = 0
        for row in rows:
            i += 1
            lst = []
            for cell in row:
                if isinstance(cell, str):
                    lst.append("'" + str(cell).replace("'", "''") + "'")
                elif cell is None:
                    lst.append('NULL')
                elif isinstance(cell, float) and numpy.isnan(cell):  # coerce numpy NaN to NULL
                    lst.append('NULL')
                elif isinstance(cell, numpy.datetime64):
                    lst.append("'" + str(cell) + "'")
                elif isinstance(cell, datetime):
                    lst.append(
                        "to_date('" + cell.strftime('%Y-%m-%d %H:%M:%S') + "','YYYY-MM-DD HH24:MI:SS')"
                    )
                else:
                    lst.append(str(cell))
            values = tuple(lst)
            sql = f"INSERT /*+ APPEND */ INTO {table} {target_fields} VALUES ({','.join(values)})"
            cur.execute(sql)
            if i % commit_every == 0:
                conn.commit()  # type: ignore[attr-defined]
                self.log.info('Loaded %s into %s rows so far', i, table)
        conn.commit()  # type: ignore[attr-defined]
        cur.close()
        conn.close()  # type: ignore[attr-defined]
        self.log.info('Done loading. Loaded a total of %s rows', i)

    def bulk_insert_rows(
        self,
        table: str,
        rows: List[tuple],
        target_fields: Optional[List[str]] = None,
        commit_every: int = 5000,
    ):
        """
        A performant bulk insert for cx_Oracle
        that uses prepared statements via `executemany()`.
        For best performance, pass in `rows` as an iterator.
        """
        if not rows:
            raise ValueError("parameter rows could not be None or empty iterable")
        conn = self.get_conn()
        cursor = conn.cursor()  # type: ignore[attr-defined]
        values_base = target_fields if target_fields else rows[0]
        prepared_stm = 'insert into {tablename} {columns} values ({values})'.format(
            tablename=table,
            columns='({})'.format(', '.join(target_fields)) if target_fields else '',
            values=', '.join(':%s' % i for i in range(1, len(values_base) + 1)),
        )
        row_count = 0
        # Chunk the rows
        row_chunk = []
        for row in rows:
            row_chunk.append(row)
            row_count += 1
            if row_count % commit_every == 0:
                cursor.prepare(prepared_stm)
                cursor.executemany(None, row_chunk)
                conn.commit()  # type: ignore[attr-defined]
                self.log.info('[%s] inserted %s rows', table, row_count)
                # Empty chunk
                row_chunk = []
        # Commit the leftover chunk
        cursor.prepare(prepared_stm)
        cursor.executemany(None, row_chunk)
        conn.commit()  # type: ignore[attr-defined]
        self.log.info('[%s] inserted %s rows', table, row_count)
        cursor.close()
        conn.close()  # type: ignore[attr-defined]
