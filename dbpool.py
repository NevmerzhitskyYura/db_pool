import threading
import time

import psycopg2

from db_settings import db_settings as dbs

pool_instance = None
pool_delay = 0.1


class DataBasePool(object):
    def __init__(self, dbname, user, password, host, port, pool_size, ttl):
        self._db_name = dbname
        self._user = user
        self._password = password
        self._host = host
        self._port = port

        self._connection_pool = []
        self._pool_size = pool_size
        self.connection_ttl = ttl
        self.lock = threading.RLock()

    def _create_connection(self):
        connection = psycopg2.connect(**dbs)
        return {'connection': connection,
                'creation_date': time.time()}

    def _close_connection(self, connection):
        connection["connection"].close()

    def _get_connection(self):
        connection = None
        while not connection:
            if self._connection_pool:
                connection = self._connection_pool.pop()
            elif len(self._connection_pool) < self._pool_size:
                connection = self._create_connection()
            time.sleep(pool_delay)
        return connection

    def manager(self):
        with self.lock:
            connection = self._get_connection()

        yield connection['connection']

        if connection['creation_date'] + self.connection_ttl < time.time():
            self._connection_pool.append(connection)
        else:
            self._close_connection(connection)


def pool_manager():
    pool_instance = DataBasePool(**dbs, pool_size=10, ttl=1)
    return pool_instance


with pool_manager().manager() as conn:
    cursor = conn.cursor()
    query = "select * from "


