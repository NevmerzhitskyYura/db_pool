import threading
import time
from contextlib import contextmanager
import logging

import psycopg2

from db_settings import db_settings as dbs

pool_delay = 0.01

#logging.basicConfig(level=logging.INFO, filename='dbpool.txt', filemode='w', format='%(message)s')
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


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
        self.logging = logging.getLogger('dbpool')
        self.logging.info("Create new pool")

    def __del__(self):
        self.logging.info('Close all connections in pool')
        for connection in self._connection_pool:
            self._close_connection(connection)

    def _create_connection(self):
        connection = psycopg2.connect(dbname=self._db_name, user=self._user, password=self._password, host=self._host)

        return {'connection': connection,
                'creation_date': time.time()}

    def _close_connection(self, connection):
        self.logging.info(f"Close connection with {id(connection)}")
        connection["connection"].close()

    def _get_connection(self):
        connection = None
        while not connection:
            if self._connection_pool:
                connection = self._connection_pool.pop()
                logging.info(f'Get connection from pool with id {id(connection)}')
            elif len(self._connection_pool) < self._pool_size:
                connection = self._create_connection()
                self.logging.info(f"Create connection with id {id(connection)}")
            time.sleep(pool_delay)
        return connection

    @contextmanager
    def manager(self):

        with self.lock:
            connection = self._get_connection()

        yield connection['connection']


        if connection['creation_date'] + self.connection_ttl > time.time():
            self._push_to_pull(connection)
        else:
            self._close_connection(connection)

    def _push_to_pull(self, connection):
        self.logging.info(f'Push to the pull connetion with {id(connection)}')
        self._connection_pool.append(connection)

def pool_manager():
    return pool_instance


pool_instance= DataBasePool(**dbs, pool_size=10, ttl=0.015)

