import logging
import threading
import time

import psycopg2

from db_settings import db_settings as dbs

pool_delay = 0.1

# logging.basicConfig(level=logging.INFO, filename='dbpool.txt', filemode='w', format='%(message)s')
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

count = 0


class DataBasePool:
    def __init__(self, dbname, user, password, host, port, pool_size):
        self._db_name = dbname
        self._user = user
        self._password = password
        self._host = host
        self._port = port

        self._pool_size = pool_size
        self._connection_pool = [self._create_connection() for _ in range(self._pool_size)]
        self.lock = threading.RLock()
        self.logging = logging.getLogger('dbpool')
        self.logging.info("Create new pool")
        self.active_connection = []

    def __del__(self):
        self.logging.info('Close all connections in pool')
        for connection in self._connection_pool:
            self._close_connection(connection)

    def __enter__(self):
        with self.lock:
            conn = self._get_connection()
            self.active_connection.append(conn)
        return conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._push_to_pull(self.active_connection.pop(0))

    def _create_connection(self):
        connection = psycopg2.connect(dbname=self._db_name, user=self._user, password=self._password, host=self._host)
        return connection

    def _close_connection(self, connection):
        self.logging.info(f"Close connection with {id(connection)}")
        connection.close()

    def _get_connection(self):
        connection = None
        while not connection:
            try:
                connection = self._connection_pool.pop()
                logging.info(f'Get connection from pool with id {id(connection)}')
                self.logging.info(f'{[id(x) for x in self._connection_pool]}')
            except:
                time.sleep(pool_delay)
                self.logging.info(f"Poll don't have available connection. Please wait.")
        return connection

    def _push_to_pull(self, connection):
        self.logging.info(f'Push to the pull connetion with id {id(connection)}')
        self._connection_pool.append(connection)


def pool_manager():
    return pool_instance


pool_instance = DataBasePool(**dbs, pool_size=50)
