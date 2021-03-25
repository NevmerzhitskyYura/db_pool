import logging
import time
from queue import Queue

import psycopg2

from db_settings import db_settings as dbs

pool_delay = 0.1

# logging.basicConfig(level=logging.INFO, filename='dbpool.txt', filemode='w', format='%(message)s')
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


class DataBasePool:
    def __init__(self, dbname, user, password, host, port, pool_size):
        self._db_name = dbname
        self._user = user
        self._password = password
        self._host = host
        self._port = port

        self._pool_size = pool_size
        self._connection_pool = Queue(maxsize=self._pool_size)
        for _ in range(self._pool_size):
            self._connection_pool.put(self._create_connection())

        self.logging = logging.getLogger('dbpool')
        self.logging.info("Create new pool")

    def __del__(self):
        self.logging.info('Close all connections in pool')
        while not self._connection_pool.empty():
            conn = self._connection_pool.get()
            self._close_connection(conn)

    def _create_connection(self):
        connection = psycopg2.connect(dbname=self._db_name, user=self._user, password=self._password, host=self._host)
        return connection

    def _close_connection(self, connection):
        self.logging.info(f"Close connection with {id(connection)}")
        connection.close()

    def _get_connection(self):
        connection = None
        while not connection:
            if not self._connection_pool.empty():
                connection = self._connection_pool.get()
                logging.info(f'Get connection from pool with id {id(connection)}')
            else:
                time.sleep(pool_delay)
                self.logging.info(f"Poll don't have available connection. Please wait.")
        return connection


class Call():
    def __init__(self, conn, pool):
        self.conn = conn
        self.pool = pool
        self.logging = logging.getLogger('dbpool2')
        self.logging.info('Initialize call')

    def __enter__(self):
        self.logging.info('Enter')
        self.logging.info(f'Pool size after enter {self.pool.qsize()}')
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logging.info('Exit')

        self.pool.put(self.conn)
        self.logging.info(f'Pool size after exit {self.pool.qsize()}')


pool_instance = DataBasePool(**dbs, pool_size=5)

def pool_manager():
    return Call(pool_instance._get_connection(), pool_instance._connection_pool)
