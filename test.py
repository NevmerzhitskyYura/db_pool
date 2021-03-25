import threading
import unittest
import multiprocessing
import psycopg2

import dbpool as db


def create_user():
    with db.pool_manager() as conn:
        cursor = conn.cursor()
        query = "insert into public.user  values ('sndfsd', 123);"
        cursor.execute(query)
        conn.commit()


def create_without_pool():
    connection = psycopg2.connect(dbname="postgres", user="postgres", password="postgres", host='localhost')
    cursor = connection.cursor()
    query = "insert into public.user  values ('sndfsd', 123);"
    cursor.execute(query)
    connection.commit()
    cursor.close()
    connection.close()


class UserTest(unittest.TestCase):
    # def test1(self):
    #     for _ in range(100):
    #         create_user()

    # def test2(self):
    #     for _ in range(100):
    #         create_without_pool()

    def test3(self):
        processes = [threading.Thread(target=create_user) for _ in range(1000)]
        for process in processes:
            process.start()
        for process in processes:
            process.join()


    # def test4(self):
    #     processes = [threading.Thread(target=create_without_pool) for _ in range(1000)]
    #     for process in processes:
    #         process.start()
    #     for process in processes:
    #         process.join()


if __name__ == "__main__":
    unittest.main()
