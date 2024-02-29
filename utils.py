import os
import threading
import time
from functools import wraps
from dotenv import load_dotenv
import psycopg2
from psycopg2 import pool

from custom_logger import logger

load_dotenv()

conn_params = {
    'dbname': os.getenv('POSTGRES_DB_NAME'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST')
}

min_conn, max_conn = 1, 10
connection_pool = pool.ThreadedConnectionPool(min_conn, max_conn, **conn_params)


def create_table(config):
    commands = """
    CREATE TABLE IF NOT EXISTS user_counter (
        user_id SERIAL PRIMARY KEY,
        counter INTEGER DEFAULT 0,
        version INTEGER DEFAULT 0
    );
    UPDATE user_counter SET user_id = 1, counter = 0, version = 0;
    """
    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                for command in commands:
                    cur.execute(command)
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)


def duration(func):
    @wraps(func)
    def duration_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        logger.info(f'Function {kwargs.get("counter").__name__} took {total_time:.4f} seconds')
        return result

    return duration_wrapper


@duration
def main(counter):
    threads = []
    print('starting ....')
    for _ in range(10):
        thread = threading.Thread(target=counter)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    create_table(conn_params)
