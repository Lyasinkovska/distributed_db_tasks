import psycopg2
from utils import connection_pool, main


def optimistic_concurrency_control_counter():
    with connection_pool.getconn() as conn:
        with conn.cursor() as cursor:
            try:
                for _ in range(10000):
                    conn.autocommit = False
                    while True:
                        cursor.execute("SELECT counter, version FROM user_counter WHERE user_id = 1")
                        counter, version = cursor.fetchone()
                        # print(counter, version)
                        counter += 1
                        cursor.execute(
                            "UPDATE user_counter SET counter = %s, version = %s WHERE user_id = %s AND version = %s",
                            (counter, version + 1, 1, version))
                        conn.commit()
                        if cursor.rowcount > 0:
                            break
            except psycopg2.Error as e:
                print(f"Database error: {e}")
                if conn:
                    conn.rollback()


if __name__ == "__main__":
    main(counter=optimistic_concurrency_control_counter)
