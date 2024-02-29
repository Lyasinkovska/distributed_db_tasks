import psycopg2
from utils import connection_pool, main


def lost_update_counter():
    with connection_pool.getconn() as conn:
        with conn.cursor() as cursor:
            try:
                for i in range(1, 10000):
                    conn.autocommit = False
                    cursor.execute("UPDATE user_counter SET counter = counter + %s WHERE user_id = %s", (1, 1))
                    conn.commit()
            except psycopg2.Error as e:
                print(f"Database error: {e}")
                conn.rollback()


if __name__ == "__main__":
    main(counter=lost_update_counter)
