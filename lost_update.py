import psycopg2
from utils import connection_pool, main


def lost_update_counter():
    with connection_pool.getconn() as conn:
        with conn.cursor() as cursor:
            try:
                for i in range(1, 10000):
                    conn.autocommit = False
                    cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE")
                    counter = cursor.fetchone()[0]
                    counter = counter + 1
                    cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, 1))
                    conn.commit()
            except psycopg2.Error as e:
                print(f"Database error: {e}")
                conn.rollback()


if __name__ == "__main__":
    main(counter=lost_update_counter)
