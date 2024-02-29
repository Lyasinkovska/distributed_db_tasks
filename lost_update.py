import psycopg2
from utils import connection_pool, main


def lost_update_counter():
    with connection_pool.getconn() as conn:
        with conn.cursor() as cursor:
            try:
                for _ in range(10000):
                    conn.autocommit = False
                    cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
                    row = cursor.fetchone()
                    counter = row[0] if row else 0
                    counter += 1
                    cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, 1))
                    conn.commit()
            except psycopg2.Error as e:
                print(f"Database error: {e}")
                if conn:
                    conn.rollback()


if __name__ == "__main__":
    main(counter=lost_update_counter)
