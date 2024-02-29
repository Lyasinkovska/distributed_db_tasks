import psycopg2
from utils import main, conn_params


def row_level_locking_counter():
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cursor:
            try:
                for _ in range(10000):
                    conn.autocommit = False
                    cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE")
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
    main(counter=row_level_locking_counter)
