import psycopg2
import requests
from retrying import retry

DB_PARAMS = {
    'dbname': 'ciastore',
    'user': 'USER',
    'password': "PASSWORD",
    'host': 'produsa-ciastoredb.fanthreesixty.com',
    'port': '5432'
}

PATCH_URL = "https://theuphoria.com/centrifuge/integration-message/replay"

HEADERS = {
    "Content-Type": "application/json",
}

SQL_QUERY = """
        SELECT DISTINCT integration_message_id 
        FROM centrifuge.cs_tm_note 
        WHERE active_ind = TRUE AND priority = 'Deleted' 
        LIMIT %s OFFSET %s;
    """

BATCH_SIZE = 25


@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def send_patch_request(data):
    response = requests.patch(PATCH_URL, json=data, headers=HEADERS)
    response.raise_for_status()
    return response


def main():
    connection = psycopg2.connect(**DB_PARAMS)
    cursor = connection.cursor()

    cursor.execute("SET search_path TO centrifuge")

    offset = 0

    while True:
        cursor.execute(SQL_QUERY, (BATCH_SIZE, offset))
        rows = cursor.fetchall()

        if not rows:
            break

        data_to_send = [str(row[0]) for row in rows]

        print(f"Batching rows {offset} to {offset + BATCH_SIZE}")

        try:
            response = send_patch_request(data_to_send)
            print(response.status_code, response.text)
        except requests.RequestException as e:
            print(f"Max retries reached. Error during PATCH request: {e}")

        offset += BATCH_SIZE

    # Close the cursor and connection
    cursor.close()
    connection.close()


if __name__ == '__main__':
    main()
