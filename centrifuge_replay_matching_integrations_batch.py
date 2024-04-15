import time
import psycopg2
import requests
from retrying import retry

DB_PARAMS = {
    'dbname': 'ciastore',
    'user': None,
    'password': None,
    'host': 'produsa-ciastoredb.fanthreesixty.com',
    'port': '5432'
}

PATCH_URL = "https://theuphoria.com/centrifuge/integration-message/replay"
HEADERS = {"Content-Type": "application/json"}

RESULTS_FILE = 'query_results.txt'
LAST_PROCESSED_ID_FILE = 'last_processed_id.txt'
ERROR_LOG_FILE = 'error_log.txt'

SQL_QUERY = """
        WITH RankedMessages AS (
            SELECT
                im.id,
                im.create_dt_tm,
                ROW_NUMBER() OVER (PARTITION BY im.id ORDER BY im.create_dt_tm ASC) AS rn
            FROM
                centrifuge.integration_message im
            WHERE
                table_name = 'tran_paciolan_stubhub'
        )
        SELECT
            id,
            create_dt_tm
        FROM
            RankedMessages
        WHERE
            rn = 1
        ORDER BY
            create_dt_tm ASC
    """


@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def send_patch_request(data):
    response = requests.patch(PATCH_URL, json=data, headers=HEADERS)
    response.raise_for_status()
    return response


def fetch_data_and_write_to_file():
    connection = psycopg2.connect(**DB_PARAMS)
    cursor = connection.cursor()
    cursor.execute("SET search_path TO centrifuge")
    cursor.execute(SQL_QUERY)
    rows = cursor.fetchall()

    with open(RESULTS_FILE, 'w') as f:
        for row in rows:
            f.write(f"{row[0]},{row[1]}\n")

    cursor.close()
    connection.close()


def read_from_file_and_send_requests():
    try:
        with open(LAST_PROCESSED_ID_FILE, 'r') as f:
            last_processed_id = f.read().strip()
    except FileNotFoundError:
        last_processed_id = ''

    with open(RESULTS_FILE, 'r') as f, open(ERROR_LOG_FILE, 'a') as error_log:
        for line in f:
            row_id, _ = line.strip().split(',', 1)
            if last_processed_id and row_id <= last_processed_id:
                continue

            data_to_send = [row_id]
            print(f"Sending data for integration_message_id: {data_to_send[0]}")

            try:
                response = send_patch_request(data_to_send)
                print(response.status_code, response.text)
            except requests.RequestException as e:
                error_msg = f"Error during PATCH request for integration_message_id {data_to_send[0]}: {e}\n"
                print(error_msg)
                error_log.write(error_msg)
            finally:
                with open(LAST_PROCESSED_ID_FILE, 'w') as f:
                    f.write(row_id)
            time.sleep(10)


def main():
    fetch_data_and_write_to_file()
    read_from_file_and_send_requests()


if __name__ == '__main__':
    main()
