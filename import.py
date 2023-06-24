import os
import sys
import csv
from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

load_dotenv()

db_credentials = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}
db_table = os.getenv("DB_TABLE")
csv_file_name = os.getenv("CSV_FILE_NAME") or (sys.argv[1] if len(sys.argv) > 1 else None)


def clean_data(headers, row, data_cleaning_dict):
    for i, value in enumerate(row):
        header = headers[i]
        if header in data_cleaning_dict:
            if value == data_cleaning_dict[header]["search"]:
                row[i] = data_cleaning_dict[header]["replace"]
            elif value == "" and "default" in data_cleaning_dict[header]:
                row[i] = data_cleaning_dict[header]["default"]

        if header == "Date":
            row.append(value[:10])

    return row


data_cleaning_dict = {
    "Date": {"search": "T", "replace": " "},
    "Commissions": {"search": "--", "replace": "0"},
    "Value": {"default": "0"},
    "Quantity": {"default": "0"},
    "Average Price": {"default": "0"},
    "Fees": {"default": "0"},
    "Multiplyer": {"default": "1"},
}

with connect(**db_credentials) as conn:
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {db_table} (
                date date,
                type text,
                action text,
                symbol text,
                instrument_type text,
                description text,
                value numeric DEFAULT 0,
                quantity numeric DEFAULT 0,
                average_price numeric DEFAULT 0,
                commissions numeric DEFAULT 0,
                fees numeric DEFAULT 0,
                multiplier numeric DEFAULT 1,
                root_symbol text,
                underlying_symbol text,
                expiration_date text,
                strike_price text,
                call_or_put text,
                order_number text,
                trade_day date
            )
            """
        )

        with open(csv_file_name, "r") as f:
            reader = csv.reader(f)
            headers = next(reader)

            for row in reader:
                cleaned_row = clean_data(headers, row, data_cleaning_dict)

                cursor.execute(
                    f"""
                    INSERT INTO {db_table} (
                        date, type, action, symbol, instrument_type,
                        description, value, quantity, average_price,
                        commissions, fees, multiplier, root_symbol,
                        underlying_symbol, expiration_date, strike_price,
                        call_or_put, order_number, trade_day
                    ) VALUES %s""",
                    (tuple(cleaned_row),),
                )
