import os
import sys
import csv
from psycopg2 import connect, sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

load_dotenv()

db_credentials = {
    "dbname": os.getenv("DB_NAME", "mydb"),
    "user": os.getenv("DB_USER", "user"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "host": os.getenv("DB_HOST", "postgres"),
    "port": os.getenv("DB_PORT", "5432"),
}
db_table = os.getenv("DB_TABLE", "activity")
csv_file_name = os.getenv("CSV_FILE_NAME") or (sys.argv[1] if len(sys.argv) > 1 else None)

# Check if csv_file_name is not None
if csv_file_name is None:
    raise ValueError("CSV filename not found in environment variables or command line arguments")

def clean_data(headers, row, data_cleaning_dict):
    for i, value in enumerate(row):
        if i < len(headers):
            header = headers[i]
            if header in data_cleaning_dict:
                if "search" in data_cleaning_dict[header] and value == data_cleaning_dict[header]["search"]:
                    row[i] = data_cleaning_dict[header]["replace"]
                elif value == "" and "default" in data_cleaning_dict[header]:
                    row[i] = data_cleaning_dict[header]["default"]

            if header == "Date":
                # Keep the full date-time string, including the time part
                row.append(value)
            else:
                # Remove commas if the value is numeric
                try:
                    row[i] = float(value.replace(",", ""))
                except ValueError:
                    pass

    return row


data_cleaning_dict = {
    "Commissions": {"search": "--", "replace": "0", "default": "0"},
    "Value": {"search": ",", "replace": "", "default": "0"},
    "Quantity": {"search": ",", "replace": "", "default": "0"},
    "Average Price": {"search": ",", "replace": "", "default": "0"},
    "Fees": {"search": ",", "replace": "", "default": "0"},
    "Multiplier": {"search": ",", "replace": "", "default": "1"},
}

try:
    with connect(**db_credentials) as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        with conn.cursor() as cursor:
            # Use SQL-identifier instead of string formatting
            create_table_query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {} (
                    date text,
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
            """).format(sql.Identifier(db_table))

            cursor.execute(create_table_query)

            create_pl_view_query = sql.SQL("""
                CREATE OR REPLACE VIEW daily_profit_loss_with_running_total AS
                WITH profit_loss_by_day AS (
                    SELECT trade_day, SUM(value) AS profit_loss, SUM(value+commissions+fees) AS profit_loss_fees
                    FROM activity
                    where description not like 'ACH%' and description not like '%mark to market%'
                    GROUP BY trade_day
                )
                SELECT trade_day, profit_loss, profit_loss_fees, SUM(profit_loss) OVER (ORDER BY trade_day) AS running_total, SUM(profit_loss_fees) OVER (ORDER BY trade_day) AS running_total_fees
                FROM profit_loss_by_day
            """).format(sql.Identifier(db_table))

            cursor.execute(create_pl_view_query)

            with open(csv_file_name, "r") as f:
                reader = csv.reader(f)
                headers = next(reader)

                for row in reader:
                    cleaned_row = clean_data(headers, row, data_cleaning_dict)

                    # Use a parameterized query instead of string formatting
                    insert_query = sql.SQL("""
                        INSERT INTO {} (
                            date, type, action, symbol, instrument_type,
                            description, value, quantity, average_price,
                            commissions, fees, multiplier, root_symbol,
                            underlying_symbol, expiration_date, strike_price,
                            call_or_put, order_number, trade_day
                        ) VALUES %s
                    """).format(sql.Identifier(db_table))

                    cursor.execute(insert_query, (tuple(cleaned_row),))
except Exception as error:
    print(f"An error occurred: {error}")
