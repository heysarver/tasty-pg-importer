import os
import sys
import psycopg2
import csv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

# Load .env file if exists
load_dotenv()

# Get database credentials and csv file name from environment variables and command line arguments
db_name = os.getenv('DB_NAME')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_table = os.getenv('DB_TABLE')
csv_file_name = os.getenv('CSV_FILE_NAME')

if len(sys.argv) > 1:
    csv_file_name = sys.argv[1]

# Function to clean up data
def clean_data(headers, row, search_replace_dict, default_value_dict):
    for i, value in enumerate(row):
        if headers[i] in search_replace_dict and value == search_replace_dict[headers[i]][0]:
            row[i] = search_replace_dict[headers[i]][1]
        elif headers[i] in default_value_dict and value == '':
            row[i] = default_value_dict[headers[i]]
        if headers[i] == 'Date':
            row.append(value[:10])  # Add "trade_day" to the end of the row
    return row

# Connect to the database
conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()

# Create table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS activity (
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
""")

# Define search and replace values and default values
search_replace_dict = {
                        'Date': ['T', ' '],
                        'Commissions': ['--', '0'],
                        'column2': ['search_value', 'replace_value']
                      }
default_value_dict = {
                        'Value': '0',
                        'Quantity': '0',
                        'Average Price': '0',
                        'Commissions': '0',
                        'Fees': '0',
                        'Multiplyer': '1'
                      }

# Load CSV file into the table
with open(csv_file_name, 'r') as f:
    reader = csv.reader(f)
    headers = next(reader)  # Get the header row
    for row in reader:
        # Clean up the data
        row = clean_data(headers, row, search_replace_dict, default_value_dict)
        cursor.execute(
            f"""
            INSERT INTO {db_table} (
                "date",
                type,
                action,
                symbol,
                instrument_type,
                description,
                value,
                quantity,
                average_price,
                commissions,
                fees,
                multiplier,
                root_symbol,
                underlying_symbol,
                expiration_date,
                strike_price,
                call_or_put,
                order_number,
                trade_day
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            row
        )

# Commit the transaction
conn.commit()

# Close the connection
cursor.close()
conn.close()
