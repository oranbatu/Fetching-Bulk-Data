import requests
import mysql.connector
from datetime import datetime, timedelta
import time

# Binance API URLs
api_url = "https://api.binance.com/api/v3/exchangeInfo"
api_url_2 = "https://api.binance.com/api/v3/klines"

# MySQL connection configuration
db_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="xxxxxxxxx",
    database="xxxxxxxxx"
)

# MySQL cursor
cursor = db_connection.cursor()

# Truncate the table to remove existing data
truncate_table_sql = "TRUNCATE TABLE kline_data;"
cursor.execute(truncate_table_sql)
db_connection.commit()

# Binance API: Fetch symbols ending with "USDT"
response = requests.get(api_url)
data = response.json()
symbols = [symbol_info['symbol'] for symbol_info in data['symbols'] if symbol_info['symbol'].endswith("USDT")]

# Time interval for Kline data (1 hour candles)
interval = "1h"

# Define start and end dates
start_date = int(datetime.strptime("2017-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ").timestamp() * 1000)
end_date = int(datetime.strptime("2023-09-21T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ").timestamp() * 1000)

# Loop through symbols and fetch Kline data
for symbol in symbols:
    current_start_date = start_date
    kline_data_to_insert = []  # Create a list to hold kline data for bulk insert

    while current_start_date < end_date:
        # API parameters
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": current_start_date,
            "endTime": current_start_date + (1000 * 60 * 60 * 1000),  # 1000 candles (1 hour each)
            "limit": 1000,
        }

        # Fetch Kline data from Binance API
        response = requests.get(api_url_2, params=params)
        data = response.json()

        # Process and append Kline data to the list
        for kline in data:
            kline_open_time = kline[0]
            kline_open_price = kline[1]
            kline_high_price = kline[2]
            kline_low_price = kline[3]
            kline_close_price = kline[4]

            kline_data_to_insert.append((symbol, kline_open_time, kline_open_price, kline_high_price, kline_low_price, kline_close_price))

        # Update the start date for the next batch
        current_start_date += (1000 * 60 * 60 * 1000)

        # Add a delay between API calls to avoid rate limiting
        time.sleep(1)

    # Bulk insert the Kline data for the current symbol
    insert_sql = "INSERT INTO kline_data (symbol, open_time, open_price, high_price, low_price, close_price) VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.executemany(insert_sql, kline_data_to_insert)
    print(f"{symbol} has fetched")
    db_connection.commit()

# Close the cursor and database connection
cursor.close()
db_connection.close()
