import os
import requests
import psycopg2

def fetch_and_store_stock_data():
    API_KEY = os.getenv("STOCK_API_KEY")
    DB_HOST = os.getenv("DB_HOST")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")

    SYMBOL = "AAPL"

    try:
        url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={SYMBOL}&apikey={API_KEY}"
        response = requests.get(url)
        data = response.json()

        if "Global Quote" not in data:
            print("Missing data in API response")
            return

        quote = data["Global Quote"]

        price = quote.get("05. price")
        volume = quote.get("06. volume")
        latest_trading_day = quote.get("07. latest trading day")

        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_prices (
                symbol TEXT,
                price FLOAT,
                volume BIGINT,
                trading_day DATE,
                fetched_at TIMESTAMP DEFAULT NOW()
            )
        """)

        cursor.execute("""
            INSERT INTO stock_prices (symbol, price, volume, trading_day)
            VALUES (%s, %s, %s, %s)
        """, (SYMBOL, price, volume, latest_trading_day))

        conn.commit()
        cursor.close()
        conn.close()

        print("Data inserted successfully!")

    except Exception as e:
        print("Error:", e)
