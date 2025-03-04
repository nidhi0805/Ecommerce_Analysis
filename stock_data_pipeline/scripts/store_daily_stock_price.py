import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Snowflake Connection
conn = snowflake.connector.connect(
    user="nidhipatel066",
    password="J8xru@gndJ8xru@gnd",
    account="YYAZVZV-DOB64621",
    warehouse="COMPUTE_WH",
    database="STOCKS",
    schema="STOCK_PRICES"
    table="STOCK_PRICES_DAILY"
)

def store_daily_stock_prices():
    """Aggregate intraday stock prices into daily values for AI."""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO STOCK_PRICES_DAILY (Ticker, Date, Open, High, Low, Close, Volume)
        SELECT 
            Ticker,
            DATE(Timestamp) AS Date,
            FIRST_VALUE(Open) OVER (PARTITION BY Ticker, DATE(Timestamp) ORDER BY Timestamp) AS Open,
            MAX(High) AS High,
            MIN(Low) AS Low,
            LAST_VALUE(Close) OVER (PARTITION BY Ticker, DATE(Timestamp) ORDER BY Timestamp) AS Close,
            SUM(Volume) AS Volume
        FROM STOCK_PRICES
        GROUP BY Ticker, DATE(Timestamp);
    """)
    conn.commit()
    print(" Daily stock prices stored in Snowflake.")

if __name__ == "__main__":
    store_daily_stock_prices()
