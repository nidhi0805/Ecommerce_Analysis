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
)

def store_daily_stock_prices():
    """Aggregate intraday stock prices into daily values for AI."""
    cursor = conn.cursor()
    cursor.execute("USE DATABASE STOCKS;")
    cursor.execute("USE SCHEMA STOCK_PRICES;")

    cursor.execute("""
        INSERT INTO STOCK_PRICES_DAILY (Ticker, Date, Open, High, Low, Close, Volume)
        SELECT 
            Ticker,
            DATE(Timestamp) AS Date,
            Open,  
            MAX(High) AS High,
            MIN(Low) AS Low,
            Close,
            SUM(Volume) AS Volume
            FROM (
                SELECT 
                Ticker,
                Timestamp,
                DATE(Timestamp) AS Date,
                Open,
                High,
                Low,
                Close,
                Volume,
                ROW_NUMBER() OVER (PARTITION BY Ticker, DATE(Timestamp) ORDER BY Timestamp) AS rn_open,
                ROW_NUMBER() OVER (PARTITION BY Ticker, DATE(Timestamp) ORDER BY Timestamp DESC) AS rn_close
            FROM STOCK_PRICES
            ) 
            WHERE rn_open = 1 OR rn_close = 1
            GROUP BY Ticker, Date, Open, Close,Date(Timestamp), Volume;
        """)    
    conn.commit()
    print(" Daily stock prices stored in Snowflake.")

if __name__ == "__main__":
    store_daily_stock_prices()
