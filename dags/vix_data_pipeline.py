from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import yfinance as yf
import pandas as pd
import logging
from datetime import datetime


@dag(
    start_date=datetime(2024, 3, 4),
    schedule="0 0 * * *",  # Runs daily at midnight UTC
    catchup=False,
    default_args={"owner": "Nidhi", "retries": 3},
    tags=["finance", "vix_index"],
)
def vix_data_pipeline():

    @task()
    def fetch_vix_data():
        """Fetch the VIX index data from Yahoo Finance."""
        try:
            vix = yf.Ticker("^VIX")
            vix_history = vix.history(period="1d")  # Get latest day's data

            if vix_history.empty:
                logging.warning("No VIX data available for today.")
                return None

            latest_vix = vix_history.iloc[-1]  # Get the latest row

            data = {
                "date": datetime.today().strftime("%Y-%m-%d"),
                "vix_open": latest_vix["Open"],
                "vix_high": latest_vix["High"],
                "vix_low": latest_vix["Low"],
                "vix_close": latest_vix["Close"],
                "vix_volume": latest_vix["Volume"],
            }

            return [data]  # Returning list of dicts for Snowflake insertion

        except Exception as e:
            logging.error(f"Error fetching VIX data: {e}")
            return None

    @task()
    def store_vix_data(vix_data):
        """Store the VIX data in Snowflake."""
        if not vix_data:
            logging.warning("No VIX data to store.")
            return

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("USE DATABASE STOCKS;")
        cursor.execute("USE SCHEMA MARKET_DATA;")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS VIX_DATA (
                date DATE PRIMARY KEY,
                vix_open FLOAT,
                vix_high FLOAT,
                vix_low FLOAT,
                vix_close FLOAT,
                vix_volume FLOAT
            );
        """)

        merge_query = """
            MERGE INTO VIX_DATA AS target
            USING (SELECT %(date)s AS date, %(vix_open)s AS vix_open, %(vix_high)s AS vix_high, 
                          %(vix_low)s AS vix_low, %(vix_close)s AS vix_close, %(vix_volume)s AS vix_volume) AS source
            ON target.date = source.date
            WHEN MATCHED THEN UPDATE SET 
                target.vix_open = source.vix_open,
                target.vix_high = source.vix_high,
                target.vix_low = source.vix_low,
                target.vix_close = source.vix_close,
                target.vix_volume = source.vix_volume
            WHEN NOT MATCHED THEN 
                INSERT (date, vix_open, vix_high, vix_low, vix_close, vix_volume)
                VALUES (source.date, source.vix_open, source.vix_high, source.vix_low, source.vix_close, source.vix_volume);
        """

        cursor.executemany(merge_query, vix_data)

        conn.commit()
        cursor.close()
        conn.close()
        logging.info("VIX data successfully stored in Snowflake!")

    # DAG Execution Flow
    vix_data = fetch_vix_data()
    store_vix_data(vix_data)


# Instantiate DAG
vix_data_pipeline()
