from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import yfinance as yf
import pandas as pd
import logging
from datetime import datetime


@dag(
    start_date=datetime(2024, 3, 4),
    schedule="0 0 * * *",  # Runs every day at midnight UTC
    catchup=False,
    default_args={"owner": "Nidhi", "retries": 3},
    tags=["finance", "stock_fundamentals"],
)
def stock_fundamentals_pipeline():

    @task()
    def fetch_tickers_from_snowflake():
        """Retrieve stock tickers from Snowflake."""
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("USE DATABASE STOCKS;")
        cursor.execute("USE SCHEMA MARKET_DATA;")
        cursor.execute("SELECT Ticker FROM SP500_TICKERS")

        tickers = [row[0] for row in cursor.fetchall()]  # Extract tickers
        cursor.close()
        conn.close()
        return tickers  # Return list of tickers for the next task

    @task()
    def fetch_stock_fundamentals(tickers):
        """Fetch stock fundamentals from Yahoo Finance."""
        data = []

        for ticker in tickers:
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                data.append({
                    "symbol": ticker,
                    "date": datetime.today().strftime("%Y-%m-%d"),
                    "revenue": info.get("totalRevenue", None),
                    "eps": info.get("trailingEps", None),
                    "pe_ratio": info.get("trailingPE", None),
                    "market_cap": info.get("marketCap", None),
                    "debt_to_equity": info.get("debtToEquity", None),
                })
            except Exception as e:
                logging.error(f"Error fetching data for {ticker}: {e}")

        df = pd.DataFrame(data)
        print(df.to_string(index=False))  # Display in logs
        return df.to_dict(orient="records")  # Return as list of dicts for next task

    @task()
    def store_stock_fundamentals(stock_data):
        """Store stock fundamentals in Snowflake."""
        if not stock_data:
            logging.warning("No stock fundamentals data to store.")
            return

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("USE DATABASE STOCKS;")
        cursor.execute("USE SCHEMA MARKET_DATA;")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS STOCK_FUNDAMENTALS (
                symbol STRING,
                date DATE,
                revenue FLOAT,
                eps FLOAT,
                pe_ratio FLOAT,
                market_cap FLOAT,
                debt_to_equity FLOAT
            );
        """)

        insert_query = """
            INSERT INTO STOCK_FUNDAMENTALS (symbol, date, revenue, eps, pe_ratio, market_cap, debt_to_equity)
            VALUES (%(symbol)s, %(date)s, %(revenue)s, %(eps)s, %(pe_ratio)s, %(market_cap)s, %(debt_to_equity)s)
        """
        cursor.executemany(insert_query, stock_data)

        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Stock fundamentals stored in Snowflake!")

    # **Assign task outputs to variables**
    tickers = fetch_tickers_from_snowflake()  # Get tickers from Snowflake
    stock_data = fetch_stock_fundamentals(tickers)  # Fetch stock fundamentals
    store_stock_fundamentals(stock_data)  # Store results in Snowflake


# Instantiate DAG
stock_fundamentals_pipeline()