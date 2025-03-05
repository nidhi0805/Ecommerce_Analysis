from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd
import snowflake.connector
import yfinance as yf

# Define the DAG
@dag(
    start_date=datetime(2024, 3, 4),
    schedule="*/15 * * * *",  # Runs every 15 minutes
    catchup=False,
    default_args={"owner": "Nidhi", "retries": 3},
    tags=["finance", "stock_data"],
)
def stock_data_pipeline():
    
    @task
    def fetch_tickers_from_snowflake() -> list:
        """Retrieves the list of S&P 500 tickers from Snowflake."""
        conn = snowflake.connector.connect(
            user="nidhipatel066",
            password="J8xru@gndJ8xru@gnd",
            account="YYAZVZV-DOB64621",
            database="STOCKS",
            schema="MARKET_DATA"
        )

        cursor = conn.cursor()
        cursor.execute("SELECT Ticker FROM SP500_TICKERS")
        tickers = [row[0] for row in cursor.fetchall()]  # Extract tickers as a list

        cursor.close()
        conn.close()

        return tickers

    @task
    def fetch_stock_prices(tickers: list) -> pd.DataFrame:
        """Fetches stock prices from Yahoo Finance for given tickers."""
        stock_data = []
        for ticker in tickers:
            data = yf.download(ticker, period="1d", interval="15m")
            if not data.empty:
                data.reset_index(inplace=True)
                data["Ticker"] = ticker
                stock_data.append(data)

        return pd.concat(stock_data) if stock_data else pd.DataFrame()

    @task
    def store_to_snowflake(stock_df: pd.DataFrame):
        """Stores stock price data into Snowflake."""
        if stock_df.empty:
            print("⚠️ No stock data to store.")
            return

        print("📝 First row sample:", stock_df.iloc[0].to_dict())
        conn = snowflake.connector.connect(
            user="nidhipatel066",
            password="J8xru@gndJ8xru@gnd",
            account="YYAZVZV-DOB64621",
            database="STOCKS",
            schema="STOCK_PRICES"
        )

        cursor = conn.cursor()

        for _, row in stock_df.iterrows():
            cursor.execute("""
                INSERT INTO STOCK_PRICES (Ticker, Timestamp, Open, High, Low, Close, Volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                row["Ticker"], 
                row["Datetime"].isoformat(),
                row["Open"], 
                row["High"], 
                row["Low"], 
                row["Close"], 
                int(row["Volume"]) if not pd.isna(row["Volume"]) else None
            ))

        conn.commit()
        print("✅ Stock data successfully stored in Snowflake!")
        cursor.close()
        conn.close()

    # Task Dependencies
    tickers = fetch_tickers_from_snowflake()
    stock_prices = fetch_stock_prices(tickers)
    store_to_snowflake(stock_prices)

# Instantiate the DAG
stock_data_pipeline()
