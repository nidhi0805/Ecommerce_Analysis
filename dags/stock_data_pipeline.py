from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import datetime
import pandas as pd
import snowflake.connector
import yfinance as yf
import os

def get_snowflake_connection():
    """Retrieve Snowflake connection from Airflow"""
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    return hook.get_conn()

# Define the intraday DAG (runs every 15 minutes)
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
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Explicitly set database and schema
        cursor.execute("USE DATABASE STOCKS;")
        cursor.execute("USE SCHEMA MARKET_DATA;")

        cursor.execute("SELECT Ticker FROM SP500_TICKERS")
        tickers = [row[0] for row in cursor.fetchall()]  # Extract tickers as a list

        cursor.close()
        conn.close()

        return tickers

    @task
    def fetch_stock_prices(tickers: list) -> str:
        """Fetch stock prices and store them in a persistent location."""
        stock_data = []
    
        for ticker in tickers[:10]:  # Fetch only first 10 tickers
            print(f"🔍 Fetching data for {ticker}...")
            data = yf.download(ticker, period="1d", interval="15m")

            if data is None or data.empty:
                print(f"⚠️ No valid data found for {ticker}. Skipping...")
                continue  

            print(f"Data fetched for {ticker} with shape {data.shape}")

            # Flatten column names if MultiIndex exists
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = ['_'.join(col).strip() for col in data.columns.values]

            data.reset_index(inplace=True)

            try:
                volume_col = next(col for col in data.columns if "Volume" in col)
                open_col = next(col for col in data.columns if "Open" in col)
                high_col = next(col for col in data.columns if "High" in col)
                low_col = next(col for col in data.columns if "Low" in col)
                close_col = next(col for col in data.columns if "Close" in col)
            except StopIteration:
                print(f" ERROR: Could not find required columns for {ticker}. Skipping...")
                continue

            data.rename(columns={
            volume_col: "Volume",
            open_col: "Open",
            high_col: "High",
            low_col: "Low",
            close_col: "Close"
            }, inplace=True)


            try:
                data["Ticker"] = ticker  # Add ticker column
            except Exception as e:
                print(f"ERROR: Could not assign 'Ticker' for {ticker}: {e}")
                continue

            stock_data.append(data)

        # Ensure at least one valid DataFrame exists before concatenating
        if not stock_data:
            print(" ERROR: No valid stock data retrieved. Cannot create DataFrame.")
            return None

        stock_df = pd.concat(stock_data, ignore_index=True)
        stock_df.rename(columns={"Datetime": "Timestamp"}, inplace=True)
        stock_df["Timestamp"] = pd.to_datetime(stock_df["Timestamp"]).dt.tz_localize(None)
        stock_df["Volume"] = stock_df["Volume"].fillna(0).astype(int)

        # Save the DataFrame to a CSV file
        file_path = "/home/astro/stock_data.csv"

        print(f"📁 Saving CSV to: {file_path}")
        try:
            stock_df.to_csv(file_path, index=False, header=True, mode='w')
            print(f"Stock data successfully saved at: {file_path}")
        except Exception as e:
            print(f"ERROR: Could not save file! {str(e)}")

        return file_path

    @task
    def store_to_snowflake(file_path: str):
        """Reads stock price data from CSV and stores it into Snowflake."""
    
        # Load the DataFrame from CSV
        try:
            stock_df = pd.read_csv(file_path)
            print(f"📊 Loaded CSV with {len(stock_df)} rows.")
        except Exception as e:
            print(f"ERROR: Could not read CSV file! {str(e)}")
            return

        if stock_df.empty:
            print(" No stock data to store. The DataFrame is empty.")
            return

        print(f"First row sample: {stock_df.iloc[0].to_dict()}")
        print(f"Total rows to insert: {len(stock_df)}")

        # Establish Snowflake Connection
        try:
            hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
            conn = hook.get_conn()
            cursor = conn.cursor()
            print("Successfully connected to Snowflake!")
            cursor.execute("USE DATABASE STOCKS;")
            cursor.execute("USE SCHEMA STOCK_PRICES;")
            cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();")
            print(f"📌 Active Snowflake DB & Schema: {cursor.fetchall()}")

        except Exception as e:
            print(f"ERROR: Unable to connect to Snowflake! {str(e)}")
            return
       
        # Insert Data into Snowflake Table
        inserted_rows = 0
        failed_rows = 0

        for index, row in stock_df.iterrows():
            print(f"Attempting to insert row {index+1}/{len(stock_df)}: {row.to_dict()}")

            try:
                cursor.execute("""
                    INSERT INTO STOCK_PRICES (Ticker, Timestamp, Open, High, Low, Close, Volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    row["Ticker"], 
                    row["Timestamp"],
                    row["Open"], 
                    row["High"], 
                    row["Low"], 
                    row["Close"], 
                    row["Volume"]
                ))
                inserted_rows += 1
            except Exception as e:
                failed_rows += 1
                print(f" ERROR: Failed to insert row {index+1}: {str(e)}")
                continue

        conn.commit()
        print(f" {inserted_rows} rows successfully stored in Snowflake!")

        cursor.close()
        conn.close()

    tickers = fetch_tickers_from_snowflake()
    stock_prices = fetch_stock_prices(tickers)
    store_to_snowflake(stock_prices)

stock_data_pipeline()

# Define the daily DAG (runs at midnight)
@dag(
    start_date=datetime(2024, 3, 4),
    schedule="0 0 * * *",  #  Runs at midnight
    catchup=False,
    default_args={"owner": "Nidhi", "retries": 3},
    tags=["finance", "daily_stock_data"],
)
def daily_stock_pipeline():
    @task
    def store_daily_stock_prices():
        """Aggregate intraday stock prices into daily values."""
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
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
        print("Daily stock prices stored in Snowflake!")

    store_daily_stock_prices()

daily_stock_pipeline()
