from get_stock_prices import fetch_stock_prices
import pandas as pd
import snowflake.connector

# Snowflake Connection
conn = snowflake.connector.connect(
    user="nidhipatel066",
    password="J8xru@gndJ8xru@gnd",
    account="YYAZVZV-DOB64621",
    warehouse="COMPUTE_WH",
    database="STOCKS",
    schema="STOCK_PRICES"
)

def store_stock_prices_snowflake():
    """Fetch and store stock prices in Snowflake."""
    stock_df = fetch_stock_prices()

    if stock_df.empty:
        print("⚠️ No stock prices found.")
        return

    cursor = conn.cursor()

    for _, row in stock_df.iterrows():
        cursor.execute("""
            INSERT INTO STOCK_PRICES (Ticker, Timestamp, Open, High, Low, Close, Volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            row["Ticker"], 
            row["Timestamp"].isoformat(),  # Convert timestamp to string
            row["Open"], 
            row["High"], 
            row["Low"], 
            row["Close"], 
            int(row["Volume"]) if not pd.isna(row["Volume"]) else None  # Handle NaN in Volume
        ))

    conn.commit()
    print(f" {len(stock_df)} stock prices stored in Snowflake!")

def delete_old_stock_prices():
    """Delete stock prices older than 6 months to free up space."""
    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM STOCK_PRICES 
        WHERE Timestamp < DATEADD(MONTH, -6, CURRENT_TIMESTAMP);
    """)
    conn.commit()
    print("✅ Deleted stock prices older than 6 months.")

delete_old_data_task = PythonOperator(
    task_id="delete_old_stock_prices",
    python_callable=delete_old_stock_prices,
    dag=dag,
)


# Run function
if __name__ == "__main__":
    store_stock_prices_snowflake()
