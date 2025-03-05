import yfinance as yf
import pandas as pd
import snowflake.connector

# Snowflake Connection
conn = snowflake.connector.connect(
    user="nidhipatel066",
    password="J8xru@gndJ8xru@gnd",
    account="YYAZVZV-DOB64621",
    warehouse="COMPUTE_WH",
    database="STOCKS",
    schema="MARKET_DATA"
)

def fetch_stock_prices():
    """Fetch latest stock prices for all tickers in Snowflake."""
    cursor = conn.cursor()
    
    # Fetch tickers from Snowflake
    cursor.execute("SELECT Ticker FROM SP500_TICKERS")
    tickers = [row[0] for row in cursor.fetchall()]

    stock_data = []

    for ticker in tickers :  # Limiting to 10 tickers for testing
        try:
            stock = yf.Ticker(ticker)
            data = stock.history(period="1d", interval="1h")  # 1-day data, hourly intervals
            
            for index, row in data.iterrows():
                stock_data.append((ticker, index, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"]))
        
        except Exception as e:
            print(f"⚠️ Error fetching data for {ticker}: {e}")

    df = pd.DataFrame(stock_data, columns=["Ticker", "Timestamp", "Open", "High", "Low", "Close", "Volume"])
    return df

# Example Run
if __name__ == "__main__":
    stock_df = fetch_stock_prices()
    print(stock_df.head())
