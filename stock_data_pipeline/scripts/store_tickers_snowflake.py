import snowflake.connector
from get_sp500_symbols import get_sp500_tickers  

# Snowflake Connection
conn = snowflake.connector.connect(
    user="nidhipatel066",
    password="J8xru@gndJ8xru@gnd",
    account="YYAZVZV-DOB64621",
    warehouse="COMPUTE_WH",
    database="STOCKS",
    schema="MARKET_DATA"
)

def clean_value(value):
    """Convert empty strings to NULL for Snowflake compatibility."""
    return f"NULL" if value.strip() == "" else f"'{value.replace("'", "''")}'"

def store_sp500_tickers():
    """Fetch and store S&P 500 tickers in Snowflake."""
    tickers_df = get_sp500_tickers()

    if tickers_df.empty:
        print("⚠️ No tickers found.")
        return

    cursor = conn.cursor()

    for _, row in tickers_df.iterrows():
        # Convert empty strings to NULL
        query = f"""
            MERGE INTO SP500_TICKERS AS target
            USING (SELECT {clean_value(row['Ticker'])} AS Ticker, 
                          {clean_value(row['Company'])} AS Company, 
                          {clean_value(row['Sector'])} AS Sector, 
                          {clean_value(row['Sub-Industry'])} AS Sub_Industry, 
                          {clean_value(row['Location'])} AS Location, 
                          {clean_value(row['Date Added'])} AS Date_Added, 
                          {clean_value(row['CIK'])} AS CIK, 
                          {clean_value(row['Founded'])} AS Founded) AS source
            ON target.Ticker = source.Ticker
            WHEN NOT MATCHED THEN
                INSERT (Ticker, Company, Sector, Sub_Industry, Location, Date_Added, CIK, Founded)
                VALUES (source.Ticker, source.Company, source.Sector, source.Sub_Industry, 
                        source.Location, source.Date_Added, source.CIK, source.Founded);
        """
        cursor.execute(query)
    conn.commit()
    print(f"✅ {len(tickers_df)} S&P 500 tickers stored in Snowflake!")

# Run function
if __name__ == "__main__":
    store_sp500_tickers()
