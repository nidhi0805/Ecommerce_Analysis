from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
import snowflake.connector
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Allows requests from React frontend
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)


def get_snowflake_connection():
    return snowflake.connector.connect(
    user="nidhipatel066",
    password="J8xru@gndJ8xru@gnd",
    account="YYAZVZV-DOB64621",
    warehouse="COMPUTE_WH",
    database="STOCKS",
    schema="market_data"
    )

@app.get("/stock/{symbol}")
def get_stock_info(symbol: str):
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    query = f"SELECT * FROM STOCK_FUNDAMENTALS WHERE symbol = '{symbol}' ORDER BY date DESC LIMIT 1"
    cursor.execute(query)
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if row:
        return {
            "symbol": row[0],
            "date": row[1],
            "revenue": row[2],
            "eps": row[3],
            "pe_ratio": row[4],
            "market_cap": row[5],
            "debt_to_equity": row[6]
        }
    return {"error": "Stock not found"}

    
