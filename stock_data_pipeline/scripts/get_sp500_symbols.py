import requests
import pandas as pd
from bs4 import BeautifulSoup

def get_sp500_tickers():
    """Fetch the latest S&P 500 stock tickers from Wikipedia, handling <a> tags correctly."""
    
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, headers=headers, timeout=10)  # Add timeout
    response.raise_for_status()  # Raise error if request fails

    # Parse HTML
    soup = BeautifulSoup(response.text, "html.parser")

    # Find table with id="constituents"
    table = soup.find("table", {"id": "constituents"})

    # Extract rows manually
    tickers = []
    rows = table.find_all("tr")[1:]  # Skip header row

    for row in rows:
        columns = row.find_all("td")
        
        if len(columns) < 8:  # Ensure row has enough columns
            continue

        # Extract ticker from the <a> tag inside <td>
        ticker_tag = columns[0].find("a")
        ticker = ticker_tag.text.strip() if ticker_tag else columns[0].text.strip()

        company = columns[1].text.strip()  # Extract company name
        sector = columns[2].text.strip()  # Extract sector
        sub_industry = columns[3].text.strip()  # Extract sub-industry
        location = columns[4].text.strip()  # Extract HQ location
        date_added = columns[5].text.strip()  # Extract date added
        cik = columns[6].text.strip()  # Extract CIK
        founded = columns[7].text.strip()  # Extract founded year

        # Clean ticker symbols for Yahoo Finance format
        ticker = ticker.replace(".", "-")  # Yahoo Finance uses "-" instead of "."

        tickers.append({
            "Ticker": ticker,
            "Company": company,
            "Sector": sector,
            "Sub-Industry": sub_industry,
            "Location": location,
            "Date Added": date_added,
            "CIK": cik,
            "Founded": founded
        })

    # Convert to Pandas DataFrame
    df = pd.DataFrame(tickers)
    return df

# Example: Fetch and display the first 10 tickers
if __name__ == "__main__":
    sp500_df = get_sp500_tickers()
    if not sp500_df.empty:
        print(f"{len(sp500_df)} S&P 500 stocks found.")
        print(sp500_df.head(10))  # Show first 10 tickers
    else:
        print("⚠️ No data fetched.")
