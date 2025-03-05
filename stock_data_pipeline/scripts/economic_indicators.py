from fredapi import Fred

FRED_API_KEY = "d2f982b7dcf2570dc681cce3c648384c"
fred = Fred(api_key=FRED_API_KEY)

gdp = fred.get_series('GDP')  # US GDP data
inflation = fred.get_series('CPIAUCSL')  # US Inflation (CPI)
interest_rates = fred.get_series('DFF')  # Fed Funds Rate
