import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("FMP_API_KEY")
BASE_URL = 'https://financialmodelingprep.com/api/v3'

def fetch_and_save(symbol: str, endpoint: str, folder: str):
    url = f"{BASE_URL}/{endpoint}/{symbol}?period=annual&apikey={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, f"{endpoint}.json")
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Saved {endpoint} data for {symbol} at {filepath}")

def main(symbol: str):
    raw_folder = os.path.join("data", "raw", symbol)
    fetch_and_save(symbol, "income-statement", raw_folder)
    fetch_and_save(symbol, "balance-sheet-statement", raw_folder)
    fetch_and_save(symbol, "cash-flow-statement", raw_folder)

if __name__ == "__main__":
    import sys
    sym = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    main(sym)