import requests
import json
import os

def test_indian_stock_api():
    """Test if the Indian Stock API is working"""
    # Replace with your API key
    api_key = input("Enter your API key: ")
    
    # Sample stock symbols
    symbols = ['RELIANCE', 'TCS']
    
    results = {}
    
    for symbol in symbols:
        print(f"Testing API for {symbol}...")
        url = f"https://stock.indianapi.in/stock"
        headers = {"X-Api-Key": api_key}
        params = {"name": symbol}
        
        try:
            response = requests.get(url, headers=headers, params=params)
            status_code = response.status_code
            print(f"Response status: {status_code}")
            
            if status_code == 200:
                data = response.json()
                print(f"Successfully fetched data for {symbol}")
                print(f"Current price: {data['currentPrice']['NSE']}")
                print(f"percentChange: {data['percentChange']}")
                results[symbol] = "Success"
            else:
                print(f"Failed to fetch data. Status code: {status_code}")
                print(f"Response: {response.text}")
                results[symbol] = f"Failed - Status {status_code}"
                
        except Exception as e:
            print(f"Error processing {symbol}: {str(e)}")
            results[symbol] = f"Error - {str(e)}"
        
        print("-" * 50)
    
    print("\nSummary:")
    for symbol, result in results.items():
        print(f"{symbol}: {result}")

if __name__ == "__main__":
    test_indian_stock_api()