import functions_framework
from google.cloud import bigquery
import requests
import datetime
import pandas as pd
import numpy as np
import os

# Initialize BigQuery client
project_id = os.environ.get('PROJECT_ID')
client = bigquery.Client(project=project_id)

def get_tracked_symbols():
    """Get list of stocks to track"""
    query = f"""
    SELECT symbol FROM `{project_id}.indian_stock_pipeline.tracked_stocks`
    """
    return [row.symbol for row in client.query(query).result()]

def get_historical_data(symbol, days=7):
    """Get historical data for calculating moving average"""
    query = f"""
    SELECT date, close 
    FROM `{project_id}.indian_stock_pipeline.stocks`
    WHERE symbol = '{symbol}'
    ORDER BY date DESC
    LIMIT {days}
    """
    
    df = client.query(query).to_dataframe()
    return df

def detect_anomaly(percent_change):
    """Simple anomaly detection - flagging > 3% changes"""
    return abs(percent_change) > 3.0

@functions_framework.http
def fetch_stocks(request):
    """Cloud Function entry point - fetches stock data and stores in BigQuery"""
    api_key = os.environ.get('API_KEY')
    symbols = get_tracked_symbols()
    
    # Today's date
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    
    rows = []
    processed_symbols = []
    errors = []
    
    for symbol in symbols:
        # Fetch data from API
        url = f"https://stock.indianapi.in/stock"
        headers = {"X-Api-Key": api_key}
        params = {"name": symbol}
        
        try:
            print(f"Fetching data for {symbol} from {url}")
            response = requests.get(url, headers=headers, params=params)
            print(f"Response status: {response.status_code}")
            
            response.raise_for_status()
            data = response.json()
            
            # Extract only essential metrics
            extracted_data = {
                'symbol': symbol,
                'date': today,
                'close': float(data.get('currentPrice', {}).get('BSE', 0)),
                'volume': int(data.get('stockTechnicalData', {}).get('volume', 0)),
                'percent_change': float(data.get('percentChange', 0)),
                'timestamp': datetime.datetime.now().isoformat()
            }
            
            # Calculate 7-day moving average
            hist_data = get_historical_data(symbol)
            ma_7day = extracted_data['close'] if hist_data.empty else np.mean(hist_data['close'].tolist() + [extracted_data['close']])
            extracted_data['ma_7day'] = ma_7day
            
            # Detect anomaly
            extracted_data['is_anomaly'] = detect_anomaly(extracted_data['percent_change'])
            
            rows.append(extracted_data)
            processed_symbols.append(symbol)
            
        except Exception as e:
            error_msg = f"Error processing {symbol}: {str(e)}"
            print(error_msg)
            errors.append(error_msg)
    
    # Insert data into BigQuery
    if rows:
        table_id = f"{project_id}.indian_stock_pipeline.stocks"
        insert_errors = client.insert_rows_json(table_id, rows)
        
        if insert_errors:
            print(f"Errors inserting rows: {insert_errors}")
            return (f"Error inserting rows: {insert_errors}", 500)
    
    result = {
        "processed_symbols": processed_symbols,
        "errors": errors,
        "total_processed": len(processed_symbols)
    }
    
    return (result, 200)