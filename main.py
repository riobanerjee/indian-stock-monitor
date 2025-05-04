import os
import requests
import datetime
import pandas as pd
import numpy as np
from flask import Flask, request, jsonify
from google.cloud import bigquery

app = Flask(__name__)

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

def calculate_percent_change(current_close, symbol):
    """Calculate percentage change from previous day"""
    query = f"""
    SELECT close
    FROM `{project_id}.indian_stock_pipeline.stocks`
    WHERE symbol = '{symbol}'
    ORDER BY date DESC
    LIMIT 1
    """
    
    result = client.query(query).result()
    rows = list(result)
    
    if rows:
        prev_close = rows[0].close
        return ((current_close - prev_close) / prev_close) * 100
    
    return 0.0

def detect_anomaly(percent_change):
    """Simple anomaly detection - flagging > 3% changes"""
    return abs(percent_change) > 3.0

def fetch_and_process_stocks():
    """Fetches stock data and stores in BigQuery"""
    api_key = os.environ.get('API_KEY')
    symbols = get_tracked_symbols()
    
    # Today's date
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    
    rows = []
    processed_symbols = []
    errors = []
    
    for symbol in symbols:
        # Fetch data from API
        url = f"https://indianapi.in/indian-stock-market/stock"
        headers = {"X-API-KEY": api_key}
        params = {"name": symbol}
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Extract current price and volume
            close = float(data['close'])
            volume = int(data['volume'])
            
            # Calculate 7-day moving average
            hist_data = get_historical_data(symbol)
            ma_7day = close if hist_data.empty else np.mean(hist_data['close'].tolist() + [close])
            
            # Calculate percent change
            percent_change = calculate_percent_change(close, symbol)
            
            # Check for anomaly
            is_anomaly = detect_anomaly(percent_change)
            
            # Create row
            row = {
                'symbol': symbol,
                'date': today,
                'close': close,
                'volume': volume,
                'ma_7day': ma_7day,
                'percent_change': percent_change,
                'is_anomaly': is_anomaly,
                'timestamp': datetime.datetime.now().isoformat()
            }
            
            rows.append(row)
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
            errors.extend(insert_errors)
    
    return {
        "processed_symbols": processed_symbols,
        "errors": errors,
        "total_processed": len(processed_symbols)
    }

@app.route('/', methods=['GET'])
def home():
    """Simple home endpoint"""
    return jsonify({
        "service": "Indian Stock Market Pipeline",
        "status": "running",
        "endpoints": {
            "/fetch": "Trigger stock data fetching (POST)",
            "/health": "Health check endpoint (GET)"
        }
    })

@app.route('/fetch', methods=['POST'])
def fetch_stocks():
    """Endpoint to trigger stock data fetching"""
    result = fetch_and_process_stocks()
    return jsonify(result)

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy"})

if __name__ == "__main__":
    # This is used when running locally
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))