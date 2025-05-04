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

def fetch_and_process_stocks(request):
    """Cloud Function entry point - fetches stock data and stores in BigQuery"""
    api_key = os.environ.get('API_KEY')
    symbols = get_tracked_symbols()
    
    # Today's date
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    
    rows = []
    
    for symbol in symbols:
        # Fetch data from API
        url = f"https://indianapi.in/indian-stock-market/stock"
        headers = {"X-Api-Key": api_key}
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
            
            # Send alert if anomaly detected
            # if is_anomaly:
            #     send_alert(symbol, close, percent_change)
            
        except Exception as e:
            print(f"Error processing {symbol}: {str(e)}")
    
    # Insert data into BigQuery
    if rows:
        table_id = f"{project_id}.indian_stock_pipeline.stocks"
        errors = client.insert_rows_json(table_id, rows)
        
        if errors:
            print(f"Errors inserting rows: {errors}")
            return (f"Error inserting rows: {errors}", 500)
        
        return f"Successfully processed {len(rows)} stocks", 200
    
    return "No data to process", 200

# def send_alert(symbol, close, percent_change):
#     """Simple function to send email alerts"""
#     from sendgrid import SendGridAPIClient
#     from sendgrid.helpers.mail import Mail
    
#     sendgrid_api_key = os.environ.get('SENDGRID_API_KEY')
#     from_email = os.environ.get('FROM_EMAIL')
#     to_email = os.environ.get('TO_EMAIL')
    
#     direction = "up" if percent_change > 0 else "down"
    
#     message = Mail(
#         from_email=from_email,
#         to_emails=to_email,
#         subject=f"Stock Alert: {symbol} moved {direction} by {abs(percent_change):.2f}%",
#         html_content=f"""
#         <h3>Stock Movement Alert</h3>
#         <p>Symbol: <strong>{symbol}</strong></p>
#         <p>Current Price: <strong>₹{close:.2f}</strong></p>
#         <p>Change: <strong>{percent_change:.2f}%</strong></p>
#         <p>Date: {datetime.datetime.now().strftime('%Y-%m-%d')}</p>
#         """
#     )
    
#     try:
#         sg = SendGridAPIClient(sendgrid_api_key)
#         response = sg.send(message)
#         print(f"Alert sent for {symbol}, status code: {response.status_code}")
#         return True
#     except Exception as e:
#         print(f"Error sending alert for {symbol}: {str(e)}")
#         return False

# HTTP Trigger
@functions_framework.http
def fetch_stocks(request):
    return fetch_and_process_stocks(request)