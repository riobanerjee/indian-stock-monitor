-- Find all anomalies in the last 30 days
SELECT date, symbol, close, percent_change
FROM `indian_stock_pipeline.stocks`
WHERE is_anomaly = TRUE
AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY date DESC;

-- Calculate average daily volume by stock
SELECT symbol, AVG(volume) as avg_volume
FROM `indian_stock_pipeline.stocks`
GROUP BY symbol
ORDER BY avg_volume DESC;

-- Find days with highest percentage gains
SELECT date, symbol, percent_change
FROM `indian_stock_pipeline.stocks`
WHERE percent_change > 0
ORDER BY percent_change DESC
LIMIT 10;

-- Find potential buy signals (price crosses above MA)
SELECT 
  current.date,
  current.symbol,
  current.close,
  current.ma_7day
FROM `indian_stock_pipeline.stocks` AS current
JOIN `indian_stock_pipeline.stocks` AS previous
  ON current.symbol = previous.symbol
  AND previous.date = DATE_SUB(current.date, INTERVAL 1 DAY)
WHERE current.close > current.ma_7day
  AND previous.close < previous.ma_7day
ORDER BY current.date DESC;