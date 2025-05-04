CREATE TABLE indian_stock_pipeline.stocks (
  symbol STRING NOT NULL,
  date DATE NOT NULL,
  price FLOAT64 NOT NULL,
  year_high FLOAT64,
  year_low FLOAT64,
  risk STRING,
  ma_7day FLOAT64,
  percent_change FLOAT64,
  is_anomaly BOOL,
  timestamp TIMESTAMP NOT NULL,
) PARTITION BY date;

-- stocks to track
CREATE TABLE indian_stock_pipeline.tracked_stocks (
  symbol STRING NOT NULL
);

INSERT INTO indian_stock_pipeline.tracked_stocks (symbol)
VALUES ('RELIANCE'), ('TCS'), ('HDFCBANK'), ('INFY'), ('ICICIBANK');