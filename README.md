# 📈 Indian Stock Market Monitoring Pipeline

A lightweight, serverless GCP pipeline that fetches, analyzes, and visualizes Indian stock market data.

## Features

- Daily data ingestion from Indian Stock Exchange API
- Technical indicators (7-day moving average)
- Anomaly detection (>3% price movements)
- Risk categorization
- Interactive Looker Studio dashboard

## Tech Stack

- **Cloud Functions**: Data ingestion
- **BigQuery**: Storage and analysis
- **Cloud Scheduler**: Automated daily triggers
- **Looker Studio**: Visualization

## Quick Setup

1. Create BigQuery tables:
   ```bash
   bq mk --dataset indian_stock_pipeline
   bq query --use_legacy_sql=false < sql/create_tables.sql
   ```

2. Deploy Cloud Function:
   ```bash
   gcloud functions deploy fetch-stocks \
     --runtime=python311 \
     --trigger-http \
     --entry-point=fetch_stocks \
     --allow-unauthenticated \
     --set-env-vars PROJECT_ID=your-project-id,API_KEY=your-api-key
   ```

3. Set up Cloud Scheduler:
   ```bash
   gcloud scheduler jobs create http daily-stock-fetcher \
     --schedule="0 11 * * 1-5" \
     --uri=https://REGION-PROJECT_ID.cloudfunctions.net/fetch-stocks \
     --http-method=POST
   ```

4. Create Looker Studio dashboard using the BigQuery data source

## Future Features

- Email/Slack alerts for anomalous price movements
- Machine learning-based price prediction
- Portfolio performance tracking

## License

MIT