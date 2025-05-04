indian-stock-monitor % gcloud functions deploy fetch-stocks \                 
  --runtime=python311 \
  --trigger-http \
  --entry-point=fetch_stocks \
  --allow-unauthenticated \
  --set-env-vars PROJECT_ID=project-id,API_KEY=api-key

  ----
  gcloud scheduler jobs create http daily-stock-fetcher \
  --schedule="0 11 * * 1-5" \
  --uri=https://region-project-id.cloudfunctions.net/fetch-stocks \
  --http-method=POST \
  --location=region