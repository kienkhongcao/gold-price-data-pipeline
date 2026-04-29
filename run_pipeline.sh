#!/bin/bash
set -e

cd /app/source-code

echo "Bắt đầu pipeline"

python3 ingestion/gold_api_ingest.py
python3 ingestion/dxy_index_ingest.py
python3 ingestion/fedfunds_ingest.py
python3 ingestion/fred_10y_ingest.py
python3 ingestion/fred_cpi_ingest.py

python3 features/silver_features.py
python3 features/gold_features.py

python3 ml/feature_engineering.py
python3 ml/train_model.py

echo "Hoàn tất pipeline"