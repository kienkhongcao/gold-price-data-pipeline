from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
	"owner": "kien",
	"retries": 2,
}

with DAG(
	dag_id= "gold_price_pipeline",
	default_args=default_args,
	start_date= datetime(2024,1,1),
	schedule_interval="@daily",
	catchup=False,
) as dag:
	gold_ingest = BashOperator(
		task_id="gold_ingest",
		bash_command="python /app/source-code/ingestion/gold_api_ingest.py")

	dxy_ingest = BashOperator(
		task_id="dxy_ingest",
		bash_command="python /app/source-code/ingestion/dxy_index_ingest.py")

	fedfunds_ingest = BashOperator(
		task_id="fedfunds_ingest",
		bash_command="python /app/source-code/ingestion/fedfunds_ingest.py")

	fred_10y_ingest = BashOperator(
		task_id="fred_10y_ingest",
		bash_command="python /app/source-code/ingestion/fred_10y_ingest.py")

	fred_cpi_ingest = BashOperator(
		task_id="fred_cpi_ingest",
		bash_command="python /app/source-code/ingestion/fred_cpi_ingest.py")

	silver = BashOperator(
		task_id="silver_features",
		bash_command="python /app/source-code/features/silver_features.py")

	gold = BashOperator(
		task_id="gold_features",
		bash_command="python /app/source-code/features/gold_features.py")

	feature_engineering = BashOperator(
		task_id="feature_engineering",
		bash_command="python /app/source-code/ml/feature_engineering")

	train_ml = BashOperator(
		task_id="train_model",
		bash_command="python /app/source-code/ml/train_model")

	#dependency
	[gold_ingest, dxy_ingest, fedfunds_ingest, fred_10y_ingest, fred_cpi_ingest] >> silver >> gold >> feature_engineering >> train_ml