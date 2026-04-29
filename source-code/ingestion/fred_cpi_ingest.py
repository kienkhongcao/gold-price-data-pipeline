import os
import json
from uuid import uuid4
from pathlib import Path
from dotenv import load_dotenv
import logging
from minio import Minio
import time
from datetime import datetime, timezone
import requests
import pandas as pd 
import uuid
from io import BytesIO
from io import StringIO

#Cấu hình
PIPLINE_VERSION = "1.0"
ENVIRONMENT = "dev"

#biến mt

env_path = Path(__file__).resolve().parents[2] / "configs" / ".env"
load_dotenv(env_path)

FRED_BASE_URL = os.getenv("FRED_BASE_URL")
FRED_API_KEY = os.getenv("FRED_API_KEY")
FRED_CPI_SERIES = os.getenv("FRED_CPI_SERIES")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "datalake")

#cấu hình log

logging.basicConfig(
	level =  logging.INFO,
	format = "%(asctime)s - %(levelname)s - %(message)s"
	)
logger = logging.getLogger(__name__)

#hàm dựng cấu hình minio
def get_minio_client() -> Minio:
	return Minio(
		MINIO_ENDPOINT,
		access_key = MINIO_ACCESS_KEY,
		secret_key =  MINIO_SECRET_KEY,
		secure =  False
	)

#hàm kéo dữ liệu
def keo_fred_cpi(retries=3):
	for attempt in range(retries):
		try:			
			start_time = time.time()

			params = {
				"series_id": FRED_CPI_SERIES,
				"api_key": FRED_API_KEY,
				"file_type": "json"
			}

			response = requests.get(FRED_BASE_URL, params=params, timeout=30)
			response.raise_for_status()
			
			data = response.json()["observations"]
			latency = int((time.time() - start_time) * 1000)

			df = pd.DataFrame(data)[["date", "value"]]
			df.columns = ["Date", "CPI"]
			df["Date"] = pd.to_datetime(df["Date"])
			df["CPI"] = pd.to_numeric(df["CPI"], errors="coerce")
			df = df.dropna().sort_values("Date")

			return df, response, latency 
		except Excpetion as e:
			logger.warning(f"Attempt to try again {attempt+1} - error: {e}")

	raise Exception(f"Thử lại thất bại")

def build_metadata_source(response, latency):
	return {
		"source_name": "fred_cpi",
		"endpoint": response.url.replace(FRED_BASE_URL, ""),
		"http_status": response.status_code,
		"request_time": latency,
		"response_size_bytes": len(response.content)
	}

def build_metadata_ingestion(retry_count: int= 0):
	return {
		"ingest_id": str(uuid.uuid4()),
		"ingestion_time": datetime.now(timezone.utc).isoformat(),
		"ingestion_type": "url pull",
		"pipeline_version": PIPLINE_VERSION,
		"environment": ENVIRONMENT,
		"retry_count": retry_count,
		"ingested_by": "fred_cpy_ingest.py"
	}

def build_file_name(ingestion_time):
	timestamp = ingestion_time.strftime('%Y%m%dT%H%M%S')
	uid = uuid4().hex[:6]

	return f"fred_cpi_{timestamp}_{uid}"

def build_object_path(ingestion_time):
	file_path = (
		f"raw/fred_cpi/"
		f"source=fred_cpi/"
		f"year={ingestion_time.year}/"
		f"month={ingestion_time.month:02d}/"
		f"day={ingestion_time.day:02d}/"
	)
	base_name = build_file_name(ingestion_time)

	data_path = file_path + base_name + ".csv"
	metadata_path = file_path + base_name + "_metadata.json"

	return data_path, metadata_path

#hàm đẩy dl lên minio
def push_csv_to_minio(client: Minio, object_path:str, df: pd.DataFrame):
	
	data = df.to_csv(index=False).encode("utf-8")

	client.put_object(
		bucket_name= MINIO_BUCKET,
		object_name = object_path,
		data = BytesIO(data),
		length = len(data),
		content_type = "text/csv"
	)

def push_metadata_to_minio(client: Minio, object_path:str, metadata: dict):
	
	data = json.dumps(metadata, indent=2).encode("utf-8")

	client.put_object(
		bucket_name= MINIO_BUCKET,
		object_name = object_path,
		data = BytesIO(data),
		length = len(data),
		content_type = "application/json"
	)

#main
def main():
	logger.info("bắt đầu CPI ingest")

	client = get_minio_client()

	if not client.bucket_exists(MINIO_BUCKET):
		client.make_bucket(MINIO_BUCKET)
		print(f"đã tạo bucket: {MINIO_BUCKET}")

	payload, response, latency = keo_fred_cpi()

	
	metadata_source = build_metadata_source(response, latency)
	metadata_ingestion = build_metadata_ingestion()

	ingestion_time = datetime.fromisoformat(metadata_ingestion["ingestion_time"])
	
	data_path, metadata_path = build_object_path(ingestion_time)

	metadata = {
		"metadata_source":metadata_source,
		"metadata_ingestion": metadata_ingestion,
		"data_file": data_path 
	}

	push_csv_to_minio(client, data_path, payload)
	push_metadata_to_minio(client,metadata_path, metadata)

	logging.info(f"Đã ingest fred CPI tại {data_path}")

if __name__ == "__main__":
	try:
		logger.info("[START] fred cpi ingest")
		main()
		logger.info("[SUCCESS] fred cpi ingest")
	except Exception as e:
		logger.exception(f"Ingest CPI thất bại do: {e}")
		exit(1)





