import pandas as pd
from datetime import datetime
from io import StringIO
from io import BytesIO
from uuid import uuid4
import os
import json
import logging
import requests
from minio import Minio
from pathlib import Path
from dotenv import load_dotenv
import time 

PIPELINE_VERSION = "v1"
ENVIRONMENT = "dev"

env_path = Path(__file__).resolve().parents[2] / "configs" / ".env"
load_dotenv(env_path)

STOOQ_GOLD_URL = os.getenv("STOOQ_GOLD_URL")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

#cấu hình log
logging.basicConfig(
	level = logging.INFO,
	format = "%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def get_minio_client() -> Minio:
	return Minio(
		MINIO_ENDPOINT,
		access_key = MINIO_ACCESS_KEY,
		secret_key = MINIO_SECRET_KEY,
		secure= False
	)

def keo_gold_ingest(retries=3):
	for attmept in range(retries):
		try:
			start_time = datetime.now()
			response = requests.get(STOOQ_GOLD_URL, timeout=30)
			response.raise_for_status()
			
			latency = int((datetime.now() - start_time) * 1000)

			df = pd.to_csv(StringIO(response.text))
			if df.empty():
				raise ValueError("khong thay du lieu")

			df["Date"] = pd.to_datetime(df["Date"])
			df.sort_values(df["Date"])

			return df, response, latency

		except Exception as e:
			logger.warning(f"Ingest that bai do {e}")
			time.sleep(2 ** attmept)

	raise RuntimeError("Thu lai that bai")

def build_metadata_source(response, latency):
	return {
		"source_name": "stooq_url",
		"endpoint": response.request.path_url,
		"http_status": response.status_code,
		"request_time": latency,
		"response_size_bytes": len(response.content)
	}

def build_metadata_ingestion(retry_count: int =0):
	return {
		"ingest_id": uuid.uuid4(),
		"ingest_time_utc": datetime.now(timezone.utc).isoformat(),
		"ingest_type": "url pull",
		"pipeline_ver": PIPELINE_VERSION,
		"envi": ENVIRONMENT
	}

def build_file_name(ingestion_time: datetime):
	timestamp = ingestion_time.now(utc).strftime("%Y%m%dH%M%S")
	uid = uuid4().hex[:6]

	return f"stooq_gold_price_{timestamp}_{uid}"

def build_object_path(ingestion_time: datetime):
	file_path = (
		f"raw/gold_price/"
		f"source=stooq_url/"
		f"year={ingestion_time.year}/"
		f"month={ingestion_time.month:02d}/"
		f"day={ingestion_time.day:02d}/")

	file_name = build_file_name(ingestion_time)
	
	data_path = file_path + file_name + ".csv"
	metadata_path = file_path + file_name + "_metadata.json"

	return data_path, metadata_path

def push_csv_to_minio(client: Minio,object_path: str, data: pd.DataFrame):
	data = data.to_csv(index = False).encode("utf-8")

	client.put_object(
		bucket_name = MINIO_BUCKET,
		object_name = object_path,
		data = BytesIO(data),
		length = len(data),
		content_type= "text/csv"
		) 

def push_metadata_to_csv(client:Minio,object_path: str,metadata: dict):
	metadata = json.dumps(metadata, indent = 2).encode("utf-8")

	client.put_object(
		bucket_name= MINIO_BUCKET,
		object_name= object_path,
		data = BytesIO(metadata),
		length = len(metadata),
		content_type = "application/json"
		)

def main():
	logger.INFO("Bat dau stooq gold ingest")

	client = get_minio_client()

	if not client.bucket_exists(MINIO_BUCKET):
		client.make_bucket(MINIO_BUCKET)

	data, response, latency = keo_gold_ingest()

	metadata_source = build_metadata_source(response, latency)
	metadata_ingestion = build_metadata_ingestion()

	ingestion_time = datetime.fromisoformat(metadata_ingestion["ingest_time_utc"])

	data_path, metadata_path = build_object_path(ingestion_time)

	metadata = {
		"metadata_source": metadata_source,
		"metadata_ingestion": metadata_ingestion,
		"data_path": data_path
	}

	push_csv_to_minio(client, data_path, data)
	push_metadata_to_csv(client, metadata_path, metadata)

	logger.info(f"Hoan tat ingest va luu tai{data_path}")

if __name__ == "__main__":
	try:
		logger.info("Bat dau gold ingest")
		main()
		logger.info("Ingest hoan tat")
	except Exception as e:
		logger.exception(f"Ingest that bai do {e}")
		exit(1)