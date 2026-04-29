import os
import json
from pathlib import Path 
from dotenv import load_dotenv
from minio import Minio
import logging
import time
from datetime import datetime, timezone
import requests
import pandas as pd 
from io import StringIO
from io import BytesIO
import uuid
from uuid import uuid4




#Cấu hình
PIPELINE_VERSION = "1.0"
ENVIRONMENT = "dev"

#biến môi trường
env_path = Path(__file__).resolve().parents[2] / "configs" / ".env"
load_dotenv(env_path)

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
FRED_API_KEY = os.getenv("FRED_API_KEY")
FRED_DXY_SERIES=os.getenv("FRED_DXY_SERIES")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "datalake")

#Cấu hình log
logging.basicConfig(
	level = logging.INFO,
	format = "%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

#hàm dựng cấu hình minio
def get_minio_client() -> Minio:
	return Minio(
		MINIO_ENDPOINT,
		access_key = MINIO_ACCESS_KEY,
		secret_key = MINIO_SECRET_KEY,
		secure = False
		)

#hàm kéo dữ liệu
def keo_dxy_index(retries=3):
	for attempt in range(retries):
		try:
			start_time = time.time()

			params = {
				"series_id": FRED_DXY_SERIES,
				"api_key": FRED_API_KEY,
				"file_type": "json"
		    }

			response = requests.get(FRED_BASE_URL, params=params, timeout=30)
			latency = int((time.time() - start_time) * 1000)

			response.raise_for_status()

			data = response.json()["observations"]

			df = pd.DataFrame(data)[["date", "value"]]
			df.columns = ["Date", "DXY"]
			df["Date"] = pd.to_datetime(df["Date"])
			df["DXY"] = pd.to_numeric(df["DXY"], errors="coerce")
			df = df.dropna().sort_values("Date")
			return df, response, latency
		except Exception as e:
			logger.warning(f"retry {attempt+1} - error: {e}")
			time.sleep(2 ** attempt)

	raise Exception("Thử lại thất bại")

def build_metadata_source(response, latency):
	return{
		"source_name" : "fred_dxy_index",
		"endpoint": response.url.replace(FRED_BASE_URL, ""),
		"http_status": response.status_code,
		"request_time": latency,
		"response_size_bytes": len(response.content)
	}

def build_metadata_ingestion(retry_count: int= 0):
	return {
		"ingest_id": str(uuid.uuid4()),
		"ingest_time_utc": datetime.now(timezone.utc).isoformat(),
		"ingestion_type": "url_pull",
		"pipeline_version": PIPELINE_VERSION,
		"environment": ENVIRONMENT,
		"retry_count": retry_count,
		"ingested_by": "dxy_index_ingest.py"
	}

def build_file_name(ingestion_time: datetime):
	timestamp = ingestion_time.strftime('%Y%m%dT%H%M%S')
	uid = uuid4().hex[:6]

	return f"fed_dxy_index_{timestamp}_{uid}"

def build_object_path(ingestion_time: datetime):
	file_path = (
		f"raw/dxy_index/"
		f"source=fred_dxy_index/"
		f"year={ingestion_time.year}/"
		f"month={ingestion_time.month:02d}/"
		f"day={ingestion_time.day:02d}/"
	)

	base_name = build_file_name(ingestion_time)

	data_path = file_path + base_name + ".csv"
	metadata_path = file_path + base_name +"_metadata.json"

	return data_path, metadata_path 

#hàm push data lên minio
def push_csv_to_minio(client:Minio, object_path:str, df:pd.DataFrame):

	data = df.to_csv(index= False).encode("utf-8")

	client.put_object(
		bucket_name = MINIO_BUCKET,
		object_name= object_path,
		data= BytesIO(data),
		length=len(data),
		content_type= "text/csv"
	)

def push_metadata_to_minio(client:Minio, object_path:str, metadata:dict):
	data = json.dumps(metadata, indent=2).encode("utf-8")

	client.put_object(
		bucket_name = MINIO_BUCKET,
		object_name= object_path,
		data= BytesIO(data),
		length=len(data),
		content_type= "application/json"
	)

#hàm chính
def main():
	logger.info("Bắt đầu DXY ingestion")

	client = get_minio_client()

	if not client.bucket_exists(MINIO_BUCKET):
		client.make_bucket(MINIO_BUCKET)
		print(f"Đã tạo bucket {MINIO_BUCKET}")

	payload, response, latency = keo_dxy_index()

	metadata_source = build_metadata_source(response, latency)
	metadata_ingestion = build_metadata_ingestion()

	ingestion_time = datetime.fromisoformat(
		metadata_ingestion["ingest_time_utc"]
		)


	data_path, metadata_path = build_object_path(ingestion_time)

	metadata = {
		"metadata_ingestion": metadata_ingestion,
		"metadata_source": metadata_source,
		"data_file": data_path
	}

	push_csv_to_minio(client, data_path, payload)
	push_metadata_to_minio(client, metadata_path, metadata)

	logger.info(f"Ingest hoan tat DXY INDEX den {data_path}")


if __name__ == "__main__":
	try:
		logger.info("[START] dxy index ingest")
		main()
		logger.info("[SUCCESS] dxy index ingest")
		main()
	except Exception as e:
		logger.exception(f"Ingest that bai do {e}")
		exit(1)