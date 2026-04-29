import os
import json
import time
import uuid
from uuid import uuid4
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

import pandas as pd
from io import BytesIO
from io import StringIO
import requests
from minio import Minio
from pathlib import Path


#Cấu hình
PIPELINE_VERSION = "v1.0" #để xác định phiên bản pipe, nếu thay features, model.. ta nâng phiên bản, tránh nhầm lẫn
ENVIRONMENT = "dev" #để xác định môi trường chạy pipe, dev như là thử nghiệm, có thể chạy local với data vừa để thử 

#Biến môi trường
env_path = Path(__file__).resolve().parents[2] / "configs" / ".env"
load_dotenv(env_path) #đọc file và load biến vào mt py

STOOQ_GOLD_URL = os.getenv("STOOQ_GOLD_URL")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "datalake")

#Cấu hình log
logging.basicConfig(
	level= logging.INFO,
	format = "%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

#Tạo hàm chứa cấu hình Minio
def get_minio_client() -> Minio:
	return Minio(
		MINIO_ENDPOINT,
		access_key = MINIO_ACCESS_KEY,
		secret_key = MINIO_SECRET_KEY,
		secure = False
		)
#Hàm kéo dữ liệu về
def keo_gold_price(retries=3):
	for attempt in range(retries):
		try:
			start_time = time.time() #lấy thời gian tại thời điểm hiện tại

			response = requests.get(STOOQ_GOLD_URL, timeout= 30)
			logger.info(f"Response preview:\n{response.text[:100]}")
			latency = int((time.time() - start_time) * 1000) # nhân 1k để chuyển milisecond cho dễ đọc

			response.raise_for_status() #trả về thông báo nếu request lỗi

			#chuyển sang csv
			df = pd.read_csv(StringIO(response.text)) #stringIO tạo file giả chứa respone.text giúp pd có thể đọc
			if df.empty:
				raise ValueError("API trả về file rỗng")

			#chuẩn hoá định dạng datetime
			df["Date"] = pd.to_datetime(df["Date"]) #vì khi kéo về Date thường ở dạng string nên ta phải đổi
			df = df.sort_values("Date")
			return df, response, latency

		except Exception as e:
			logger.warning(f"retry {attempt+1} - error: {e}")
			time.sleep(2 ** attempt)

	raise Exception("Thử lại thất bại")

def build_metadata_source(response, latency):
	return {
		"source_name": "stooq",
		"endpoint": response.request.path_url,
		"http_status": response.status_code,
		"request_time": latency,
		"response_size_bytes": len(response.content)
	}

def build_metadata_ingestion(retry_count: int =0 ):
	return {
		"ingestion_id": str(uuid.uuid4()),
		"ingest_time_utc": datetime.now(timezone.utc).isoformat(),
		"ingestion_type": "api_pull",
		"pipeline_version": PIPELINE_VERSION,
		"environment": ENVIRONMENT,
		"retry_count": retry_count,
		"ingested_by": "gold_api_ingest.py"
	}


def build_file_name(ingestion_time: datetime):
	timestamp = ingestion_time.strftime("%Y%m%dT%H%M%S")
	uid = uuid4().hex[:6] # random id, lấy 6 ký tự đầu string không dấu

	return f"stooq_gold_price_{timestamp}_{uid}"



def build_object_path(ingestion_time: datetime):
	file_path = (
		f"raw/gold_price/"
		f"source=stooq_gold_price/"
		f"year={ingestion_time.year}/"
		f"month={ingestion_time.month:02d}/"
		f"day={ingestion_time.day:02d}/"
	)

	base_name = build_file_name(ingestion_time)

	data_path = file_path + base_name + ".csv"
	metadata_path = file_path + base_name + "_metadata.json"

	return data_path, metadata_path

#hàm đẩy dữ liệu lên Minio
def push_csv_to_minio(client: Minio, object_path: str, df: pd.DataFrame):
	data = df.to_csv(index=False).encode("utf-8")
	
	client.put_object(
		bucket_name=MINIO_BUCKET,
		object_name=object_path,
		data=BytesIO(data),
		length=len(data),
		content_type="text/csv"
	)

def push_metadata_to_minio(client: Minio, object_path: str, metadata: dict):
	data = json.dumps(metadata, indent=2).encode("utf-8")

	client.put_object(
		bucket_name=MINIO_BUCKET,
		object_name=object_path,
		data=BytesIO(data),
		length=len(data),
		content_type="application/json"
	)

#hàm chính
def main():
	logger.info("Bắt đầu GoldAPI ingestion")

	client = get_minio_client()

	if not client.bucket_exists(MINIO_BUCKET):
		client.make_bucket(MINIO_BUCKET)

	data, response, latency = keo_gold_price()

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

	push_csv_to_minio(client, data_path, data)
	push_metadata_to_minio(client, metadata_path, metadata)

	logger.info(f"Hoan tat ingest GoldAPI den {data_path}")


if __name__ == "__main__":
	try:
		logger.info("[START] gold ingest")
		main()
		logger.info("[SUCCESS] gold ingest")
	except Exception as e:
		logger.exception("GoldAPI ingestion thất bại")
		exit(1)

