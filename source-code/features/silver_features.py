import os
from pathlib import Path
import pandas as pd 
from io import BytesIO
from dotenv import load_dotenv
from datetime import datetime
from minio import Minio
import logging


env_path = Path(__file__).resolve().parents[2] / "configs" / ".env"
load_dotenv(env_path)

#biến mt
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "datalake")

#cấu hình log
logging.basicConfig(
	level=logging.INFO,
	format= "%(asctime)s - %(levelname)s - %(message)s"
	)
logger = logging.getLogger(__name__)

#kết nối server minio
client = Minio(
	MINIO_ENDPOINT,
	access_key = MINIO_ACCESS_KEY,
	secret_key = MINIO_SECRET_KEY,
	secure = True
)

#loop đọc và load file từ minio 
def read_raw(prefix: str) -> pd.DataFrame:
	objs = list(client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))

	csv_objs = [obj for obj in objs if obj.object_name.endswith(".csv")]

	print(f"[DEBUG] prefix={prefix}, found {len(objs)} objects")

	if not csv_objs:
		raise RuntimeError(f"Không tìm thấy file tại {prefix}")

	latest_obj = max(csv_objs, key=lambda x: x.last_modified)
	logger.info(f"Reading lastest file: {latest_obj.object_name}")

	data = client.get_object(MINIO_BUCKET, latest_obj.object_name).read()
	df = pd.read_csv(BytesIO(data))

	return df



#làm sạch
def clean_series(df, date_col, value_col, name):
	if date_col not in df.columns or value_col not in df.columns:
		raise ValueError(f"Missing columns: {name}")

	df = df[[date_col, value_col]].copy()
	df.columns = ["Date", "value"]

	df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
	df["value"] = pd.to_numeric(df["value"], errors="coerce")

	df = df.dropna().drop_duplicates("Date").sort_values("Date")
	df = df.set_index("Date").resample("D").ffill(limit=3)

	logger.info(f"{name}: cleaned {len(df)} rows")
 
	return df.reset_index()

#hàm đẩy lại lên minio
def push_to_minio(df, name):
	now = datetime.utcnow()
	timestamp = now.strftime("%Y%m%dT%H%M%S")

	path = (
		f"silver/{name}/"
		f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
		f"{name}_clean_{timestamp}.csv"
	)

	data = df.to_csv(index=False).encode()
	
	client.put_object(
		MINIO_BUCKET,
		path,
		BytesIO(data),
		len(data),
		content_type="text/csv")

def main():
	logger.info("Bắt đầu chu trình silver")

	sources = {
		"gold_price": ("raw/gold_price/", "Date", "Close"),
		"dxy": ("raw/dxy_index/", "Date", "DXY"),
		"fedfunds": ("raw/fred_fedfunds/", "Date", "FedFunds"),
		"cpi": ("raw/fred_cpi/", "Date", "CPI"),
		"dgs10": ("raw/fred_10y_series/", "Date", "DGS10")
	}

	for name, (prefix, dcol, vcol) in sources.items():
		try:
			logger.info(f"đang xử lý: {name}")

			raw = read_raw(prefix)
			clean = clean_series(raw, dcol, vcol, name)
			push_to_minio(clean, name)
		except Exception as e:
			logger.warning(f"Xử lý {name} thất bại: {e}")

	logger.info("đã hoàn thành chu trình silver")

if __name__=="__main__":
	try:
		logger.info("[START] silver features")
		main()
		logger.info("[SUCCESS] silver features")
	except Exception as e:
		logger.exception(f"[FALSE] silver feature, cause: {e}")
		exit(1)

