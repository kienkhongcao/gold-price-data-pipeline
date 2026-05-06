import os
import pandas as pd 
from pathlib import Path 
from datetime import datetime
from minio import Minio
import logging
from dotenv import load_dotenv
from io import BytesIO

env_path = Path(__file__).resolve().parents[2] / "configs" / ".env"
load_dotenv(env_path)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET","datalake")

logging.basicConfig(
	level = logging.INFO,
	format = "%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

client = Minio(
	MINIO_ENDPOINT,
	access_key = MINIO_ACCESS_KEY,
	secret_key = MINIO_SECRET_KEY,
	secure = False
)

def read_raw(prefix:str)->pd.DataFrame:
	objs = client.list_object(MINIO_BUCKET, prefix=prefix, recursive=True)
	csv_objs = [obj for obj in objs if obj.object_name.endswith(".csv")]

	print(f"[DEBUG] at {prefix} founded {len(objs)} objects")

	if not csv_objs:
		raise RuntimeError(f"Not found any csv file in {prefix}")

	lastest_obj = max(csv_objs, key = lambda x: x.last_modified)

	data = client.get_object(MINIO_BUCKET, lastest_obj.object_name).read()

	df = pd.read_csv(BytesIO(data))
	return df

def deep_cleaning(df, dcol, vcol, name):
	if dcol not in df.columns or vcol not in df:
		raise ValueError(f"Missing value: {name}")

	df = df[[dcol, vcol]].copy()
	df.columns = ["Date", "value"]

	df["Date"] = pd.to_datetime(df["Date"],errors= coerce)
	df["value"] = pd.to_numeric(df["value"], errors= coerce)

	df.dropna().drop_duplicate().sort_values("Date")
	df.set_index("Date").resample("D").ffill(limit=3)
	logger.info(f"cleaned {name}: {len(df)}")

	df.reset_index()
	return df

def push_to_minio(df, name):
	now = datetime.utcnow()
	timestamp = now.strftime("%Y%m%dH%M%S")

	path = (
		f"silver/{name}/"
		f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
		f"{name}_cleaned_{timestamp}.csv"
	)

	data = df.to_csv(index=False)

	client.put_object(
		MINIO_BUCKET,
		path,
		BytesIO(data),
		len(data),
		content_type= "text/csv"
	)

def main():
	Logger.info("Start silver process")

	source = {
		"gold_price": ("raw/gold_price/", "Date", "Close"),
		"dxy": ("raw/dxy_index", "Date/", "DXY"),
		"fedfunds": ("raw/fred_fedfunds/", "Date", "FedFunds"),
		"cpi": ("raw/fred_cpi", "Date/", "CPI"),
		"dgs10": ("raw/fred_dgs10_series/", "Date", "DGS10")
	}

	for name, (prefix, dcol, vcol) in source.item():
		try:
			logger.info(f"Start cleaning {name}")
			raw = read_raw(prefix)
			clean = deep_cleaning(raw, dcol, vcol, name)
			push_to_minio(clean, name)			
		except Exception as e:
			logger.exception(f"Clean false because: {e}")

	logger.info(f"Cleand {name}: {len(clean)} rows") 

if __name__ == "__main__":
	try:
		logger.info("bắt đầu chu trình silver")
		main()
		logger.info("Hoàn tất chu trình silver")
	except Exception as e:
		raise RuntimeError(f"Chu trình silver thất bại do: {e}")
		exit(1)