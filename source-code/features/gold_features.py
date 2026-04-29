import os
import pandas as pd 
from io import BytesIO
from minio import Minio
from dotenv import load_dotenv
from pathlib import Path 
from datetime import datetime
import logging

env_path = Path(__file__).resolve().parents[2] / "configs" / ".env"
load_dotenv(env_path)

logging.basicConfig(
	level = logging.INFO,
	format = "%(asctime)s - %(levelname)s - %(message)s"
	)
logger = logging.getLogger(__name__)

#biến
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "datalake")

client = Minio(
	MINIO_ENDPOINT,
	access_key = MINIO_ACCESS_KEY,
	secret_key = MINIO_SECRET_KEY,
	secure = False
	)

#hàm đọc file silver từ minio
def read_silver(prefix):
	objs = client.list_objects(MINIO_BUCKET, prefix=prefix, recursive= True)

	csv_file = [obj for obj in objs if obj.object_name.endswith(".csv")]

	if not csv_file:
		raise RuntimeError(f"Không thấy file tại đường dẫn: {prefix}")

	latest_file = sorted(csv_file, key = lambda x: x.object_name)[-1]

	print(f"[INFO] đang xử lý file: {latest_file.object_name}")

	data = client.get_object(MINIO_BUCKET, latest_file.object_name).read()
	return pd.read_csv(BytesIO(data))

def main():
	logger.info("Bắt đầu chu trình gold features")

	gold = read_silver("silver/gold_price/")
	dxy = read_silver("silver/dxy/")
	fedfunds = read_silver("silver/fedfunds/")
	cpi = read_silver("silver/cpi/")
	dgs10 = read_silver("silver/dgs10/")

	for df in [gold, dxy, fedfunds, cpi, dgs10]:
		df["Date"] = pd.to_datetime(df["Date"])
		df.sort_values("Date", inplace=True)

	gold = gold.set_index("Date")
	dxy = dxy.set_index("Date")
	fedfunds = fedfunds.set_index("Date")
	cpi = cpi.set_index("Date")
	dgs10 = dgs10.set_index("Date")


	gold = gold.rename(columns={"value": "Gold"})
	dxy = dxy.rename(columns={"value": "Dxy"})
	fedfunds = fedfunds.rename(columns={"value": "FedFunds"})
	cpi = cpi.rename(columns={"value": "CPI"})
	dgs10 = dgs10.rename(columns={"value": "DGS10"})


	features = gold.join([dxy, fedfunds, cpi, dgs10], how = "left")
	features.columns = ["Gold", "Dxy", "FedFunds", "CPI", "DGS10"]
	features = features.ffill()

	now = datetime.utcnow().date()
	object_path = f"gold/features/date={now}/gold_features.csv"
	data = features.reset_index().to_csv(index=False).encode()

	client.put_object(MINIO_BUCKET, object_path, BytesIO(data), len(data), content_type = "text/csv")
	logger.info(f"Đã xử lý và lưu features tại {object_path}")

if __name__ == "__main__":
	try:
		logger.info("[START] gold features")
		main()
		logger.info("[SUCCESS] gold features")
	except Exception as e:
		logger.exception(f"[FALSE] gold feature, cause: {e}")
		exit(1)

