import os
import pandas as pd 
from minio import Minio
from io import BytesIO
from dotenv import load_dotenv
from pathlib import Path 
from datetime import datetime
import logging

env_path = Path(__file__).resolve().parents[2] / "configs" / ".env"
load_dotenv(env_path)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "datalake")

logging.basicConfig(
	level= logging.INFO,
	format = "%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

client = Minio(
	MINIO_ENDPOINT,
	access_key = MINIO_ACCESS_KEY,
	secret_key = MINIO_SECRET_KEY,
	secure = False
)

def read_silver(prefix: str):
	objs = client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True)

	csv_objs = [obj for obj in objs if obj.object_name.endswith(".csv")]
	if not csv_objs:
		raise RuntimeError(f"object not found at: {prefix}")
	lastest_obj = sorted(csv_objs, key = lambda x: x.object_name)[-1]
	data = client.get_objects(MINIO_BUCKET, lastest_obj.object_name).read()
	df = pd.read_csv(BytesIO(data))

	return df

def main():
	logger.info("Start gold process")

	gold = read_silver("silver/gold_price")
	dxy = read_silver("silver/dxy/")
	fedfunds = read_silver("silver/fedfunds/")
	cpi = read_silver("silver/cpi/")
	dgs10 = read_silver("silver/dgs10/")

	for df in [gold, dxy, fedfunds, cpi, dgs10]:
		df["Date"] = pd.to_datetime(df["Date"])
		df.sort_values("Date")
		df.set_index("Date")

	gold = gold.rename(columns={"value": "Gold"})
	dxy = dxy.rename(columns={"value": "Dxy"})
	fedfunds = fedfunds.rename(columns= {"value": "FedFunds"})
	cpi = cpi.rename(columns={"value": "CPI"})
	dgs10 = dgs10.rename(columns={"value": "DGS10"})

	features = gold.join(["dxy", "fedfunds", "cpi", "dgs10"], how = "left")
	features.columns = ["Date", "Gold", "Dxy","FedFunds", "CPI", "DGS10"]
	features = features.ffill(limit=3)

	now = datetime.utcnow().date()
	object_path = f"gold/features/date={now}/gold_features.csv"
	data = features.reset_index().to_csv(index=False).encode()

	client.put_object(
		MINIO_BUCKET,
		object_path,
		BytesIO(data),
		len(data),
		content_type= "text/csv"
		)


if __name__=="__main__":
	try:
		logger.info("Bắt đầu chu trình gold")
		main()
		logger.info("Hoàn tất chu trình gold")
	except Exception as e:
		logger.warning(f"Chu trình gold thất bại do: {e}")
		exit(1)