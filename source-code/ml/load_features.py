from io import BytesIO
import pandas as pd 
import os
from minio import Minio
from dotenv import load_dotenv
import re
from datetime import datetime
from pathlib import Path

env_path = Path(__file__).resolve().parents[2] / "configs" / ".env"
load_dotenv(env_path)

client = Minio(
	os.getenv("MINIO_ENDPOINT", "localhost:9001"),
	access_key = os.getenv("MINIO_ACCESS_KEY"),
	secret_key = os.getenv("MINIO_SECRET_KEY"),
	secure= False
)

BUCKET = os.getenv("MINIO_BUCKET")
if not BUCKET:
	raise RuntimeError("Không tìm thấy bucket")

#hàm lấy đường dẫn file gold feature mới nhất
def get_lastest_gold_prefix():
	base = "gold/features/"
	objs = client.list_objects(BUCKET, prefix=base, recursive=True)

	dates = set()

	for obj in objs:
		m = re.search(r"date=(\d{4}-\d{2}-\d{2})", obj.object_name)
		if m:
			dates.add(m.group(1))

	if not dates:
		raise RuntimeError("Không tìm thấy thư mục")

	lastest = max(dates, key = lambda d: datetime.strptime(d, "%Y-%m-%d"))
	print(f"[INFO] file gold feature mới nhất: {lastest}")

	return f"{base}date={lastest}/"

#hàm đọc
def read_gold_features(prefix: str) -> pd.DataFrame:
	objs = client.list_objects(BUCKET, prefix=prefix, recursive=True)
	for obj in objs:
		if obj.object_name.endswith(".csv"):
			data = client.get_object(BUCKET, obj.object_name).read()
			df = pd.read_csv(BytesIO(data), parse_dates= ["Date"])
			return df 
	raise RuntimeError("Không tìm thấy gold features")

#gọi hàm đọc
def load_data():
	PREFIX = get_lastest_gold_prefix()
	df = read_gold_features(PREFIX)
	df = df.sort_values("Date").set_index("Date")
	marco_cols = ['Dxy','FedFunds','CPI','DGS10']
	df[marco_cols]= df[marco_cols].ffill()
	df = df.dropna()

	return df
'''	target = "Gold"
	features = [c for c in df.columns if c != target]
'''

