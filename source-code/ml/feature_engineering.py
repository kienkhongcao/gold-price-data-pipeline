import pandas as pd
import os
from load_features import load_data
from pathlib import Path
from dotenv import load_dotenv
from io import BytesIO
from minio import Minio
from datetime import datetime
import logging

env_path = Path(__file__).resolve().parents[2] / "configs" / ".env"
load_dotenv(env_path)

logging.basicConfig(
	level= logging.INFO,
	format= "%(asctime)s - %(levelname)s - %(message)s"
	)
logger = logging.getLogger(__name__)

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "datalake")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")

client = Minio(
	MINIO_ENDPOINT,
	access_key = os.getenv("MINIO_ACCESS_KEY"),
	secret_key = os.getenv("MINIO_SECRET_KEY"),
	secure = False
	)


'''def load_dataset():
	df, target, features = load_data()
	return df, target, features


def create_lags(df, features):
	lags = [1,3,7,14,30]

	for col in features + ["Gold"]:
		 for lag in lags:
		 	df[f"{col}_lag{lag}"] = df[col].shift(lag)

	return df 

def create_rolling(df):
	df["Gold_roll7"] = df["Gold"].rolling(7).mean()
	df["Gold_roll30"] = df["Gold"].rolling(30).mean()

	return df 

def create_financial(df):
	df["Gold_return"] = df["Gold"].pct_change()
	df["yield_spread"] = df["DGS10"] - df["FedFunds"]

	return df

def main():
	df, target, features = load_data()

	df = create_lags(df, features)

	df = create_rolling(df)

	df = create_financial(df)

	df = df.dropna()

	buffer = BytesIO()
	data = df.reset_index().to_csv(buffer, index=False)
	buffer.seek(0)


	now = datetime.utcnow().date()
	object_path = f"features/date={now}/features_ml.csv"
	client.put_object(
		MINIO_BUCKET,
		object_path,buffer,
		length=buffer.getbuffer().nbytes,
		content_type = "text/csv" )

	print(f"Đã lưu file features tại bucket: {MINIO_BUCKET}, đường dẫn: {object_path}")
	print("Định dạng:", df.shape)
	print(df.columns)
'''

def gold_lag(df):
	for lag in [7, 14, 30]:
		df[f"Gold_lag{lag}"] = df["Gold"].shift(lag)

	df["Gold_roll30"] = df["Gold"].shift(1).rolling(30).mean()

	df["Gold_return_1"] = df["Gold"].pct_change(1)
	df["Gold_return_7"] = df["Gold"].pct_change(7)

	df["Gold_momentum_7"] = df["Gold"] - df["Gold"].shift(7)
	df["Gold_volatility_7"] = df["Gold"].shift(1).rolling(7).std()

	return df

def macro_trans(df):
	for col in ["Dxy", "FedFunds", "CPI", "DGS10"]:
		df[f"{col}_change_1"] = df[col].pct_change(1)
		df[f"{col}_change_7"] = df[col].pct_change(7)

		for lag in [7, 14, 30]:
			df[f"{col}_lag_{lag}"] = df[col].shift(lag)

		df[f"{col}_trend_30"] = df[col].shift(1).rolling(30).mean()

		df[f"{col}_vol_30"] = df[col].shift(1).rolling(30).std()
	return df

def economic_feas(df):
	df["real_yield"] = df["DGS10"] - df["CPI"]

	df["yield_spread"] = df["DGS10"] - df["FedFunds"]
	df["DXY_inverse"] = -df["Dxy_change_1"]

	df["Dxy_x_DGS10"] = df["Dxy"] * df["DGS10"]
	df["realYield_x_Dxy"] = df["real_yield"] * df["Dxy"]

	return df

def main():
	logger.info("Bắt đầu features engineering")

	df = load_data()
	df =df.reset_index()

	required_cols = ["Gold", "Dxy", "FedFunds", "CPI", "DGS10"]
	missing = [c for c in required_cols if c not in df.columns]
	if missing:
		raise ValueError(f"Không thấy column: {missing}")
	
	df["Date"] = pd.to_datetime(df["Date"])
	df = df.sort_values("Date").set_index("Date")

	df = gold_lag(df)
	df = macro_trans(df)
	df = economic_feas(df)

	df = df.ffill().dropna()

	buffer = BytesIO()
	df.reset_index().to_csv(buffer, index=False)
	buffer.seek(0)

	now = datetime.utcnow().date()
	obj_path = f"features/date={now}/features_ml.csv"

	client.put_object(
		MINIO_BUCKET,
		obj_path,
		buffer,
		length=buffer.getbuffer().nbytes,
		content_type = "text/csv"
		)
	logger.info(f"Đã lưu file features tại bucket: {MINIO_BUCKET}, đường dẫn: {obj_path}")

if __name__ == "__main__":
	try:
		logger.info("[START] feature engineering")
		main()
		logger.info("[SUCCESS] feature engineering")
	except Exception as e:
		logger.exception(f"[FALSE] feature engineering, cause: {e}")
		exit(1)