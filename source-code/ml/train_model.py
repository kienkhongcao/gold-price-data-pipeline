import os
from minio import Minio
from pathlib import Path 
from dotenv import load_dotenv
import pandas as pd
import re
from datetime import datetime
import logging

env_path = Path(__file__).resolve().parents[2] / "configs" / ".env"
load_dotenv(env_path)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "datalake")

logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s - %(levelname)s - %(message)s"
	)

logger = logging.getLogger(__name__)

client = Minio(
	MINIO_ENDPOINT,
	access_key = os.getenv("MINIO_ACCESS_KEY"),
	secret_key = os.getenv("MINIO_SECRET_KEY"),
	secure = False
	)

def load_lastest_dataset():
	prefix = "features/"

	objects = client.list_objects(
		MINIO_BUCKET,
		prefix=prefix,
		recursive=True
	)

	date_map = {}

	for obj in objects:
		if obj.object_name.endswith(".csv"):
			l = re.search(r"date=(\d{4}-\d{2}-\d{2})", obj.object_name)

			if l:
				date_str = l.group(1)
				date_map[date_str] = obj.object_name

	if not date_map:
		raise RuntimeError("Không thấy file mới nhất")

	lastest_date = max(date_map, key=lambda d: datetime.strptime(d, "%Y-%m-%d"))

	lastest_file = date_map[lastest_date]
	print(f"[INFO] Load dataset: {lastest_file}")

	response = client.get_object(MINIO_BUCKET, lastest_file)
	df = pd.read_csv(response)

	return df


def validate_data(df):
	required_cols = ["Gold", "Dxy", "FedFunds", "CPI", "DGS10"]

	missing = [c for c in required_cols if c not in df.columns]
	if missing:
		raise ValueError(f"Không thấy column: {missing}")

def select_features(df):

	features = ["Gold_roll30","Gold_return_7","CPI_trend_30","CPI_lag_7",
				"real_yield","yield_spread","Dxy_trend_30","Dxy_change_7","realYield_x_Dxy"]

	X = df[features]
	y = df["Gold"]

	return X, y, features

def train_test_splits(X, y, split_ratio=0.8):
	split = int(len(X) * split_ratio)

	return (
		X.iloc[:split], X.iloc[split:],
		y.iloc[:split], y.iloc[split:]
	)

def train_models(X_train, y_train):
	#random forest
	from sklearn.ensemble import RandomForestRegressor

	rf_model = RandomForestRegressor(
		n_estimators=100,
		max_depth=10,
		random_state=0
		)
	rf_model.fit(X_train, y_train)

	#xgboost
	from xgboost import XGBRegressor

	xgb_model = XGBRegressor(
		n_estimators=200,
		max_depth=5,
		learning_rate=0.05
		)
	xgb_model.fit(X_train, y_train)

	return rf_model, xgb_model

def evaluate_model(model, X_test, y_test):
	from sklearn.metrics import mean_squared_error

	y_pred = model.predict(X_test)
	rmse = mean_squared_error(y_test, y_pred)

	return rmse, y_pred

def get_feature_importance(model, features):
	return pd.DataFrame({
		"feature": features,
		"importance": model.feature_importances_
	}).sort_values(by="importance", ascending=False)


def save_model(model, name):
	import joblib
	from io import BytesIO
	
	buffer = BytesIO()
	joblib.dump(model, buffer)
	buffer.seek(0)

	now = datetime.utcnow().date()
	path = f"model_save/{name}/date={now}/model.pkl"

	client.put_object(MINIO_BUCKET, path, buffer, length=buffer.getbuffer().nbytes)

def main():
	df = load_lastest_dataset()
	df = df.reset_index()

	validate_data(df)

	df["Date"] = pd.to_datetime(df["Date"])
	df = df.sort_values("Date")

	X, y, features = select_features(df)

	X_train, X_test, y_train, y_test = train_test_splits(X, y)

	rf_model,xgb_model = train_models(X_train, y_train) 

	rf_rmse, rf_pred = evaluate_model(rf_model, X_test, y_test)
	xgb_rmse, xgb_pred = evaluate_model(xgb_model, X_test, y_test)

	print(f"RF rmse: {rf_rmse}")
	print(f"XGB rmse: {xgb_rmse}")

	print(get_feature_importance(rf_model, features))
	print(get_feature_importance(xgb_model, features))

	save_model(xgb_model, "xgb")

if __name__ == "__main__":
	try:
		logger.info("[START] trainning model")
		main()
		logger.info("[SUCCESS] trainning model")
	except Exception as e:
		logger.exception(f"[FALSE] trainning model, cause: {e}")
		exit(1)

'''#Đánh giá chỉ số


rmse_lr = mean_squared_error(y_test, y_pred_lr)
r2_lr = r2_score(y_test, y_pred_lr)

rmse_rf = mean_squared_error(y_test, y_pred_rf)
r2_rf = r2_score(y_test, y_pred_rf)

rmse_xgb = mean_squared_error(y_test, y_pred_xgb)
r2_xgb = r2_score(y_test, y_pred_xgb)

#bảng so sánh
tatistic_table = pd.DataFrame({
	"model":["lr", "rf", "xgb"],
	"rmse": [rmse_lr, rmse_rf, rmse_xgb],
	"r2": [r2_lr, r2_rf, r2_xgb]
	})
print("bảng so sánh chỉ số")
print(tatistic_table)
'''