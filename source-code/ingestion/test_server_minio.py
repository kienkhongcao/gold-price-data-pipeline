import os
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from io import BytesIO

def test_minio_connection():
	load_dotenv("../../configs/.env")

	endpoint = os.getenv("MINIO_ENDPOINT")
	access_key = os.getenv("MINIO_ACCESS_KEY")
	secret_key = os.getenv("MINIO_SECRET_KEY")
	secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
	bucket_name = os.getenv("MINIO_BUCKET")

	#tạo client
	client = Minio(
		endpoint = endpoint,
		access_key = access_key,
		secret_key = secret_key,
		secure = secure,
	)

	try:
		if client.bucket_exists(bucket_name):
			print(f"Bucket {bucket_name} đã tồn tại")
		else:
			client.make_bucket(bucket_name)
			print(f"Đã tạo bucket {bucket_name}")

		test_content = b"day la file test minio."
		test_path = "raw/test/test.txt"

		data_stream = BytesIO(test_content)

		#load thử
		client.put_object(
			bucket_name= bucket_name,
			object_name= test_path,
			data = data_stream,
			length= len(test_content),
			content_type= "text/plain"
	)
		print("Đã kết nối Minio thành công")
		print(f"đã load test file tại {test_path}")	

	except S3Error as e:
		print(f"Lỗi minio: {e}")
	except Exception as e:
		print(f"Lỗi {e}")

if __name__ == "__main__":
	test_minio_connection()
