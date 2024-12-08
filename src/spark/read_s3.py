from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

load_dotenv()

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Read S3 Data") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") \
    .getOrCreate()

# 예상되는 스키마 정의
schema = StructType([
    StructField("region", IntegerType(), True),  # 'region' 컬럼을 Integer로 강제 변환
])

try:
    print("Starting to read S3 file...")
    df = spark.read.format('parquet').load("s3a://t1-tu-data/logs/")
    print("Data preview:")
    df.show()
except Exception as e:
    print(f"Error reading S3 file: {e}")
finally:
    spark.stop()

