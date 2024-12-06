from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

load_dotenv()

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Read S3 Data") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") \
    .getOrCreate()

# S3에서 Parquet 파일 읽기
df = spark.read.parquet("s3a://t1-tu-data/logs/2024-12-06_01-39-04.parquet")

# 데이터 확인
df.show()

