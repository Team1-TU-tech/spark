from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import os
from dotenv import load_dotenv

# .env 파일을 로드하여 환경 변수 설정
load_dotenv()  # .env 파일을 읽어 환경 변수로 설정

conf = SparkConf()
conf.set("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
conf.set("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY"))  # AWS 액세스 키
conf.set("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY"))  # AWS 시크릿 키

# Spark 세션 생성
spark = SparkSession.builder.config(conf=conf).appName("Read S3 Data").getOrCreate()

df = spark.read.format('parquet').load('s3a://t1-tu-data/logs/')

df.show()
