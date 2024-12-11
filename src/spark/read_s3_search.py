from pyspark.sql import SparkSession
import os
from pyspark.sql.types import StructType, StructField, StringType

# Spark 세션 생성
spark = SparkSession.builder.appName("Read S3 Data").getOrCreate()
spark.conf.set("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
spark.conf.set("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
spark.conf.set("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
    #.config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    #.config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    #.config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") \


# 예상되는 스키마 정의
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("device", StringType(), True),
    StructField("action", StringType(), True),
    StructField("ticket_id", StringType(), True),
    StructField("keyword", StringType(), True),
    StructField("title", StringType(), True),
    StructField("category", StringType(), True),
    StructField("region", StringType(), True),  # 'region' 컬럼을 Integer로 강제 변환
    StructField("timestamp", StringType(), True),  # 'region' 컬럼을 Integer로 강제 변환
])


try:
    print("Starting to read S3 file...")
    #df = spark.read.option("mergeSchema", "true").format('parquet').load("s3a://t1-tu-data/logs/")
    #df = spark.read.format('parquet').load("s3a://t1-tu-data/search_log/")
    df = spark.read.format('parquet').schema(schema).load("s3a://t1-tu-data/search_log/")

    #df.select(df.region.cast("string")).collect()
    print("Data preview:")
    df.show()
    df.printSchema()
    
except Exception as e:
    print(f"Error reading S3 file: {e}")
finally:
    spark.stop()
