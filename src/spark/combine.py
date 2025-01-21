from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BinaryType, LongType
import os
import boto3
import datetime as dt
from pyspark.sql.functions import udf, col


# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Read S3 Data in Parallel") \
    .getOrCreate()

# 스키마 정의
schema = StructType([
    StructField("user_id", BinaryType(), True),
    StructField("device", StringType(), True),
    StructField("action", StringType(), True),
    StructField("category", StringType(), True),
    StructField("region", StringType(), True),
    StructField("keyword", StringType(), True),
    StructField("birthday", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("email", StringType(), True),
    StructField("error", StringType(), True),
    StructField("requested_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("create_at", StringType(), True),
])

# 사용자 정의 함수 정의 (BinaryType -> String 변환)
def binary_to_string(binary_data):
    if binary_data is None:
        return None
    return binary_data.decode("utf-8")

binary_to_string_udf = udf(binary_to_string, StringType())


try:
    print("Starting to read S3 files in parallel...")

    # S3 클라이언트 생성
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name="ap-northeast-2"
    )

    # 1. S3에서 logs 디렉토리 파일 리스트 가져오기
    bucket_name = "t1-tu-data"
    prefix = "logs"
    obj_list = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if 'Contents' not in obj_list:
        print("No files found in the specified S3 bucket and prefix.")
        raise Exception("No files found")

    file_keys = [
        f"s3a://{bucket_name}/{o['Key']}" for o in obj_list["Contents"]
        if o["Key"].split("/")[-1] > "2024-12-13"
    ]

    print(f"Found {len(file_keys)} files to process.")

    # 2. Spark의 DataFrame API로 파일 병렬 읽기
    # 파일 리스트를 병렬로 읽고 병합
    # 파일 읽기
    #df = spark.read.schema(schema).parquet(*file_keys)
    df = spark.read.option("mergeSchema", "true").parquet(*file_keys)

    # user_id 데이터 타입 확인 및 변환
    if isinstance(df.schema["user_id"].dataType, BinaryType):
        df = df.withColumn("user_id", binary_to_string_udf(col("user_id")))
    
    # user_id를 STRING으로 변환
    df = df.withColumn("user_id", col("user_id").cast("string"))

    df.printSchema()
    df.show(5)
    print("::::: Loading Completed :::::")

    # 3. S3에 저장
    today = dt.datetime.now().strftime("%Y-%m-%d")
    output_path = f"s3a://{bucket_name}/visualize/log_{today}.parquet"

    df.write.mode("overwrite").parquet(output_path)

    print(f"로그가 S3에 업로드되었습니다: {output_path}")

# except Exception as e:
#     print(f"Error during processing: {e}")
finally:
    spark.stop()