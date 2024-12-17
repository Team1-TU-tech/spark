from pyspark.sql import SparkSession
import os
#from dotenv import load_dotenv
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

#load_dotenv()

# Spark 세션 생성
spark = SparkSession.builder.appName("Read S3 Data").getOrCreate()

"""
# 예상되는 스키마 정의
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("device", StringType(), True),
    StructField("action", StringType(), True),
    StructField("category", StringType(), True),
    StructField("region", StringType(), True),  # 'region' 컬럼을 Integer로 강제 변환
    StructField("keyword", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("current_timestamp", StringType(), True),
])
"""
try:
    print("Starting to read S3 file...")
    ###########################
    log_dir_list=["Auth_log/","Kakao_log/","Kakao/","KakaoLogin_log/","KakaoLogout_log/","Login_log/","Logout_log/","search_log/","Signup_log/","view_detail_log/"]

    df=spark.read.parquet(f"s3a://t1-tu-data/{log_dir_list[0]}")

    for log_dir in log_dir_list[1:]:
        print(f"s3a://t1-tu-data/{log_dir} loading...")
        df=df.unionByName(spark.read.parquet(f"s3a://t1-tu-data/{log_dir}"), allowMissingColumns=True)
    print("Loading Completedi ::::")
    df.show()
    df.printSchema()

    df.createOrReplaceTempView("logs")
    ###########################
    import datetime as dt

    start_day=dt.datetime.strptime(f"2024-12-10", "%Y-%m-%d")

    today=dt.datetime.now().strftime("%Y-%m-%d")


    while start_day.strftime("%Y-%m-%d")<=today:
        s=start_day.strftime("%Y-%m-%d")

        start_day=dt.datetime.strptime( f"{start_day.year}-{start_day.month}-{start_day.day+1}", "%Y-%m-%d" )
        e=start_day.strftime("%Y-%m-%d")

        print(f"{s}~{e}")

        spark.sql(f"SELECT * FROM logs WHERE CAST(timestamp as timestamp) BETWEEN '{s}' AND '{e}'").show()
    ###########################
except Exception as e:
    print(f"Error reading S3 file: {e}")
finally:
    spark.stop()

