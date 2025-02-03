from pyspark.sql import SparkSession
import os
import boto3
import datetime as dt


# Spark 세션 생성
spark = SparkSession.builder.appName("Read S3 Data").getOrCreate()

try:
    print("Starting to read S3 file...")

    df=spark.createDataFrame([{}])
    ########################################################################################
    ################ 1. logs 디렉토리 안에서 로그 가져오기, 12/13부터 #########################
    s3 = boto3.client('s3',
            aws_access_key_id= os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name="ap-northeast-2"
        )

    obj = s3.list_objects( Bucket="t1-tu-data",Prefix="logs")

    for o in obj["Contents"]:
        if o["Key"].split("/")[-1]>"2024-12-13" :
            print(o["Key"])
            df=df.unionByName(spark.read.parquet(f"s3a://t1-tu-data/{o['Key']}"), allowMissingColumns=True)
        else:
            pass


    ########################################################################################
    ####### 2. logs 디렉토리 제외 모든 logs 디렉토리에서 로그 가져오기, 12/13부터 ##############
    # log_dir_list=["Auth_log/","Kakao_log/","Kakao/","KakaoLogin_log/","KakaoLogout_log/","Login_log/","Logout_log/","search_log/","Signup_log/","view_detail_log/"]
    #
    # for log_dir in log_dir_list:
    #     print(f"s3a://t1-tu-data/{log_dir} loading...")
    #     df=df.unionByName(spark.read.parquet(f"s3a://t1-tu-data/{log_dir}"), allowMissingColumns=True)
    print("::::: Loading Completed :::::")

    ###########################################################################################
    today=dt.datetime.now().strftime("%Y-%m-%d")

    ########################################################################################
    ####### 3. 생성된 DataFrame 다시 s3에 저장하기 ##############
    #df.toPandas().to_csv(f"/home/tmp/log_{today}.csv")
    import pyarrow as pa
    import pyarrow.parquet as pq
    from io import BytesIO

    dfp=df.toPandas()
    table = pa.Table.from_pandas(dfp)

    # 메모리 버퍼에 Parquet 파일을 저장
    buffer = BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)  # 버퍼의 처음으로 이동

    # S3에 Parquet 파일 업로드
    #date = time.strftime("%Y-%m-%d")  # timestamp로 파일명 생성
    filePath=f'visualize/log_{today}.parquet'

    s3.put_object(
        Bucket='t1-tu-data',
        Key=filePath,
        Body=buffer
    )

    print(f'로그가 S3에 업로드되었습니다: {filePath}')
except Exception as e:
    print(f"Error reading S3 file: {e}")
finally:
    spark.stop()