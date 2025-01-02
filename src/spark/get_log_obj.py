import boto3
import os

s3 = boto3.client('s3', 
                  aws_access_key_id= os.getenv("AWS_ACCESS_KEY_ID"), 
                  aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                  region_name="ap-northeast-2" 
                )

o=s3.list_objects( Bucket="t1-tu-data",Prefix="logs")

for i in o["Contents"]:
	if i["Key"].split("/")[-1]>"2024-12-13" :
		print(i["Key"])
