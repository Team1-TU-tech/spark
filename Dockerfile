FROM apache/spark:latest

USER root

# 필요한 Hadoop 및 AWS 관련 라이브러리 추가
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar /opt/spark/jars/

# Spark 설정 파일 수정 (S3 관련 설정 추가)
COPY ./conf/ /opt/spark/conf/

# 로컬 Python 파일을 컨테이너로 복사
COPY src/spark/ /opt/spark-apps/

COPY requirements.txt /opt/spark-apps/
RUN pip install --no-cache-dir --upgrade -r /opt/spark-apps/requirements.txt

