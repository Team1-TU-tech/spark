FROM apache/spark:3.5.3

USER root

# 필요한 Hadoop 및 AWS 관련 라이브러리 추가
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.2/hadoop-aws-3.2.2.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar /opt/spark/jars/

# Python 파일 복사할 디렉토리 생성
RUN mkdir -p /opt/spark-apps

# 로컬 Python 파일을 컨테이너로 복사
COPY src/spark/read_s3.py /opt/spark-apps/read_s3.py

COPY requirements.txt /opt/spark-apps/ 
RUN pip install --no-cache-dir --upgrade -r /opt/spark-apps/requirements.txt

# spark-submit 실행 명령 추가
#CMD ["spark-submit", "/opt/spark-apps/read_s3.py"]
#CMD ["spark-submit", "/opt/spark-apps/read_s3.py", "&&", "tail", "-f", "/dev/null"]

# Spark-submit 후 sleep 명령 추가
CMD ["spark-submit", "/opt/spark-apps/read_s3.py", "&&", "sleep", "infinity"]

