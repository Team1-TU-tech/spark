from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import logging
import time

# Line Notify API 토큰 설정
LINE_NOTIFY_TOKEN = "YOUR_LINE_NOTIFY_TOKEN"
LINE_NOTIFY_API = "https://notify-api.line.me/api/notify"

# Line 알림 전송 함수
def send_line_notification(message):
    headers = {
        "Authorization": f"Bearer {LINE_NOTIFY_TOKEN}"
    }
    data = {"message": message}
    response = requests.post(LINE_NOTIFY_API, headers=headers, data=data)
    if response.status_code != 200:
        logging.error("Line Notify failed: %s", response.text)

# S3 작업 함수
def task_s3(**kwargs):
    try:
        logging.info("S3 작업 시작")
        # S3 관련 로직 실행
        time.sleep(5)  # 예제용으로 대기 시간 추가
        logging.info("S3 작업 완료")
        send_line_notification("S3 작업이 성공적으로 완료되었습니다.")
    except Exception as e:
        logging.error("S3 작업 실패: %s", str(e))
        send_line_notification(f"S3 작업 실패: {str(e)}")
        raise

# Spark 작업 함수
def task_spark(**kwargs):
    try:
        logging.info("Spark 작업 시작")
        # Spark 관련 로직 실행
        time.sleep(5)  # 예제용으로 대기 시간 추가
        logging.info("Spark 작업 완료")
        send_line_notification("Spark 작업이 성공적으로 완료되었습니다.")
    except Exception as e:
        logging.error("Spark 작업 실패: %s", str(e))
        send_line_notification(f"Spark 작업 실패: {str(e)}")
        raise

# Tableau 작업 함수
def task_tableau(**kwargs):
    try:
        logging.info("Tableau 작업 시작")
        # Tableau 관련 로직 실행
        time.sleep(5)  # 예제용으로 대기 시간 추가
        logging.info("Tableau 작업 완료")
        send_line_notification("Tableau 작업이 성공적으로 완료되었습니다.")
    except Exception as e:
        logging.error("Tableau 작업 실패: %s", str(e))
        send_line_notification(f"Tableau 작업 실패: {str(e)}")
        raise

# DAG 정의
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "s3_spark_tableau_pipeline",
    default_args=default_args,
    description="S3, Spark, Tableau 작업 파이프라인",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)

# DAG에 작업 추가
task_1 = PythonOperator(
    task_id="s3_task",
    python_callable=task_s3,
    provide_context=True,
    dag=dag,
)

task_2 = PythonOperator(
    task_id="spark_task",
    python_callable=task_spark,
    provide_context=True,
    dag=dag,
)

task_3 = PythonOperator(
    task_id="tableau_task",
    python_callable=task_tableau,
    provide_context=True,
    dag=dag,
)

# 작업 순서 지정
task_1 >> task_2 >> task_3

