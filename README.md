# S3 로그 집계 with Apache Spark
이 프로젝트는 Amazon S3에서 로그 파일을 읽고, Apache Spark를 사용하여 처리한 후 결과를 S3에 Parquet 파일로 다시 저장합니다. 처리된 데이터는 집계된 후, 태블로(Tableau) 를 사용하여 시각화할 예정입니다. 이 과정에서는 날짜를 기준으로 로그를 필터링하고, Spark의 분산 처리 기능을 활용하여 데이터를 효율적으로 처리합니다.
<br></br>
## 개요
- Spark 세션 생성: 분산 데이터 처리를 위한 Spark 세션을 초기화합니다.
- S3에서 로그 가져오기: boto3 라이브러리를 사용해 AWS S3와 상호작용하며, logs 디렉토리 내의 객체들을 가져옵니다. 로그는 2024-12-13 이후의 데이터로 필터링됩니다.
- 데이터 읽기 및 처리: S3에서 읽어온 로그를 Spark의 read.parquet 메서드를 사용해 병렬로 읽고, 이를 하나의 DataFrame으로 병합합니다.
- Parquet 형식으로 변환: 처리된 DataFrame을 Pandas DataFrame으로 변환한 후, pyarrow를 사용해 Parquet 테이블로 변환합니다.
- 결과를 S3에 저장: 변환된 Parquet 파일을 S3에 업로드하며, 파일명에는 현재 날짜가 포함됩니다.
- 태블로 시각화 준비: S3에 저장된 Parquet 파일은 태블로에서 직접 불러와 시각화합니다.
<br></br>
## 기술스택
<img src="https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=Python&logoColor=F5F7F8"/>  <img src="https://img.shields.io/badge/Apache Spark-E25A1C?style=flat&logo=apachespark&logoColor=ffffff"/>    <img src="https://img.shields.io/badge/Amazon%20S3-569A31?style=flat&logo=Amazon%20S3&logoColor=ffffff"/> 
<br></br>
## 개발기간
`2024.12.18 ~ 2024.12.22(5일)` 
현재 수정 진행중...
<br></br>
## 필수 사항
- Python 3.11
- Apache Spark
- PyArrow
- Boto3
필요한 라이브러리를 설치하려면 다음 명령어를 사용할 수 있습니다:
```bash
pip install pyspark pyarrow boto3
```
<br></br>
## Contributors
`Mingk42`, `hamsunwoo`
<br></br>
## License
이 애플리케이션은 TU-tech 라이선스에 따라 라이선스가 부과됩니다.
<br></br>
## 문의
질문이나 제안사항이 있으면 언제든지 연락주세요:
<br></br>
- 이메일: TU-tech@tu-tech.com
- Github: `Mingk42`, `hahahellooo`, `hamsunwoo`, `oddsummer56`
