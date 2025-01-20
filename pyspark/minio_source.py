from io import BytesIO
from pyspark.sql import SparkSession
from minio import Minio

MAX_MEMORY = "16g"

print("Spark 세션 생성 시작")
spark = SparkSession.builder \
    .appName("S3-to-MinIO") \
    .config("spark.executor.memory", MAX_MEMORY) \
    .config("spark.driver.memory", MAX_MEMORY) \
    .config("spark.hadoop.fs.s3a.threads.max", "50") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "50") \
    .config("spark.hadoop.fs.s3a.access.key", "") \
    .config("spark.hadoop.fs.s3a.secret.key", "") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
print("Spark 세션 생성 완료")

s3_file_path = "s3a://noaa-ghcn-pds/csv.gz/by_station/ASN0000509*.csv.gz"

print("CSV 데이터 로드 시작")
minio_df = spark.read.csv(
    path=s3_file_path,
    header=False,  # 헤더가 없는 파일임
    inferSchema=True,  # 데이터 타입 자동 추론
    sep=",",  # 쉼표로 데이터 구분
)
print("CSV 데이터 로드 완료")

# MinIO 클라이언트 생성
minio_client = Minio(
    "localhost:9000",  # MinIO 서버 주소
    "NvqZkPJZsKTiPVFQczZo",  # MinIO 접근 키
    "2N52qmlnEJ7zaj8pC8sGlhM1f2ZnKcfowlz1dvOZ",  # MinIO 비밀 키
    secure=False  # HTTPS가 아닌 HTTP로 연결할 경우 False로 설정
)
print("MinIO 클라이언트 생성 완료")

try:
    # 컬럼 이름 추가
    minio_df = minio_df.toDF("STATION", "DATE", "ELEMENT", "VALUE", "MFLAG", "QFLAG", "SFLAG", "ETC")

    # DataFrame을 메모리에 CSV로 저장
    csv_buffer = BytesIO()
    pandas_df = minio_df.toPandas()  # Spark DataFrame을 Pandas DataFrame으로 변환
    pandas_df.to_csv(csv_buffer, index=False)  # CSV로 변환하여 BytesIO에 저장
    csv_buffer.seek(0)  # 파일 포인터를 처음으로 이동
    print("DataFrame 을 메모리에 csv 로 저장 완료")

    # MinIO에 파일 업로드
    minio_client.put_object(
        bucket_name="vm-workplace",  # MinIO 버킷 이름
        object_name="test_data/data.csv",  # MinIO에 저장될 경로 및 파일명
        data=csv_buffer,  # 바이너리 데이터
        length=len(csv_buffer.getvalue()),  # 파일 크기
        content_type="application/csv"  # 파일 타입 지정
    )

    print("데이터가 MinIO에 성공적으로 업로드되었습니다.")

except Exception as e:
    print(f"Error while streaming data from S3 to MinIO: {e}")