from io import BytesIO
from pyspark.sql import SparkSession
from minio import Minio

MAX_MEMORY="16g"

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
    header=False,           # 헤더가 없는 파일임
    inferSchema=True,       # 데이터 타입 자동 추론
    sep=","                # 쉼표로 데이터 구분
)
print("CSV 데이터 로드 완료")


minio_client = Minio(
    "localhost:9001",  # MinIO 서버 주소
    "NvqZkPJZsKTiPVFQczZo",  # MinIO 접근 키
    "2N52qmlnEJ7zaj8pC8sGlhM1f2ZnKcfowlz1dvOZ",  # MinIO 비밀 키
    secure=False  # HTTPS가 아닌 HTTP로 연결할 경우 False로 설정
)

try:
    minio_df = minio_df.toDF("STATION", "DATE", "ELEMENT", "VALUE", "MFLAG", "QFLAG", "SFLAG", "ETC")

    # 출력: 데이터 프레임 스키마 확인
    print("데이터 컬럼 정보:")
    minio_df.printSchema()

    # 데이터 예시 출력
    print("데이터 예시:")
    minio_df.show(10, truncate=False)

    # MinIO에 파일 업로드
    minio_client.put_object(
        'vm-workplace',  # MinIO 버킷 이름
        'test_data/',  # MinIO에서의 저장 경로
        BytesIO(minio_df),  # 파일 데이터
        len(minio_df)  # 파일 크기
    )

        # 저장 확인
    print(f"데이터가 {output_path} 경로에 Parquet 형식으로 저장되었습니다.")

except Exception as e:
    print(f"Error while streaming data from S3 to MinIO: {e}")