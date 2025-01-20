from pyspark.sql import SparkSession

# Spark 세션 생성
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
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
print("Spark 세션 생성 완료")

# 원격 S3 파일 경로
s3_file_path = "s3a://noaa-ghcn-pds/csv.gz/by_station/ASN0000509*.csv.gz"

# MinIO S3 설정
minio_endpoint = "localhost:9000"  # MinIO 서버 주소
minio_access_key = "NvqZkPJZsKTiPVFQczZo"
minio_secret_key = "2N52qmlnEJ7zaj8pC8sGlhM1f2ZnKcfowlz1dvOZ"
minio_bucket = "s3a://vm-workplace/uploaded_data/"  # MinIO 버킷 경로

# MinIO 연결 설정 추가
spark.conf.set("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
spark.conf.set("spark.hadoop.fs.s3a.access.key", minio_access_key)
spark.conf.set("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

try:
    # 원격 S3에서 데이터 읽기
    print("S3에서 데이터 읽기 시작")
    df = spark.read.csv(s3_file_path, header=False, inferSchema=True)
    print("S3에서 데이터 읽기 완료")

    # MinIO에 데이터 저장
    print("MinIO에 데이터 저장 시작")
    df.write.csv(minio_bucket, mode="overwrite", header=True)
    print(f"데이터가 MinIO S3에 성공적으로 업로드되었습니다: {minio_bucket}")

except Exception as e:
    print(f"Error while streaming data from S3 to MinIO: {e}")

finally:
    # Spark 세션 종료
    spark.stop()