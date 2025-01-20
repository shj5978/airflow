from pyspark.sql import SparkSession
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("S3-to-MinIO")

MAX_MEMORY = "16g"
logger.info("AWS Spark 세션 생성 시작")
aws_spark = SparkSession.builder \
    .appName("AWS S3") \
    .config("spark.executor.memory", MAX_MEMORY) \
    .config("spark.driver.memory", MAX_MEMORY) \
    .config("spark.hadoop.fs.s3a.threads.max", "50") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "50") \
    .config("spark.hadoop.fs.s3a.access.key", "") \
    .config("spark.hadoop.fs.s3a.secret.key", "") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
logger.info("AWS Spark 세션 생성 완료")

# MinIO Spark 세션 생성
logger.info("MinIO Spark 세션 생성 시작")
min_spark = SparkSession.builder \
    .appName("MinIO S3") \
    .config("spark.hadoop.fs.s3a.endpoint", "localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "oMGrfbg5iz0zgt1iMT5w") \
    .config("spark.hadoop.fs.s3a.secret.key", "GQBVemsvQVSnypFw6qQaWj5eCBPjapVMux972Fpg") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()
logger.info("MinIO Spark 세션 생성 완료")

s3_file_path = "s3a://noaa-ghcn-pds/csv.gz/by_station/ASN0000509*.csv.gz"
minio_bucket = "s3a://vm-workplace/uploaded_data"

try:
    logger.info("AWS S3에서 데이터 읽기 시작")
    # aws_spark를 사용해 AWS S3에서 데이터 읽기
    df = aws_spark.read.csv(s3_file_path, header=False, inferSchema=True)  # compression 제거
    logger.info("AWS S3에서 데이터 읽기 완료")

    # MinIO에 데이터를 저장하려면 min_spark 세션을 사용해야 합니다
    logger.info("MinIO에 데이터 저장 시작")
    # min_spark.write.csv로 MinIO에 데이터 저장
    df.write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(minio_bucket)

    logger.info(f"데이터가 MinIO에 성공적으로 업로드되었습니다: {minio_bucket}")

except Exception as e:
    logger.error(f"Error while streaming data from S3 to MinIO: {e}")

finally:
    aws_spark.stop()
    min_spark.stop()