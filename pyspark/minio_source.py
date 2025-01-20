from pyspark.sql import SparkSession
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("S3-to-MinIO")

MAX_MEMORY = "16g"
logger.info("Spark 세션 생성 시작")
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
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
logger.info("Spark 세션 생성 완료")

s3_file_path = "s3a://noaa-ghcn-pds/csv.gz/by_station/ASN0000509*.csv.gz"

minio_endpoint = "localhost:9000"
minio_access_key = "NvqZkPJZsKTiPVFQczZo"
minio_secret_key = "2N52qmlnEJ7zaj8pC8sGlhM1f2ZnKcfowlz1dvOZ"
minio_bucket = "s3a://vm-workplace/uploaded_data"

# MinIO 연결 설정
spark.conf.set("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
spark.conf.set("spark.hadoop.fs.s3a.access.key", minio_access_key)
spark.conf.set("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

try:
    logger.info("S3에서 데이터 읽기 시작")
    df = spark.read.csv(s3_file_path, header=False, inferSchema=True, compression="gzip")
    logger.info("S3에서 데이터 읽기 완료")

    logger.info("MinIO에 데이터 저장 시작")
    df.write.csv(minio_bucket, mode="overwrite", header=True)
    logger.info(f"데이터가 MinIO에 성공적으로 업로드되었습니다: {minio_bucket}")

except Exception as e:
    logger.error(f"Error while streaming data from S3 to MinIO: {e}")

finally:
    spark.stop()