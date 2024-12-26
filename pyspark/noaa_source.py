from pyspark.sql import SparkSession

MAX_MEMORY="16g"

# Spark 세션 생성
print("Spark 세션 생성 시작")
spark = SparkSession.builder \
    .appName("NOAA Weather Data") \
    .config("spark.executor.memory", MAX_MEMORY)\
    .config("spark.driver.memory", MAX_MEMORY)\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "5000ms") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "10") \
    .config("spark.hadoop.fs.s3a.threads.max", "20") \
    .config("spark.hadoop.fs.s3a.threads.core", "10") \
    .getOrCreate()
    #.config("spark.hadoop.fs.s3a.access.key", "<YOUR_ACCESS_KEY>") \  # 공개데이터의 경우 access key 와 secret key 필요 없음
    #.config("spark.hadoop.fs.s3a.secret.key", "<YOUR_SECRET_KEY>") \
print("Spark 세션 생성 완료")
    
# S3 버킷에서 NOAA 데이터 경로
print("S3 버킷에서 NOAA 데이터 경로 설정")
s3_path = "s3a://noaa-ghcn-pds/csv.gz/"

# CSV 데이터 로드
print("CSV 데이터 로드 시작")
weather_df = spark.read.csv(s3_path, header=True, inferSchema=True)
print("CSV 데이터 로드 완료")

# 출력: 데이터 프레임 스키마 확인 (컬럼 정보)
print("데이터 컬럼 정보:")
weather_df.printSchema()

# 2024년 1월 1일 데이터 필터링
print("데이터 필터링 시작")
filtered_df = weather_df.filter(weather_df.DATE == "2024-01-01")
print("데이터 필터링 완료")

# 데이터 저장 (Parquet 형식)
output_path = "/opt/airflow/pyspark_data/noaa_source/noaa_weather_2024-01-01.parquet"
filtered_df.write.parquet(output_path, mode="overwrite")

# 저장 확인
print(f"데이터가 {output_path} 경로에 Parquet 형식으로 저장되었습니다.")