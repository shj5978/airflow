from pyspark.sql import SparkSession

MAX_MEMORY="16g"

# Spark 세션 생성
print("Spark 세션 생성 시작(15)")
spark = SparkSession.builder \
    .appName("NOAA Weather Data") \
    .config("spark.executor.memory", MAX_MEMORY)\
    .config("spark.driver.memory", MAX_MEMORY)\
    .config("spark.hadoop.fs.s3a.access.key", "") \
    .config("spark.hadoop.fs.s3a.secret.key", "") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
print("Spark 세션 생성 완료")

# NOAA 데이터 파일 경로
print("S3 파일 경로 설정")
s3_file_path = "s3a://noaa-ghcn-pds/csv.gz/by_station/ASN00005095.csv.gz"

# CSV 데이터 로드
print("CSV 데이터 로드 시작")
weather_df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_file_path)
print("CSV 데이터 로드 완료")

# 출력: 데이터 프레임 스키마 확인
print("데이터 컬럼 정보:")
weather_df.printSchema()
    

# 2024년 1월 1일 데이터 필터링
#print("데이터 필터링 시작")
#filtered_df = weather_df.filter(weather_df.DATE == "2024-01-01")
#print("데이터 필터링 완료")

# 데이터 저장 (Parquet 형식)
output_path = "/opt/airflow/pyspark_data/noaa_source/noaa_weather.parquet"
weather_df.write.parquet(output_path, mode="overwrite")

# 저장 확인
print(f"데이터가 {output_path} 경로에 Parquet 형식으로 저장되었습니다.")