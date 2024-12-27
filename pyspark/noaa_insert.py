# spark 찾아오기
import findspark
findspark.init()

print("Spark 환경 초기화 완료.")

from pyspark.sql import SparkSession
from sqlalchemy import create_engine, text
import pandas as pd


MAX_MEMORY="16g"

print("SparkSession 생성 중...")
spark = SparkSession.builder.appName("NOAA Weather Data")\
                .config("spark.executor.memory", MAX_MEMORY)\
                .config("spark.driver.memory", MAX_MEMORY)\
                .getOrCreate()
print("SparkSession 생성 완료.")

########################    Parquet 데이터 읽기   ################################################
data_dir = "/opt/airflow/pyspark_data/noaa_source"
print(f"데이터 디렉토리 설정: {data_dir}")

print("Parquet 파일 읽기 시작...")
data_df = spark.read.parquet(f"{data_dir}")
print(f"Parquet 파일 읽기 완료. 데이터 스키마:\n{data_df.printSchema()}")

# 데이터 createOrReplaceTempView()
print("TempView 생성 중...")
data_df.createOrReplaceTempView("weather")
print("TempView 생성 완료.")
#################################################################################################

########################    Postgres DB 에 저장   ################################################
data_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/WPTest") \
    .option("dbtable", "tb_noaa_weather_info") \
    .option("user", "postgres") \
    .option("password", "59aufcl78!") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()
print("PostgreSQL 테이블에 데이터 저장 완료!")
#################################################################################################
