# spark 찾아오기
import findspark
findspark.init()

print("Spark 환경 초기화 완료.")

from pyspark.sql import SparkSession
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

MAX_MEMORY="16g"

print("SparkSession 생성 중...")
spark = SparkSession.builder.appName("NOAA Weather Data")\
                .config("spark.executor.memory", MAX_MEMORY)\
                .config("spark.driver.memory", MAX_MEMORY)\
                .getOrCreate()
print("SparkSession 생성 완료.")

data_dir = "/opt/airflow/pyspark_data/noaa_source"
print(f"데이터 디렉토리 설정: {data_dir}")

print("Parquet 파일 읽기 시작...")
data_df = spark.read.parquet(f"{data_dir}")
print(f"Parquet 파일 읽기 완료. 데이터 스키마:\n{data_df.printSchema()}")

# 데이터 createOrReplaceTempView()
print("TempView 생성 중...")
data_df.createOrReplaceTempView("weather")
print("TempView 생성 완료.")

# 1. date 와 value 간의 산포도 그래프
print("SQL 쿼리 실행 중...")
weather_info = spark.sql("SELECT date, value FROM weather WHERE value > 0").toPandas()
print(f"SQL 쿼리 완료. 결과 데이터프레임 크기: {weather_info.shape}")

print("산포도 그래프 생성 중...")
fig, ax = plt.subplots(figsize=(16,6))
plt.title('weather_info', fontsize = 30)
sns.scatterplot(
    x = 'date',
    y = 'value',
    data = weather_info
)

output_path = "./pyspark_data/noaa_result/weather_info.png"
plt.savefig(output_path)
print(f"그래프 저장 완료: {output_path}")
