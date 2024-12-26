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
spark = SparkSession.builder.appName("taxi-fare-prediciton")\
                .config("spark.executor.memory", MAX_MEMORY)\
                .config("spark.driver.memory", MAX_MEMORY)\
                .getOrCreate()
print("SparkSession 생성 완료.")

data_dir = "/opt/airflow/pyspark_data/result"
print(f"데이터 디렉토리 설정: {data_dir}")

print("Parquet 파일 읽기 시작...")
data_df = spark.read.parquet(f"{data_dir}")
print(f"Parquet 파일 읽기 완료. 데이터 스키마:\n{data_df.printSchema()}")

# 데이터 createOrReplaceTempView()
print("TempView 생성 중...")
data_df.createOrReplaceTempView("trips")
print("TempView 생성 완료.")

# 1. trip_miles 와 driver_pay 간의 산포도 그래프
print("SQL 쿼리 실행 중...")
miles_pay = spark.sql("SELECT trip_miles, driver_pay FROM trips LIMIT 100000").toPandas()
print(f"SQL 쿼리 완료. 결과 데이터프레임 크기: {miles_pay.shape}")

print("산포도 그래프 생성 중...")
fig, ax = plt.subplots(figsize=(16,6))
plt.title('miles_pay', fontsize = 30)
sns.scatterplot(
    x = 'trip_miles',
    y = 'driver_pay',
    data = miles_pay
)

output_path = "./miles_pay.png"
plt.savefig(output_path)
print(f"산포도 그래프 저장 완료: {output_path}")
