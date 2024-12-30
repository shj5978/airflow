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

print(data_df.count())  # 전체 row 수 확인
#################################################################################################

########################    Postgres DB 에 저장   ################################################

### sqlalchemy 이용 방법
# PostgreSQL 연결 설정
# engine = create_engine('postgresql://postgres:59aufcl78!@172.23.208.1:5432/postgres')

# # Spark DataFrame을 Pandas DataFrame으로 변환
# pandas_df = data_df.toPandas()

# # DataFrame을 PostgreSQL 테이블에 저장
# pandas_df.to_sql('tb_noaa_weather_info', engine, if_exists='append', index=False)
# print("PostgreSQL 테이블에 데이터 저장 완료!")


### JDBC 를 이용한 방법 ( Pandas 를 거치면 느려지기 때문에 직접 JDBC 를 호출해서 넣는것이 효율적 --> 드라이버 설치필요 )
data_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://172.23.208.1:5432/postgres") \
    .option("dbtable", "tb_noaa_weather_info") \
    .option("user", "postgres") \
    .option("password", "59aufcl78!") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()
print("PostgreSQL 테이블에 데이터 저장 완료!")

#################################################################################################
