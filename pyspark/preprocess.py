# spark 찾아오기
import findspark
findspark.init()

# 패키지를 가져오고
from pyspark import SparkConf, SparkContext
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession


MAX_MEMORY="5g"
spark = SparkSession.builder.appName("taxi-fare-prediciton")\
                .config("spark.executor.memory", MAX_MEMORY)\
                .config("spark.driver.memory", MAX_MEMORY)\
                .getOrCreate()


trip_files = "/opt/airflow/pyspark_data/source/fhvhv_tripdata_2021-01.parquet"
trips_df = spark.read.parquet(f"file:///{trip_files}", inferSchema=True, header=True)


trips_df.show(5)
trips_df.createOrReplaceTempView("trips")


query = """
SELECT 
    PULocationID as pickup_location_id,
    DOLocationID as dropoff_location_id,
    trip_miles,
    HOUR(Pickup_datetime) as pickup_time,
    DATE_FORMAT(TO_DATE(Pickup_datetime), 'EEEE') AS day_of_week,
    driver_pay
FROM
    trips
WHERE
    driver_pay  < 5000
    AND driver_pay  > 0
    AND trip_miles > 0
    AND trip_miles < 500
    AND TO_DATE(Pickup_datetime) >= '2021-01-01'
    AND TO_DATE(Pickup_datetime) < '2021-08-01'
"""
data_df = spark.sql(query)


data_dir = "/opt/airflow/pyspark_data/result"
data_df.write.format("parquet").mode('overwrite').save(f"{data_dir}/")