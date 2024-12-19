# spark 찾아오기
import findspark
findspark.init()

from pyspark.sql import SparkSession
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


MAX_MEMORY="10g"
spark = SparkSession.builder.appName("taxi-fare-prediciton")\
                .config("spark.executor.memory", MAX_MEMORY)\
                .config("spark.driver.memory", MAX_MEMORY)\
                .getOrCreate()


data_dir = "/opt/airflow/pyspark_data/result"
data_df = spark.read.parquet(f"{data_dir}")


# 데이터 createOrReplaceTempView()
data_df.createOrReplaceTempView("trips")

# 1. trip_miles 와 driver_pay 간의 산포도 그래프
miles_pay = spark.sql("SELECT trip_miles, driver_pay FROM trips").toPandas()

fig, ax = plt.subplots(figsize=(16,6))
plt.title('miles_pay', fontsize = 30)
sns.scatterplot(
    x = 'trip_miles',
    y = 'driver_pay',
    data = miles_pay
)


plt.savefig("/miles_pay.png")