# coding=utf-8
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext("local")

"""
Q2
Implement a program to calculate the average amount in credit card trip 
for different number of passengers which are from one to four passengers 
in 2017.09 NYC Yellow Taxi trip data. In NYC Taxi data, the "Passenger_count" 
is a driver-entered value. Explain also how you deal with the data loss issue.
"""
spark = SparkSession(sc)
df = spark.read.csv("./data/yellow_tripdata_2017-09.csv")

# passenger_count : _c3, payment_type : _c9, total_amount : _c16
df.where((df["_c3"] > 0) & (df["_c9"] == 1)).groupBy("_c3").agg(
    {"_c16": "avg"}
).orderBy("_c3").show()
