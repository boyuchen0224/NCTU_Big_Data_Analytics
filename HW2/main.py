from pyspark.sql import Window
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, when, count, max

"""
Find the maximal delays (you should consider both ArrDelay and
DepDelay) for each month of 2008
"""
sc = SparkContext("local")
spark = SparkSession(sc)

# read 2008.csv
df_2008 = spark.read.csv("./data/2008.csv")

# ArrDelay : _c14, DepDelay : _c15
target = ["_c14", "_c15"]
df_2008_total_delay = df_2008.withColumn(
    "TotalDelay", sum(df_2008[col] for col in target)
)

# Calculate max delay of each month
w = Window.partitionBy("_c1")
max_row = (
    df_2008_total_delay.withColumn("MaxDelay", max("TotalDelay").over(w))
    .where(col("TotalDelay") == col("MaxDelay"))
    .drop("MaxDelay")
)
max_row.select("_c0", "_c1", "_c2", "_c9", "_c16", "_c17", "TotalDelay").show()

"""
How many flights were delayed caused by weather between 2000 ~
2005? Please show the counting for each year.
"""

# read 2000-2005.csv
df_2000_2005 = spark.read.csv("./data/200[0-5].csv")

# count _c22(CancellationCode) == "B", group by _c0(year)
df_2000_2005.groupBy("_c0").agg(count(when(col("_c22") == "B", True))).show()

# count _c25(WeatherDelay) > 0, group by _c0(year)
df_2000_2005.groupBy("_c0").agg(count(when(col("_c25") > 0, True)),).show()

"""
List Top 5 airports which occur delays most in 2007. (Please show the
IATA airport code)
"""

# read 2007.csv
df_2007 = spark.read.csv("./data/2007.csv")

# count departure delay of origin IATA airport code
df_2007_dep_delay = df_2007.groupBy("_c16").agg(count(when(col("_c15") > 0, True)))
df_2007_dep_delay_top_5 = df_2007_dep_delay.orderBy(
    "count(CASE WHEN (_c15 > 0) THEN true END)", ascending=False
).limit(5)
df_2007_dep_delay_top_5.show()

# count arrive delay of destination IATA airport code
df_2007_arr_delay = df_2007.groupBy("_c17").agg(count(when(col("_c14") > 0, True)))
df_2007_arr_delay_top_5 = df_2007_arr_delay.orderBy(
    "count(CASE WHEN (_c14 > 0) THEN true END)", ascending=False
).limit(5)
df_2007_arr_delay_top_5.show()

