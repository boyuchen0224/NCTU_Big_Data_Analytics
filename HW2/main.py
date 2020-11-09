from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, when, count

"""
Find the maximal delays (you should consider both ArrDelay and
DepDelay) for each month of 2008
"""
sc = SparkContext("local")
spark = SparkSession(sc)

# read 2008.csv
df_2008 = spark.read.csv("./data/2008.csv")

# filter month : 8
df_2008_8 = df_2008.filter(df_2008._c1 == "8")
# ArrDelay : _c14, DepDelay : _c15
target = ["_c14", "_c15"]
df_2008_total_delay = df_2008_8.withColumn(
    "TotalDelay", sum(df_2008_8[col] for col in target)
)
max_delay = df_2008_total_delay.agg({"TotalDelay": "max"}).collect()[0][
    "max(TotalDelay)"
]
max_row = df_2008_total_delay.filter(df_2008_total_delay.TotalDelay == max_delay)
max_row.select("_c0", "_c1", "_c2", "_c9", "_c16", "_c17", "TotalDelay").show()

"""
How many flights were delayed caused by weather between 2000 ~
2005? Please show the counting for each year.
"""
sc = SparkContext("local")
spark = SparkSession(sc)

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
sc = SparkContext("local")
spark = SparkSession(sc)

# read 2007.csv
df_2007 = spark.read.csv("./data/2007.csv")

# count departure delay of origin IATA airport code
df_2007_dep_delay = df_2007.groupBy("_c16").agg(count(when(col("_c15") > 0, True)))
max_delay_count = df_2007_dep_delay.agg(
    {"count(CASE WHEN (_c15 > 0) THEN true END)": "max"}
).collect()[0]["max(count(CASE WHEN (_c15 > 0) THEN true END))"]
max_row = df_2007_dep_delay.filter(
    df_2007_dep_delay["count(CASE WHEN (_c15 > 0) THEN true END)"] == max_delay_count
)
max_row.show()

# count arrive delay of destination IATA airport code
df_2007_arr_delay = df_2007.groupBy("_c17").agg(count(when(col("_c14") > 0, True)))
max_delay_count = df_2007_arr_delay.agg(
    {"count(CASE WHEN (_c14 > 0) THEN true END)": "max"}
).collect()[0]["max(count(CASE WHEN (_c14 > 0) THEN true END))"]
max_row = df_2007_arr_delay.filter(
    df_2007_arr_delay["count(CASE WHEN (_c14 > 0) THEN true END)"] == max_delay_count
)
max_row.show()

