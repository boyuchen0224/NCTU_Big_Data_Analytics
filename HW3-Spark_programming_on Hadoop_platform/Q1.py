# coding=utf-8
from pyspark.context import SparkContext

sc = SparkContext("local")
text = sc.textFile("./Youvegottofindwhatyoulove.txt")

"""
Q1
1. Show the top 30 most frequent occurring words and their average
occurrences in a sentence.
2. According to the result, what are the characteristics of these words?
"""
counts = (
    text.map(lambda x: x.lower())
    .flatMap(lambda x: x.split(" "))
    .map(lambda x: (x, 1))
    .reduceByKey(lambda x, y: x + y)
    .map(lambda x: (x[1], x[0]))
    .sortByKey(False)
)

print("Top 30 :\n", counts.take(30))
print("Average : \n", counts.keys().mean())
