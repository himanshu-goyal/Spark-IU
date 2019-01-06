import json
from pyspark import SparkContext, SparkConf
sc = SparkContext.getOrCreate()
#jsonRDD = sc.wholeTextFiles("C:\\Users\\himan\\Downloads\\Spark\\spark-2.2.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json").map(lambda x: x[1])
jsonRDD = sc.wholeTextFiles("people.json").map(lambda x: x[1])
print(jsonRDD.collect())