import json
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
conf = SparkConf().setMaster("local").setAppName("Load_Json_PYspark")
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
p=sqlContext.read.json("C:\\Users\\himan\\Downloads\\Spark\\spark-2.2.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json")
p.printSchema()
p.registerTempTable('people')
sqlContext.sql("Select distinct name from people").show()