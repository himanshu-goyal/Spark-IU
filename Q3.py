import csv
import pyspark
from pyspark import SparkConf, SparkContext
from operator import add
conf = SparkConf().setMaster("local").setAppName("Load_Csv")
sc = SparkContext.getOrCreate()

with open("C:\\Users\\himan\\Downloads\\Spark\\spark-2.2.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.txt") as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    print(type(readCSV))

    for row in readCSV:
        print(row)
print(sc.textFile("C:\\Users\\himan\\Downloads\\Spark\\spark-2.2.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.txt").map(lambda line: line.split(",")).collect())