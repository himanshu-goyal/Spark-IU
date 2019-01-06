from pyspark import SparkContext, SparkConf
import re
import sys

def normalizeWords(text):
    words = re.sub(r'[^A-Za-z0-9 ]','',text).lower().strip().split()
    return [w for w in words if len(w) > 3 and w !='']

if __name__ == '__main__':
    filename = "C:/Users/himan/Downloads/Ramp-Course/spark/assignment_2_datafile.txt"
    input = sc.textFile(filename)
    words = input.flatMap(normalizeWords)
    word_counts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: (x + y))
    results = word_counts.collect()
    print(results)