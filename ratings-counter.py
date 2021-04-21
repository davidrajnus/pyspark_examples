from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///Users/David.Raj/Desktop/pyspark_practice/dataset/ml-100k/ratings.csv")

dataheader = lines.first() 
final = lines.subtract(tagsheader)

ratings = final.map(lambda x: x.split(",")[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
