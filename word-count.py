import re
from pyspark import SparkConf, SparkContext

#function removes punctiation (W+ breaks it by words without punctuation) and lowercases, makes the words unique
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")

#flatmap differs from map as it does not preserve the count of the keys
#when you apply map on 4 rows, you perform transformation on each row but you still get an output of 4 rows
#flatmap treats each item in a row as individual row. so if you have 2 rows of 4 words, flatmap produces 8 rows
words = input.flatMap(normalizeWords)

#we since there are only keys, we use map to give a value of 1 to each key
#now that everything is (word, 1), we reduce by key and add the ones together (x and y are consecutive rows)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

#because want to sort by the value, we swap the keys(word) with values(sum). then sort by the new key
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()

results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
