from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

#dataset file consist of seven column, with the first four looking like:
# stationid,timestamp,entry type, temperature in C
#ITE00100554	18000101	TMAX	-75
#ITE00100554	18000101	TMIN	-148

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("file:///Users/David.Raj/Desktop/pyspark_practice/dataset/1800.csv")
parsedLines = lines.map(parseLine)

#filters rows where "TMIN" is in the second column
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])

#creates a dataset with just stationid and temperature column
stationTemps = minTemps.map(lambda x: (x[0], x[2]))

#by unique keys, find the minimum temperature for each station
#it works by taking the first tem observation x, compared with the next observation y, and choses the lowest
#it repeats the step until all observation is considered (both x and y are temperatures, not the key-station)
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
