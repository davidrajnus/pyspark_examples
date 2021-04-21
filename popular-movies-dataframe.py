from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

#a way to broadcast a name-key table to all executors (think dimension table to each cluster 
#to work with the distributed transaction table
def loadMovieNames():
    movieNames = {}
    with open("dataset/ml-100k/movies.csv", encoding="utf8", newline='') as f:
        next(f)
        lines = csv.reader(f, delimiter=',')
        for line in lines:
            movieNames[int(line[0])] = line[1]
    return movieNames

# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("PopularMovies").getOrCreate()

# Load up our movie ID -> name dictionary. This is a name key RDD
nameDict = loadMovieNames()

#Read data from csv. The order of the functions matters
#Options specifies how you want to read the file
#csv is whereyou specify the path of the file
#select specifies what columns you want, you can chain ("column1","column2")
movies = spark.read.options(header='True', inferSchema='True', delimiter=',') \
                    .csv("file:///Users/David.Raj/Desktop/pyspark_practice/dataset/ml-100k/ratings.csv") \
                    .select("movieId")

#movies is a dataframe


# Some SQL-style magic to sort all movies by popularity in one line!
topMovieIDs = movies.groupBy("movieId").count().orderBy("count", ascending=False).cache()

# Show the results at this point:

#|movieID|count|
#+-------+-----+
#|     50|  584|
#|    258|  509|
#|    100|  508|

topMovieIDs.show()

# Grab the top 10
top10 = topMovieIDs.take(10)

# Print the results
print("\n")
for result in top10:
    # Each row has movieID, count as above.
    print("%s: %d" % (nameDict[result[0]], result[1]))

# Stop the session
spark.stop()


#+-------+-----+
# |movieId|count|
# +-------+-----+
# |    356|  329|
# |    318|  317|
# |    296|  307|
# |    593|  279|
# |   2571|  278|
# |    260|  251|
# |    480|  238|
# |    110|  237|
# |    589|  224|
# |    527|  220|
# |   2959|  218|
# |      1|  215|
# |   1196|  211|
# |     50|  204|
# |   2858|  204|
# |     47|  203|
# |    780|  202|
# |    150|  201|
# |   1198|  200|
# |   4993|  198|
# +-------+-----+
# only showing top 20 rows



# Forrest Gump (1994): 329
# Shawshank Redemption, The (1994): 317
# Pulp Fiction (1994): 307
# Silence of the Lambs, The (1991): 279
# Matrix, The (1999): 278
# Star Wars: Episode IV - A New Hope (1977): 251
# Jurassic Park (1993): 238
# Braveheart (1995): 237
# Terminator 2: Judgment Day (1991): 224
# Schindler's List (1993): 220