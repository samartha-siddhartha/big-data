import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

#function to load movies database and clean the data

def loadMovieNames():
    movieNames = {} #empty dictionary created to append looped data
    with open("/home/cloudera/moviedata/itemfile.txt") as f:
        #opens the file and cleans the data 
        for line in f:
        	#iterates over each line and splits the data by | (pipe)
            fields = line.split('|')
            #decodes the the movie names to ascii form and appends to dictionary created 
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    #returns the dictionary created by looping on the lines in the file
    return movieNames

#Function to create pairs of movies takes user and ratings
def makePairs((user, ratings)):
	# create tuples of movie rating 
	# another process in cleaning the data and  creating in the desired format
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    #returts the tuples created
    return ((movie1, movie2), (rating1, rating2))

# function takes in userid anf rating and removes wduplicates 
def filterDuplicates( (userID, ratings) ):
	#this function returns True or False values for the data 
	#weather the data in column has occured before
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2

# function to calculate score on Similarilty 
def computeCosineSimilarity(ratingPairs):
 # logic on calculating similarity
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))
    # returns score + number of pairs taken into consideration    
    return (score, numPairs)

# configuration for spark context
conf = SparkConf().setMaster("local[*]").setAppName("MovieSimilarities")
# updating spark context with the configuartions
sc = SparkContext(conf = conf)
# printing to see the process is working
print "\nLoading movie names..."
#loading movies to namedict from loadMoviesName function
nameDict = loadMovieNames()

data = sc.textFile("file:///home/cloudera/moviedata/datafile2.txt")
#reading file to compare
# Map ratings to key / value pairs: user ID => movie ID, rating
ratings = data.map(lambda l: l.split()).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))

# Emit every movie rated together by the same user.
# Self-join to find every combination.
joinedRatings = ratings.join(ratings)

# At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))

# Filter out duplicate pairs
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

# Now key by (movie1, movie2) pairs.
moviePairs = uniqueJoinedRatings.map(makePairs)

# We now have (movie1, movie2) => (rating1, rating2)
# Now collect all ratings for each movie pair and compute similarity
moviePairRatings = moviePairs.groupByKey()

# We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
# Can now compute similarities.
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

# Save the results if desired
#moviePairSimilarities.sortByKey()
#moviePairSimilarities.saveAsTextFile("movie-sims")

# Extract similarities for the movie we care about that are "good".
if (len(sys.argv) > 1):

    scoreThreshold = 0.10
    coOccurenceThreshold = 2

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter(lambda((pair,sim)): \
        (pair[0] == movieID or pair[1] == movieID) \
        and sim[0] > scoreThreshold and sim[1] > coOccurenceThreshold)

    # Sort by quality score.
    results = filteredResults.map(lambda((pair,sim)): (sim, pair)).sortByKey(ascending = False).take(10)

    print "Top 10 similar movies for " + nameDict[movieID]
    for result in results:
        (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = pair[0]
        if (similarMovieID == movieID):
            similarMovieID = pair[1]
        print nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1])
