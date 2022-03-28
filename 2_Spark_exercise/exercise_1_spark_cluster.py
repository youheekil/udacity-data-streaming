""" Writing a Hello World pyspark Application """
# Throughout the course you will deploy spark applications on a Spark cluster.
# That means the cluster needs to be running!
# So, for each exercise you will start up the spark master and the worker.
# This requires some coordination between the worker and the master so work can be delegated.
# Once they are both up and running, you will be able to submit your application to the master.

from pyspark.sql import SparkSession

# TO-DO: create a variable with the absolute path to the text file
# /home/workspace/Test.txt
logFile = "/home/workspace/Test.txt" # should be some file on your system
# TO-DO: create a Spark session
spark = SparkSession.builder.appName("HelloSpark").getOrCreate()
# TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel("WARN")

# TO-DO: Define a python function that accepts row as in an input, and increments the total number of times the letter 'a' has been encountered (including in this row)

# TO-DO: Define a python function that accepts row as in an input, and increments the total number of times the letter 'b' has been encountered (including in this row)


# TO-DO: using the Spark session variable, call the appropriate
# function referencing the text file path to read the text file

# TO-DO: call the appropriate function to filter the data containing
# the letter 'a', and then count the rows that were found

# TO-DO: call the appropriate function to filter the data containing
# the letter 'b', and then count the rows that were found


# TO-DO: print the count for letter 'a' and letter 'b'

# TO-DO: stop the spark application
