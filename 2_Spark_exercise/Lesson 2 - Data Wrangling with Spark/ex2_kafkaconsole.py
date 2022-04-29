from pyspark.sql import SparkSession

# create a Spark Session and name the app
spark = SparkSession.builder.appName("kafka_console")

# set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

# read a stream from the kafka topic 'balance-updates',
# with the bootstrap server localhost:9092, reading from the earliest message

kafkaRawStreamingDF = spark\
    .readstream \
    .format('kafka')\
    .option('kafka.bootstrap.servers', 'localhost:9092')\
    .option('subscribe', 'balance-updates')\
    .option('startingOffsets', 'earliest')\
    .load()

# cast the key and value columns as strings and select them using a select expression function
# this is necessary step for kafka Data Frame to be readable, into a single column value
kafkaStreamingDF = kafkaRawStreamingDF.selectExpr('cast(key as string) key, 
                                                  'cast(value as strig) value')

# write the dataframe to the console and keep running indefinitely
# this takes the stream and 'sinks' it to the console as it is updated one at a time.
kafkaStreamingDF.writeStream.outputMode('append').format('console').start().awaitTermination()
