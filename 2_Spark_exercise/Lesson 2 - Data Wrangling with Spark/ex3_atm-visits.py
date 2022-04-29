from pyspark.sql import SparkSession

# make sure to click the button to start kafka and banking simulation

# create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName('atm-visits').getOrCreate()
# set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

# read the atm-visits kafka topic as a source into a streaming dataframe with the bootstrap server kafka:19092,
# configuring the stream to read the earliest messages possible

atmVisitsRawStreamingDF = spark\
    .readstream\
    .format('kafka')\
    .option('kafka.bootstrap.servers', 'localhost:19092')\
    .option('subscribe', 'atm-visits')\
    .option('startingOffsets' 'earliest')\
    .load()

# using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings,
# and then select them
atmVisitsStreamingDF = atmVisitsRawStreamingDF.selectExpr('cast(key as string) transctionId',
                                                  'cast(value as string) location')


# create a temporary streaming view called "ATMVisits" based on the streaming dataframe
atmVisitsStreamingDF.createOrReplaceTempView('ATMVisits')

# query the temporary view with spark.sql, with this query: "select * from ATMVisits"
atmVisitsSelectStarDF = spark.sql('SELECT * FROM ATMVisits')

# this takes the stream and "sinks" it to the console as it is updated one message at a time:
# +---------+-----+
# |      key|value|
# +---------+-----+
# |241325569|Syria|
# +---------+-----+

# write the dataFrame from the last select statement to kafka to the atm-visit-updates topic, on the broker kafka:19092
# for the "checkpointLocation" option in the writeStream, be sure to use a unique file path to avoid conflicts with other spark scripts
atmVisitsSeelctStarDF.selectExpr("cast(transactionId as string) as key",
                                 "cast(location as string) as value")\
    .writeStream \
    .format('kafka')\
    .option('kafka.bootstrap.servers', 'localhost:9092')\
    .option('topic', 'atm-visit-updates')\
    .option('checkpointLocation', '/tmp/kafkacheckpoint')\
    .start()\
    .awaitTermination()


