# sparkpyeventskafkastreamtoconsole.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

# create a Spark Session
spark = SparkSession.builder.appName("steadi-kafka-event").getOrCreate()

# set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

# TO-DO:using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source

kafkaRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()

# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream

# TO-DO: cast the value column in the streaming dataframe as a STRING 
kafkaStreamingDF = kafkaRawStreamingDF.selectExpr(
        "cast (value as string) value ")

# TO-DO: parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
#
# storing them in a temporary view called CustomerRisk

kafkaStediEventSchema = StructType(
        [
                StructField("customer", StringType()),
                StructField("score", FloatType()),
                StructField("riskDate", DateType())
        ]

)

kafkaStreamingDF.withColumn("value", from_json("value", kafkaStediEventSchema)).select(
    col('value.*')).createOrReplaceTempView("CustomerRisk")

# TO-DO: execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF

customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")

# TO-DO: sink the customerRiskStreamingDF dataframe to the console in append mode
# 
# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----

customerRiskStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()

# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafkastreaming.sh
# Verify the data looks correct 

