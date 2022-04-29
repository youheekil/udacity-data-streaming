from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArraryType, DateType

# create a kafka message schema StructType including the following JSON elements:
#    {
#     "accountNumber":"703934969",
#     "amount":415.94,
#     "dateAndTime":"Sep 29, 2020, 10:06:23 AM"
#     }

bank_deposities_schema = StructType(
        [
                StructField('accountNumber', StringType()),
                StructField('amount', Inttype()),
                StructField('dateAndTime', DateType())
        ]
)

# create a spark session
spark = SparkSession.builder.appName('bank-deposits').getOrCreate()

# set the log level to WARN
spark.sparkContext.setLogLevel('WARN')

# read the atm-visits kafka topic as a source into a streaming dataframe with
# the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible

bankDepositRawStreamDF = spark \
    .readStream()\
    .format('kafka')\
    .option('kafka.bootstrap.servers', 'localhost:9092')\
    .option('subscribe', 'bank-deposits')\
    .option('StartingOffsets', 'earliest')\
    .load()

# it is necessary for kafka data frame to be readable to cast each file from
# a binary to a string
bankDepositStreamingDF = bankDepositRawStreamDf\
    .selectExpr("cast (key as string) key",
                "cast (value as string) value")

# this creates a temporary streaming view vased on the string DF
# It can later be queries with spark.sql
bankDepositStreamingDF.withColumn("value", from_json("value", bank_deposities_schema))\
    .select(col('value.*')) \
    .createReplaceTempView("BankDeposits")

# Using spark.sql we can select any valid select statement from the spark view
bankDepositsSelectStarDF = spark.sql("SELECT * FROM BankDeposits")

bankDepositsSelectStarDF.writeStream.outputMode("append").format("console").start().awaitTermination()