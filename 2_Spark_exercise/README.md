## Spark Streaming
#### Start Spark Master and Worker & Deploy and run the pyspark application 
1. Start the Spark Master
2. Read the logs from the master
3. Identify the URI for Spark Master
4. Start the Spark Worker
5. Submit the your Spark application (e.g. hellospark.py)
<details>
<summary>code</summary>

```shell 
# Started the Spark master 
$ /home/workspace/spark/sbin/start-master.sh

# View the log and copy down the Spark URI
$ cat [path to log]

# Started the Spark workers
$ /home/workspace/spark/sbin/start-slave.sh <SPARK URI>

# Submitted your application
$ /home/workspace/spark/bin/spark-submit /home/workspace/<your application name>.py
```

</details>

#### Base Spark application
<details>
<summary>code</summary>

```python 
# python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, Booleantype, Arraytype, DateType

# [opt] Create a variable with the absolute path to the text file 
logFile = "/home/workspace/Test.txt"

# create a Spark Session 
spark = SparkSession.builder.appName("HelloSpark").getOrCreate()

# set the log level to WARN
spark.sparkContext.setLogLevel("WARN")

logData = spark.read.text(logFile).cache()
# write functions
# stop Spark
spark.stop()
```
</details>


#### Read stream
<details>
<summary>Read stream from a kafka topic</summary>

```python 
# read a stream from the kafka topic <topic-name>, 
# with the bootstrap server localhost:9092, 
# reading from the earliest message

kafkaRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","<topic-name>")                  \
    .option("startingOffsets","earliest")\
    .load()  
 
```


</details>

#### Write Stream / Dataframe / Parse JSON
<details>
<summary>Write Dataframe from Kafka to the console</summary>

```python 

# cast the key and value columns as strings and 
# select them using a select expression function
# this is necessary for kafka Data Frame to be readable,
# into a single column value
kafkaStreamingDF = kafkaRawStreamingDF.selectExpr(
                            "cast(key as string) transactionID", # key
                            "cast(value as string) location")  # value

# write the dataframe to the console, and keep running indefinitely
# this takes the stream and 'sinks' it to the console as it is updated one at a time
kafkaStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()

```

</details>

<details>
<summary>Write stream to an in-memory table</summary>

```python 
# this creates a temporary streaming view based on the streaming dataframe
kafkaStreamingDF.createOrReplaceTempView("<name of tmp view table>")

# using spark.sql to select any valid `select` statment from the Spark View
TempViewTableSelectDF = spark.sql("SELECT * FROM <name of tmp view table>")

# this takes the stream and "sinks" it to the console as it is updated one message at a time like this:
# +---------+-----+
# |      key|value|
# +---------+-----+
# |241325569|Syria|
# +---------+-----+

# this takes the stream and "sinks" it to the console
TempViewTableSelectDF.selectExpr("cast(transactionID as string) as key", "cast(location as string) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "<new kafka topic name>") \
    .option("checkpointLocation", "/tmp/kafkacheckpoint") \ # you need to provide the folder to spark temp file
    .start() \
    .awaitTermination()
```

</details>


#### Joins and JSON


<details>
<summary>Parse Json</summary>

```python
# {
# "accountNumber":"703934969",
# "amount":415.94,
# "dateAndTime":"Sep 29, 2020, 10:06:23 AM"
# }

kafkaMessageSchema = StructType (
    [
        StructField("accountNumber", StringType()),
        StructField("amount", FloatType()),
        StructField("dateAndTime", DateType())
    ]
    
)

# deserialize the JSON from the straming dataframe using the kafka message StructType
# and create a temporary streaming view called "BankDeposit" so that it can later be queried with spark.sql

# .withColumn('newFieldName', 'data value')
# 'data value' -> from_json('column name', 'structSchemaVariable')

bankDepositStreamingDF.withColumn("value", from_json("value", kafkaMessageSchema)) \ 
    .select(col('value.*')) \
    .createOrReplaceTempView("BankDeposits")

bankDepositsSelectStarDF=spark.sql("select * from BankDeposits")

# this takes the stream and "sinks" it to the console as it is updated one message at a time:
# +-------------+------+--------------------+
# |accountNumber|amount|         dateAndTime|
# +-------------+------+--------------------+
# |    103397629| 800.8|Oct 6, 2020 1:27:...|
# +-------------+------+--------------------+

bankDepositsSelectStarDF.writeStream.outputMode("append").format("console").start().awaitTermination() 
```

</details>


<details>
<summary>Joining streaming Dataframes</summary>

```python
# ex 2
# Join the bank deposit and customer dataframes on the accountNumber fields
customerWithDepositDF = bankDepositsSelectStarDF.join(customerSelectStarDF, expr("""
    accountNumber = customerNumber
""" 

# TO-DO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:
#. +-------------+------+--------------------+------------+--------------+
#. |accountNumber|amount|         dateAndTime|customerName|customerNumber|
#. +-------------+------+--------------------+------------+--------------+
#. |    335115395|142.17|Oct 6, 2020 1:59:...| Jacob Doshi|     335115395|
#. |    335115395| 41.52|Oct 6, 2020 2:00:...| Jacob Doshi|     335115395|
#. |    335115395| 261.8|Oct 6, 2020 2:01:...| Jacob Doshi|     335115395|
#. +-------------+------+--------------------+------------+--------------+

customerWithDepositDF.writeStream.outputMode("append").format("console").start().awaitTermination()
    
# ex 3 
# Join the bank deposit and customer dataframes on the accountNumber fields
atmWithdrawalDF = bankWithdrawalsSelectStarDF.join(atmSelectStarDF, expr("""
    transactionId = atmTransactionId
"""                                                                                 
))

# this takes the stream and "sinks" it to kafka as it is updated one message at a time in JSON format:
# {"accountNumber":"862939503","amount":"844.8","dateAndTime":"Oct 7, 2020 12:33:34 AM","transactionId":"1602030814320","transactionDate":"Oct 7, 2020 12:33:34 AM","atmTransactionId":"1602030814320","atmLocation":"Ukraine"}

atmWithdrawalDF.selectExpr("cast(transactionId as string) as key", "to_json(struct(*)) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "withdrawals-location")\
    .option("checkpointLocation","/tmp/kafkacheckpoint")\
    .start()\
    .awaitTermination()



```
</details>


---
## Redis

#### Manually save and Read with Redis and Kafka
<details>
<summary>Start the Redis CLI</summary>

```shell
- Run the Redis CLI:
     - From a second terminal type: ```/data/redis/redis-stable/src/redis-cli -a notreally```
     - From the second terminal type: ```keys **```
     - You will see the list of all the Redis tables
     - From a third terminal type ```/data/kafka/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic redis-server```
     - From the second terminal type: ```zadd testkey 0 testvalue``` 
     - ```zrange testkey 0 -1```
     - Open the third terminal
     - A JSON message will appear from the redis-server topic
     - The key contains the base64 encoded name of the Redis table
     - zSetEntries and zsetEntries contain the changes made to the Redis sorted set. 
- To decode the name of the Redis table, open a fourth terminal, and type: ```echo "[encoded table]" | base64 -d```, for example ```echo "dGVzdGtleQ==" | base64 -d```
- To decode the changes made, from the zSetEntries table, copy the "element" value, then from the fourth terminal type: ```echo "[encoded value]" | base64 -d```, for example ```echo "dGVzdGtleQ==" | base64 -d```
```

</details>

<details>
<summary>Redis Command</summary>

```shell
keys **
zadd testkey 0 testvalue
echo "[encoded value]" | base64 -d
```

</details>

#### Parse Base64 Encoded Information
<details>
<summary>code</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic

redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue",StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr",BooleanType()),
        StructField("zSetEntries", ArrayType( \
            StructType([
                StructField("element", StringType()),\
                StructField("score", StringType())   \
            ]))                                      \
        )

    ]
)


# TO-DO: create a StructType for the CustomerLocation schema for the following fields:
# {"accountNumber":"814840107","location":"France"}

customerLocationSchema = StructType (
    [
        StructField("accountNumber", StringType()),
        StructField("location", StringType()),
    ]   
)


# the source for this data pipeline is a kafka topic, defined below
spark = SparkSession.builder.appName("customer-location").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

redisServerRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","redis-server")                  \
    .option("startingOffsets","earliest")\
    .load()                                     

#it is necessary for Kafka Data Frame to be readable, to cast each field from a binary to a string
redisServerStreamingDF = redisServerRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

# this creates a temporary streaming view based on the streaming dataframe
# it can later be queried with spark.sql, we will cover that in the next section 
redisServerStreamingDF.withColumn("value",from_json("value",redisMessageSchema))\
        .select(col('value.*')) \
        .createOrReplaceTempView("RedisData")

# Using spark.sql we can select any valid select statement from the spark view
zSetEntriesEncodedStreamingDF=spark.sql("select key, zSetEntries[0].element as customerLocation from RedisData")

zSetDecodedEntriesStreamingDF= zSetEntriesEncodedStreamingDF.withColumn("customerLocation", unbase64(zSetEntriesEncodedStreamingDF.customerLocation).cast("string"))

zSetDecodedEntriesStreamingDF\
    .withColumn("customerLocation", from_json("customerLocation", customerLocationSchema))\
    .select(col('customerLocation.*'))\
    .createOrReplaceTempView("CustomerLocation")\

customerLocationStreamingDF = spark.sql("select * from CustomerLocation WHERE location IS NOT NULL")

# this takes the stream and "sinks" it to the console as it is updated one message at a time (null means the JSON parsing didn't match the fields in the schema):

# +-------------+---------+
# |accountNumber| location|
# +-------------+---------+
# |         null|     null|
# |     93618942|  Nigeria|
# |     55324832|   Canada|
# |     81128888|    Ghana|
# |    440861314|  Alabama|
# |    287931751|  Georgia|
# |    413752943|     Togo|
# |     93618942|Argentina|
# +-------------+---------+

customerLocationStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()


```
</details>

#### Sink a subset of JSON
<details>
<summary>code</summary>


```python

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic

redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue",StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr",BooleanType()),
        StructField("zSetEntries", ArrayType( \
            StructType([
                StructField("element", StringType()),\
                StructField("score", StringType())   \
            ]))                                      \
        )

    ]
)


# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic
# since we are not using the date in sql calculations, we are going
# to cast them as strings
# {"customerName":"Frank Aristotle","email":"Frank.Aristotle@test.com","phone":"7015551212","birthDay":"1948-01-01","accountNumber":"750271955","location":"Jordan"}
customerJSONSchema = StructType (
    [
        StructField("customerName",StringType()),
        StructField("email",StringType()),
        StructField("phone",StringType()),
        StructField("birthDay",StringType()),
        StructField("accountNumber",StringType()),
        StructField("location",StringType())        
    ]
)


# the source for this data pipeline is a kafka topic, defined below
spark = SparkSession.builder.appName("customer-record").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

redisServerRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","redis-server")                  \
    .option("startingOffsets","earliest")\
    .load()                                     

#it is necessary for Kafka Data Frame to be readable, to cast each field from a binary to a string
redisServerStreamingDF = redisServerRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

# this creates a temporary streaming view based on the streaming dataframe
# it can later be queried with spark.sql, we will cover that in the next section 
redisServerStreamingDF.withColumn("value",from_json("value",redisMessageSchema))\
        .select(col('value.*')) \
        .createOrReplaceTempView("RedisData")

# Using spark.sql we can select any valid select statement from the spark view
zSetEntriesEncodedStreamingDF=spark.sql("select key, zSetEntries[0].element as customer from RedisData")

zSetDecodedEntriesStreamingDF= zSetEntriesEncodedStreamingDF.withColumn("customer", unbase64(zSetEntriesEncodedStreamingDF.customer
                                                                                            ).cast("string"))

zSetDecodedEntriesStreamingDF\
    .withColumn("customer", from_json("customer", customerJSONSchema))\
    .select(col('customer.*'))\
    .createOrReplaceTempView("Customer")\

customerStreamingDF = spark.sql("select accountNumber, location, birthDay from Customer where birthDay is not null")

relevantCustomerFieldsStreamingDF = customerStreamingDF.select('accountNumber','location',split(customerStreamingDF.birthDay,"-").getItem(0).alias("birthYear"))

# this takes the stream and "sinks" it to the console as it is updated one message at a time (null means the JSON parsing didn't match the fields in the schema):

# {"accountNumber":"288485115","location":"Brazil","birthYear":"1938"}

relevantCustomerFieldsStreamingDF.selectExpr("CAST(accountNumber AS STRING) AS key", "to_json(struct(*)) AS value")\
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "customer-attributes")\
    .option("checkpointLocation","/tmp/kafkacheckpoint")\
    .start()\
    .awaitTermination()



```
</details>

### Final Bank Simulation exercise

<details>
<summary>code</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic

redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue",StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr",BooleanType()),
        StructField("zSetEntries", ArrayType( \
            StructType([
                StructField("element", StringType()),\
                StructField("score", StringType())   \
            ]))                                      \
        )

    ]
)


# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic
# since we are not using the date in sql calculations, we are going
# to cast them as strings
# {"customerName":"Frank Aristotle","email":"Frank.Aristotle@test.com","phone":"7015551212","birthDay":"1948-01-01","accountNumber":"750271955","location":"Jordan"}
customerJSONSchema = StructType (
    [
        StructField("customerName",StringType()),
        StructField("email",StringType()),
        StructField("phone",StringType()),
        StructField("birthDay",StringType()),
        StructField("accountNumber",StringType()),
        StructField("location",StringType())        
    ]
)

# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic
# {"accountNumber":"703934969","amount":625.8,"dateAndTime":"Sep 29, 2020, 10:06:23 AM","transactionId":1601395583682}
customerLocationSchema = StructType (
    [
        StructField("accountNumber", StringType()),
        StructField("location", StringType()),
    ]   
)

# the source for this data pipeline is a kafka topic, defined below
spark = SparkSession.builder.appName("customer-record").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

redisServerRawStreamingDF = spark                          \
    .readStream                                          \
    .format("kafka")                                     \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","redis-server")                  \
    .option("startingOffsets","earliest")\
    .load()                                     

#it is necessary for Kafka Data Frame to be readable, to cast each field from a binary to a string
redisServerStreamingDF = redisServerRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

# this creates a temporary streaming view based on the streaming dataframe
# it can later be queried with spark.sql, we will cover that in the next section 
redisServerStreamingDF.withColumn("value",from_json("value",redisMessageSchema))\
        .select(col('value.*')) \
        .createOrReplaceTempView("RedisData")

# Using spark.sql we can select any valid select statement from the spark view
zSetEntriesEncodedStreamingDF=spark.sql("select key, zSetEntries[0].element as redisEvent from RedisData")

# Here we are base64 decoding the redisEvent
zSetDecodedEntriesStreamingDF1= zSetEntriesEncodedStreamingDF.withColumn("redisEvent", unbase64(zSetEntriesEncodedStreamingDF.redisEvent).cast("string"))
zSetDecodedEntriesStreamingDF2= zSetEntriesEncodedStreamingDF.withColumn("redisEvent", unbase64(zSetEntriesEncodedStreamingDF.redisEvent).cast("string"))

# Filter DF1 for only those that contain the birthDay field (customer record)

zSetDecodedEntriesStreamingDF1.filter(col("redisEvent").contains("birthDay"))

# Filter DF2 for only those that do not contain the birthDay field (all records other than customer) we will filter out null rows later
zSetDecodedEntriesStreamingDF2.filter(~col("redisEvent").contains("birthDay"))


# Now we are parsing JSON from the redisEvent that contains customer record data
zSetDecodedEntriesStreamingDF1\
    .withColumn("customer", from_json("redisEvent", customerJSONSchema))\
    .select(col('customer.*'))\
    .createOrReplaceTempView("Customer")\

# Last we are parsing JSON from the redisEvent that contains customer location data
zSetDecodedEntriesStreamingDF2\
    .withColumn("customerLocation", from_json("redisEvent", customerLocationSchema))\
    .select(col('customerLocation.*'))\
    .createOrReplaceTempView("CustomerLocation")\

# Let's use some column aliases to avoid column name clashes
customerStreamingDF = spark.sql("select accountNumber as customerAccountNumber, location as homeLocation, birthDay from Customer where birthDay is not null")

# We parse the birthdate to get just the year, that helps determine age
relevantCustomerFieldsStreamingDF = customerStreamingDF.select('customerAccountNumber','homeLocation',split(customerStreamingDF.birthDay,"-").getItem(0).alias("birthYear"))

# Let's use some more column alisases on the customer location 
customerLocationStreamingDF = spark.sql("select accountNumber as locationAccountNumber, location from CustomerLocation")

currentAndHomeLocationStreamingDF = customerLocationStreamingDF.join(relevantCustomerFieldsStreamingDF, expr( """
   customerAccountNumber=locationAccountNumber
"""
))

# This takes the stream and "sinks" it to the console as it is updated one message at a time:

# +---------------------+-----------+---------------------+------------+---------+
# |locationAccountNumber|   location|customerAccountNumber|homeLocation|birthYear|
# +---------------------+-----------+---------------------+------------+---------+
# |            735944113|      Syria|            735944113|       Syria|     1952|
# |            735944113|      Syria|            735944113|       Syria|     1952|
# |             45952249|New Zealand|             45952249| New Zealand|     1939|
# |            792107998|     Uganda|            792107998|      Uganda|     1951|
# |            792107998|     Uganda|            792107998|      Uganda|     1951|
# |            212014318|     Mexico|            212014318|      Mexico|     1941|
# |             87719362|     Jordan|             87719362|      Jordan|     1937|
# |            792350429|    Nigeria|            792350429|     Nigeria|     1949|
# |            411299601|Afghanistan|            411299601| Afghanistan|     1950|
# |            947563502|     Mexico|            947563502|      Mexico|     1944|
# |            920257090|      Syria|            920257090|       Syria|     1948|
# |            723658544|       Iraq|            723658544|        Iraq|     1943|
# |             39252304|    Alabama|             39252304|     Alabama|     1965|
# |             39252304|    Alabama|             39252304|     Alabama|     1965|
# |            508037708|     Brazil|            508037708|      Brazil|     1947|
# +---------------------+-----------+---------------------+------------+---------+
currentAndHomeLocationStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()

```
</details>

---
## Docker
The docker-compose file at the root of the repository creates 9 separate containers:

* Redis
* Zookeeper (for Kafka)
* Kafka
* Banking Simulation
* Trucking Simulation
* STEDI (Application used in Final Project)
* Kafka Connect with Redis Source Connector
* Spark Master
* Spark Worker

```shell
cd [repositoryfolder]
docker-compose up
```