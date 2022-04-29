# Redis, Base64, JSON

1. Manually save to Redis Read the same data from a kafka topic
2. Parse Base64 Encoded Information 
3. Sink a subset of JSON Fields

## Terminologies
* **Redis**: a database used primarily for caching; this means it is optimized for fast reads and writes
  * is super fast in terms of read/write execution and in the developement process
  * used by a lot of companies
  * Great for rapid prototyping of new applications
  * it can take you well beyond a proof of concept, all the way to production without having to create or maintain schemas
  * later you can create a structed schema, and migrate to a relational database
  * you can stick with Redis, and create a nightly extract for reports 

* **Base64**: an encoding format used by computers to transmit and store information 
  * is used to make the text more readable by servers
  * how to recognize Base64 encoding?
    * a long string of letters
    * the string doesn't spell anything
    * the string ends with '='
    * try decoding it with a free online decoder (base64decode.org)

* **Kafka Connect**: part of the Confluent Kafka distribution; it is the component responsible for providing a path from an external system (a source) to a Kafka topic or from a Kafka topic to an external system (a sink)
* **Source Connector**: A source counnector provides a connection from an outside system (a source) to a Kafka topic
* **Sink Connector**: A sink connector provides a connection from a Kafka topic to an outside system (a source)

## 1. Manaully save to Redis and Read from Redis

* From the source to the sink (kafka)
  1. source transmits some data to kafka
  2. kafka transmits that same data to a topic w/o modification
  3. The sink, or receiving system, decodes the information that was encoded by the source system

* Redis data types - redis data is stored in various data types
  * a sorted set is a redis collection that includes a value and a score
  * a score is a secondary index. we use `zadd` to add records to a `sorted set`
  * a redis key/value pair is a simple `key` with one `value` 
  * we use the set command to `set` the value 

### kafka connect
Redis source connector properties
```python
name = redis-config
connector.class = org.apache.kafka.connect.redis.Redis.SourceConnector
tasks.max = 1
topic = redis-server
host = redis
port = 6379
password = notreally
dbName = 0

```

* How a Source Connector Works 
  * kafka starts the connector
  * the connector begins reading from the source
  * new records appear in the configured topic 


## 2. Parse Base64 Encoded Information 

* How would I decode Base64 that in Spark
  * `unbase64`
  * `unbase64(encodedStreamingDF.reservation).cast("string")`

* Using unbase64
The `encodedStreamingDF` Dataframe has a field called `"reservation"`.
We are decoding it and then casting it to a string. This statement must be used in a spark SQL or DataFrame select. 

check the [pyspark.sql module](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html)

### Exercise 2. 
Vaidate the Kafka Connect Redis Source, so that we can start writing code to extract data from the redis-server topic
Redis transmits its data to Kafka in Base64 encoded format. We will need to use the `unbase64` method to decode it in our Spark Application. 

* [ ] Write a pyspark application 
* [ ] Read a stream from the `redis-server` Kafka topic containing Base64 encoded data
* [ ] Write the dataframe from Kafka to the console with the `key`, `value` fields. 
* [ ] Base64 decode the Dataframe 
* [ ] Write the decoded stream to the console
* [ ] Start Spark master, and worker
* [ ] Deploy and run the pyspark application. 

## 3. Sink a subset of JSON Fields
### 1. Two ways to select
There are two main functions used for selecting data in Spark Streaming: `.select` and `spark.sql`

* `.select` : `truckReservationDF.select("reservationID", "truckNumber")
  * The `.select` function accepts column names
* `spark.sql`: 
  * the `spark.sql` function accepts valid SQL
```python 
spark.sql("SELECT reservationId, truckNumber 
          FROM TruckReservation
          WHERE reservatinDate IS NOT NULL")
```
### 2. Spark SQL functions
* `to_json`: create JSON
* `from_json`: parse JSON
* `unbase64`: Base64 Decode
* `base64`: Base64 Encode
* `split`: Separate a string of characters

### 3. Splitting strings

#### ex. Getting First Name 
```json
{
"customerName":"June Aristotle",
"email":"June.Aristotle@test.com",
"birthDay":"1948-01-01"
}
```

```python
split(customerStreamingDF.customerName, " ")\
.getItem(0)\
.alias("firstName")\
```

1. using `split` we extract both first and last names
2. we then call `.getItem` to get the 0th element. 
3. then we call `.alias` to crete a field called "firstName"

#### ex. Getting email domain
```json

{

"customerName":"June Aristotle",

"email":"June.Aristotle@test.com",

"birthDay":"1948-01-01"

}
```
```python
split(customerStreamingDF.email, "@"\
.getItem(1)\
.alias("emailDomain")\
```

1. call `split` passing the `cusotmerStreaming.email" field, and then the @ symbol
2. then get the element from position `1`, then get the element from position `, which will be the email domain. 
3. so call the `.alias` function to create an alias called "emailDomain"

#### example of using `to_json`
* dataframe : `truckStatusDF`
* using field : `statusTruckNumber`
* you want to prepare to sink the DataFrame by converting it to JSON, kafka also expects a key for every message

```python 
truckStatusDF\
.selectExpr(
"cast(statusTruckNumber as string) as key", 
"to_json(struck(*)) as value"
```

1. call `.selectExpr`
2. whith `.selectExpr`, we are creating two expressions,
* the key field
* the value field
3. use to `to_json` function create JSON

#### Exercise 3. 
There are a few customer fields we are lookig for from the `redis-server` server
* account number
* location 
* birth year
we want to combine those fields into one JSON message and transmit them in a `customer-attributes`

### 4. Sink it to Kafka
* dataframe: truckStatusDF
* create json with statusTruckNumber field and sink it to kafka topic `truck-status`
```python 
truckStatusDF \
  .selectExpr(
  "cast(statusTruckNumber as string) as key", 
  "to_json(struct(*)) as value"
  )\
  .writeStream \
  .format("kafka")\
  .option("kafka.bootstrap.servers", "kafkabroker.besthost.net:9092")\
  .option("topic", "truck-status")\
  .option("checkpointLocation", "/tmp/kafkacheckpoint")\
  .start()\
  .awaitTermination()
```
1. after the select expression ends, we call `.writeStream`
2. the bootstrap server (the broker) is `kafkabroker.besthost.net`
3. the bootstrap port is 9092
4. the topic is `truck-status`
5. we specified a writable path on the spark worker server for the `checkpointLocation`


[pyspark sql module](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html)
#### Exercise 4 
Decode and Join two Base64 Encoded JSON DataFrames
Now that you have learned how to use unbase64 to extract useful data, let's take things a step further. There is often a need to join data from separate data types. In this exercise we join data from the customer and the customerLocation message types.

### Edge Cases
* What if I subscribe to the wrong kafka topic?
What if you subscribe to the wrong kafka topic in your spark application and then correct it later? you may run into the error 
* such as:
```shell 
Current committed Offsets: {KafkaV2[Subscribe[topic-name]]: {'wrong-topic-name':{'0':0}}}

Some data may have been lost because they are not available in Kafka any more; 
either the data was aged out by Kafka or the topic may have been deleted
before all the data in the topic was processed. If you don't want your 
streaming query to fail on such cases, set the source option
"failOnDataLoss" to "false."
```
Then you can add the option `failOnDataLoss` to your `.writeStream` and change the topic name the application writes to as below:
```python
checkinStatusDF.selectExpr("cast(statusTruckNumber as string) as key", "to_json(struct(*)) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "checkin-status")\
    .option("checkpointLocation","/tmp/kafkacheckpoint")\
    .option("failOnDataLoss","false")\    
    .start()\
    .awaitTermination()
  ```