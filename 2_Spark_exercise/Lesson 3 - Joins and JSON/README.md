# Joins and JSON
---
## 1. Parse a Source JSON Payload
### 1. Analyze the source sample
```json
{
  "truckNumber":"5169", (Type: String)
  "destination":"Florida", (Type: String)
  "milesFromShop":505, (Type: Integer)
  "odomoterReading":50513 (Type: Integer)
}
```
### 2. Create a StructType 
* StructType: a spark class that defines the schema for a DataFrame
* StructField: a Python class used to create a typed field in a StructType
```python
# Sample JSON: 
# {
#    "truckNumber":"2352523",
#    "destination":"Accra",
#    "milesFromShop":100,
#    "odometerReading": 99789
# }

truckStatusSchema = StructType (   
        [
          StructField("truckNumber", StringType()),        
          StructField("destination", StringType()), 
          StructField("milesFromShop", IntegerType()),  
          StructField("odometerReading", IntegerType())
        ]
)
```
### 3. Parse JSON with a schema
```python
# vehicleStatusStreamingDF \
#   .withColumn('newFieldName', 'data value')
# 'data value' -> from_json('column name', 'structSchemaVariable')
# from_json: the column name we want to extract JSON out of the variable 
# the variable which holds the `StructType` defining our schema

vehicleStatusStreamingDF \
    .withColumn('value', from_json('value', truckStatusSchema))
```
* `vehicleStatusStreamingDF`: name of the DataFrame in this example
* `withColumn`: we parse the JSON field called value, using the `.withColumn` method which creates or overwrites an existing column
* `from_json`: (overwriting it) the new value is assigned using the `from_json` function which uses the column anme containg the JSON, value, and the `truckStatusSchema`

### Exercise 1 - Parse a JSON payload into seperate fields for analysis 
working with a kafka topic created to broadcast deposits in a bank. Each messages contins the following data
- account number
- amount
- date and time

* you will create a view with this data to query it using spark.sql

- [x] read a stream from a kafka topic
- [x] write the dataaframe from kafka to the console
- [x] parse the dataframe and store it as a temp view
- [x] SELECT * FROM the_temp_view, and write the stream to the console
- [x] start spark master, and worker
- [x] deploy and run the pyspark application 



## 2. Join Streaming DataFrames

- Data Mocking: to create a representative sample of an undefined source of data
  - As you interview system owners from other teams it will be important to discuss the fields they need
  - Once you have discussed the data that is needed you can create a mock data sample
  - Ask for feedback on the mock data sample before writing the spark application 
  - This will give you the chance to refine the data model

- Join: to connect two data collections by referencing a field they share in common; or the state which connects two data collections by referencing a common field. 
- What does calling `.join` do?
  - function called on a DataFrame that accepts the right hand of the join (another Dataframe), and the join expression (includes the fields to join on). This is the Spark function that implements the concept of joining two different groups of data.
  - you will need two DataFrames defined already
  - you will want to avoid using the same column names in each DataFrame avoid collisons
  - Using field aliases in each DataFrame where needed (ex: "truckNumber as statusTruckNumber")
  - the default join type is left outer join
```python 
checkinStatusDF = vehicleStatusSelectStarDF \
    .join(vehicleCheckinSelectStarDF, expr("""
        statusTruckNumber = checkinTruckNumber""")
        )
```



### exercise 2. Join Streaming DataFrames from Different Datasources

- working with the deposit topic and a topic that contains customer information.
- each customer message contains the following info:
  - customer name
  - email
  - phone 
  - birth date
  - account number
  - customer location 

- [ ] Read a stream from a kafka topic containg JSON 
- [ ] Write the dataframe from kafka to the console with the key, value fields
- [ ] Read a second stream from a seperate kafka topic containing JSON
- [ ] write the second stream from kafka fo the console with the key, value fields
- [ ] Parse the JSON from the first dataframe and store it as a temp view
- [ ] Parse the JSON from the second dataframe and store it as another temp view
- [ ] SELECT * FROM the first temp view, and write the stream to the console
- [ ] SELECT * FROM the second temp view, and write the stream to the console
- [ ] Join both streams on a common field (Ex. bank account number)
- [ ] Write the joined stream to the console. 
- [ ] Start the spark master and worker
- [ ] Deploy and run the pyspark application

## 3. Sink to Kafka

### Sinking Data to Kafka
1. Create a Straming DataFrame
2. Determine/Identify the sink kafka topic
3. Sink a DataFrame to the topic 

- In order to sink to kafka, you need to have something to sink
- That will be a Streaming DataFrame you create
- When we sink to kafka we need to identify the topic we will be sending data to 
- Then it is just a matter of writing a few lines to ink your dataframe
- They will look similar to what you have done to sink to the console

```python
vehicleStatusDF\
    # use a select expr on the DF to cast the key, and structure your fileds as JSON
    .selectExpr("cast(statusTruckNumber as string") as key",
                "to_json(struct(*)) as value")\
    # last sink the dataframe to your new kafka topic, using `writeStream`
    .writeStream\
    .format("kafka")\
    # Be sure to pass the kafka bootstrap servers parameter
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "vehicle-status-chagnes")\
    .option("checkpointLocation", "/tmp/kafkacheckpoint")\
    .start()\
    .awaitTermination()
```

- `.selectExpr`: a function called on a datafram(DF) to pass a select expression
  - `.selectExpr("cast(statusTruckNumber as string) as key", "to_json(struct(*)) as value")`
  - we are creating a DF with two fields
  - each parameter defines a field
  - when streaming to kafka, the field should be called "key" and "value"
  - the key is unique identifier
  - the value contains the JSON

- `to_json`: A Spark SQL function that accepts multiple columns containing JSON and creates a single column containing the JSON formatted data
  - `to_json(struct(*)) as value`
  - we use `to_json` in `.selectExpr` on a Df
  - first, pass the fields you want to be serialized
  - then alias them as a field
  - when passing the JSON to Kafka we want the field name to be "Value"

- `checkpointLocation`: an option passed when connecting to Kafka to send data to a topic; must be a writeable filesystem path for the Spark Worker. 
  - `.option("checkpointLocation","/tmp/kafkacheckpoint")`
  - When streaming to Kafka, you must define the same options when reading from Kafka 
  - You also need an additional option: checkpointLocation 
  - This needs to be a writeable path 
  - It is used by Spark to offload some data to disk for recovery


### exercise 3. write a Streaming Dataframe to kafka with aggregated Data

- working with a bank withdrawals topic and ATM visit topic
- the goal will be to join both those data streams in a way that allows you to identify withdrawals connected with an ATM visit
- [ ] Read a stream from a kafka topic contaning JSON
- [ ] Write the dataframe from kafka to the console with they key, value fields
- [ ] Read a second stream from a separate kafka topic containig JSON
- [ ] Write the second stream from kafka to the console with the key, value fields
- [ ] Parse the JSON from the first dataframe and store it as a temp view
- [ ] Parse the JSON from the second dataframe and store it as another temp view
- [ ] SELECT * FROM the first temp view, and write the stream to the console
- [ ] SELECT * FROM the second temp view, and write the stream to the console
- [ ] Write the joined stream to a third kafka topic
- [ ] start spark master, and worker
- [ ] deploy and run the pyspark application 