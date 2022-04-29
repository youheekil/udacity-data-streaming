# Streaming Dataframes, Views, Spark SQL

## Key Terminologies
* `Data Streaming`: Data Streaming is the process of transmitting and/or receiving data envelopes or messages over a potentially undefined space of time, while applying computational algorithms to those data envelopes or messages.
* `Spark`: an open-source framework for distributed computing across a cluster of servers; typically, programming is required.
* `Kafka`: a durable message broker used to mediate the exchange of messages between multiple applications.
* `Cluster`: an orientation of two or more servers in such a fashion that they can communicate directly with one another or with a cluster manager; often for the purpose of high availability or increased capacity.
* `Kubernetes`: an open-source technology used to coordinate and distribute computing.
* `Zookeeper`: an open-source technology that enables semi-autonomous healing of a server cluster.
* `Data Pipeline`: a series of processing steps that augment or refine a raw data source in preparation for consumption by another system.

> Structure of a Data Pipeline: 
1. Every Spark Application starts by reading in a source
2. A series of steps is executed that transforms the original flow of information
3. The end result is a DataFrame that can be sinked externally with an outside system
---
## Spark Cluster
Spark Component Inventory
* Zookeeper: connects to the Spark Master
* `spark-submit` command: deploys the Spark Application
* Spark Master: connects to the Spark workers.
* Spark Workers: connect to the Spark Master on the Spark URI in a Standalone Cluster; in a Kubernetes configuration they are orchestrated by Kubernetes.

## Staring a Spark Cluster
1. Run the start-master.sh script
```shell
/data/spark/sbin/start-master.sh
```
2. Watch for the log file location
3. Cat or tail the log file to find the Spark Master URI
```shell
tail -f /opt/spark-2.3.4-bin-hadoop2.7/logs/spark--org.apache.spark.deploy.master.Master-1-719f72471d5b.out
```
4. Run the start-slave.sh script passing the Spark Master URI parameter
```shell
$ /data/spark/sbin/start-slave.sh spark://719f72471d5b:7077
```
## Create Spark View 
* Spark View: in a Spark application, a session-bound representation of data in a certain configuration 
(eg. a view of discounted inventory)
* Creating a view is like using a colored filter that changes certain things in a picture
* Using spark you can create temporary views of data which emphasize or reveal certain attributes
* For example, if you wanted to extract a year from a field that contains a month, day, and year, you could save that as a view
* The purpose of temporary views is to save a certain configuration of a DataFrame for later reference within the same Spark application
* The view won't be available to other Spark Applications

* Creating then Querying a Spark View in Action 
```python 
fuelLevelStreamingDF.createOrReplaceTempView('FuelLevel')
fuelLevelStreamingDF = spark.sql('SELECT * FROM FuelLevel')
```
* Query a View then Sink to Kafka
```python
# select the fields using spark.sql
fuelLevelKeyValueDF = spark.sql('SELECT key, value FROM Fuellevel')

fuelLevelKeyValueDF\ # use a select expression on the resulting DataFrame to cast the fields
.selectExpr('cast(key as string) as key', \
            'cast(value as string) as value') \
.writeStream\
.format('kafka')\ # be sure to pass the kafka bootstrap servers parameter
.option('kafka.bootstrap.servers', 'localhost:9092')\
.option('topic', 'fuel-changes')\ # sink the DF to our new topic, using 'writeStream'
.option('checkpointLocation', '/tmp/kafkacheckpoint')\
.start()\
.awaitTermination()
```
* key points
1. kafka topics have two fields: `key` and `value`
2. cast binary data from kafka using the `.selectExpr` function on the Data frame
3. read from kafka using the `readStream` field on the Spark Session 
4. write to kafka using the writeStream field on the DataFrame. 


## Is Spark Running?
Typing ps -ef will give you a list of processes running on the server:

Watch for two lines that have the word spark in them
You may have to expand the terminal to see the full output
Check if there are two lines containing the word spark
Each is a process, one runs the master, one runs a worker
This means Spark is actively running
---
### Exercise 1
> Start a Spark Cluster
* From the terminal: `cd /home/workspace/spark/sbin`
* Type: `./start-master.sh`
* Watch for a message similar to this: Logging to `/home/workspace/spark/logs/spark--org.apache.spark.deploy.master.Master-1-5a00814ba363.out`
* Copy the path to your log, for example: `/home/workspace/sparklogs/logs/spark--org.apache.spark.deploy.master.Master-1-5a00814ba363.out`
* Type: `cat [path to log] or tail -f [path to log]` , (if usingtail -f, remember to use ctrl C to stop the operation)
* Look for a message like this: Starting Spark master at spark://5a00814ba363:7077
* Copy the Spark URI, for example: spark://5a00814ba363:7077
* Type: ./start-slave.sh [Spark URI]

### Exericse 2 - Creating a Spark Streaming Dataframe
**Make sure kafka is running !**
* [ ] Read a stream from Kafka topic
* [ ] Write the DataFrame from Kafka to the console
* [ ] Start Spark Master, and Worker
* [ ] Deploy the run the pyspark application 

### Exercise 3 - Query a temporary spark view

- Click the button to start Kafka and Banking Simulation: 
```shell
/home/workspace/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 /home/workspace/atm-visits.py 
```
- Start the spark master and spark worker
     - From the terminal type: ```/home/workspace/spark/sbin/start-master.sh```
     - View the log and copy down the Spark URI
     - From the terminal type: ```/home/workspace/spark/sbin/start-slave.sh [Spark URI]```
- Complete the atm-visits.py python script
- Submit the application to the spark cluster:
     - From the terminal type: 
     ```/home/workspace/submit-atm-visits.sh```
- Watch the terminal for the values to scroll past
    - Remember the first run of a new topic will raise an error so you will need to type the following in the terminal:
    ```/data/kafka/bin/kafka-console-consumer --bootstrap-server localhost:[localhost] --topic [topicName] --from-beginning```

## Docker Usages Procedure 
It is recommended that you configure Docker to allow it to use up to 2 cores and 6 GB of 
your host memory for use by the course workspace. If you are running other processes using 
Docker simultaneously with the workspace, you should take that into account also.

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

It also mounts your repository folder to the Spark Master and Spark Worker containers as 
a volume /home/workspace, making your code changes instantly available within the containers running Spark.

```shell
cd [repositoryfolder]
docker-compose up

# check 9 containers
docker ps
```
