# Start a Spark Cluster

- From the terminal: ```cd /home/workspace/spark/sbin```

- Type: ```./start-master.sh```

- Watch for a message similar to this: ```Logging to /home/workspace/spark/logs/spark--org.apache.spark.deploy.master.Master-1-5a00814ba363.out```
    
- Copy the path to your log, for example: ```/home/workspace/sparklogs/logs/spark--org.apache.spark.deploy.master.Master-1-5a00814ba363.out```

- Type: ```cat [path to log]```

- Look for a message like this: ```Starting Spark master at spark://5a00814ba363:7077```
    
- Copy the Spark URI, for example: ```spark://5a00814ba363:7077```

- Type: ```./start-slave.sh [Spark URI]```
    
# Create a hello world Spark application and submit it to the cluster

- Complete the hellospark.py application (be sure to click File Save when done)

- From the terminal type: ```cd /home/workspace/spark/bin```

- Type: ```./spark-submit /home/workspace/hellospark.py```

- Watch for the output at the end for the counts

# solution
Started the Spark master
Started the Spark workers
Submitted your application
```shell
$ /home/workspace/spark/sbin/start-master.sh
$ /home/workspace/spark/sbin/start-slave.sh [SPARK URI]
$ /home/workspace/spark/bin/spark-submit /home/workspace/hellospark.py
```