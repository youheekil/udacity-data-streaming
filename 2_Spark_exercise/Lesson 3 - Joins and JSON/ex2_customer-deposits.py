from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, FloatType, BooleanType, ArrayType, DateType,

# create deposit a kafka message schema
# {"accountNumber":"703934969",
# "amount":415.94,
# "dateAndTime":"Sep 29, 2020, 10:06:23 AM"}

deposit_schema = StructType([
        StructField("accountNumber", StringType()),
        StructField("amount", FloatType()),
        StructField("dateAndTime", DateType())
])

# create a customer kafka message schema
# {"customerName":"Trevor Anandh",
# "email":"Trevor.Anandh@test.com",
# "phone":"1015551212",
# "birthDay":"1962-01-01",
# "accountNumber":"45204068",
# "location":"Togo"}

customer_schema = StructType([
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthday", DateType()),
        StructField("accountNumber", StringType()),
        StructField("location", StringType())
])

# create a spark session
spark = SparkSession.builder.appName('customer-deposits').getOrCreate()

# set the log level to WARN
spark.sparkSession.setLogLevel('WARN')

# read a the atm-visits kafka topic as a source into a streaming dataframe
# with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible

atmVisitsRawStreamDF