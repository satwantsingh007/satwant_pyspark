from kafka import KafkaConsumer
from pyspark.sql import *
from pyspark.sql.functions import *
import json
from pyspark.sql.types import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "apr28").load()

schema = StructType([StructField('results', ArrayType(
    StructType([StructField('user', StructType([StructField('gender', StringType(), True),
                                                StructField('name',
                                                            StructType([StructField('title', StringType(), True),
                                                                        StructField('first', StringType(), True),
                                                                        StructField('last', StringType(), True)]),True),
                                                StructField('location',
                                                            StructType([StructField('street', StringType(), True),
                                                                        StructField('city', StringType(), True),
                                                                        StructField('state', StringType(), True),
                                                                        StructField('zip', IntegerType(), True)]),
                                                            True),
                                                StructField('email', StringType(), True),
                                                StructField('username', StringType(), True),
                                                StructField('password', StringType(), True),
                                                StructField('salt', StringType(), True),
                                                StructField('md5', StringType(), True),
                                                StructField('sha1', StringType(), True),
                                                StructField('sha256', StringType(), True),
                                                StructField('registered', IntegerType(), True),
                                                StructField('dob', IntegerType(), True),
                                                StructField('phone', StringType(), True),
                                                StructField('cell', StringType(), True),
                                                StructField('AVS', StringType(), True),
                                                StructField('picture',
                                                            StructType([StructField('large', StringType(), True),
                                                                        StructField('medium', StringType(), True),
                                                                        StructField('thumbnail', StringType(), True)]),
                                                            True)]), True)])), True),
                     StructField('nationality', StringType(), True),
                     StructField('seed', StringType(), True),
                     StructField('version', StringType(), True)])

res = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

def foreach_batch_function(dfd, epoch_id):
  sfOptions = {
    "sfURL":"eryqttz-lg79072.snowflakecomputing.com",
    "sfUser":"SATWANT",
    "sfPassword":"Waheguru@321a",
    "sfDatabase":"satwant",
    "sfSchema":"public",
    "sfWarehouse":"COMPUTE_WH",
    "dbtable": "liveKafkaLogs"
  }

  SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
  dfd.write.mode("append").format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).save()

  pass

# res.writeStream.outputMode("append").format("console").start().awaitTermination()
res.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()
