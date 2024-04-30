from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

df = spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "apr21").load()

ndf = df.selectExpr("CAST(value AS STRING)").withColumn("name",split(col("value"),",")[0])\
  .withColumn("age", split(col("value"),",")[1]).withColumn("city",split(col("value"),",")[2]).drop("value")


def foreach_batch_function(dfd, epoch_id):
  local_host = "jdbc:mysql://localhost:3306/satwant_venu?useSSL=false&user=root&password=Mypassword@321"
  dfd.write.mode("append").format("jdbc").option("url", local_host).option("dbtable", "kafka_Live").option("driver","com.mysql.cj.jdbc.Driver").save()


ndf.writeStream.outputMode("append").foreachBatch(foreach_batch_function).start().awaitTermination()

# ndf.writeStream.outputMode("append").format("console").start().awaitTermination()

