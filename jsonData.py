from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

data = r'C:\bigdata\drivers-20230725T062459Z-001\drivers\zips.json'
df = spark.read.json(path=data, mode="DROPMALFORMED")  # DROPMALFORMED it is used to remove corrupt records like column
# df.show(truncate=False)
print(df.count())
df.select(countDistinct("city")).show()
df.sample(fraction=0.20).take(4)
print(df.take(6))
df.first()

df = df.withColumn("loc", explode(col("loc"))) \
    .withColumnRenamed("_id", "id") \
    .withColumnRenamed("pop", "pincode")
res = df.groupby(col("state")).agg(count("*").alias("count")).orderBy(col("count").desc())
res.show()


local_host = "jdbc:mysql://localhost:3306/satwant_venu?useSSL=false&user=root&password=Mypassword@321"
df.write.mode("overwrite").format("jdbc").option("url", local_host).option("dbtable", "json_data") \
    .option("driver", "com.mysql.cj.jdbc.Driver").save()
df.show()
df.printSchema()
