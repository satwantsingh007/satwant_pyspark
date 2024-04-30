from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

data = r'C:\bigdata\drivers-20230725T062459Z-001\drivers\date_differ_timezone.csv'
df = spark.read.format("csv").option("header", "true").load(data)
df = df.withColumn("date_Time",concat(col("dateofbirth"),col("birthTime")))
rs = df.drop("email","timezone").withColumn("ts",to_timestamp(col("date_Time"),'yyyy-MM-ddHH:mm'))\
    .withColumn('ux',unix_timestamp(col("ts")))\
    .withColumn("est_time", to_utc_timestamp(from_utc_timestamp(col("ts"), "EST"), "IST"))
# rs.printSchema()
# rs.show(truncate=False)
print(df.count())
print(rs.count())
