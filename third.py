from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

data = r'C:\bigdata\drivers-20230725T062459Z-001\drivers\us-500.csv'
df = spark.read.csv(path=data,header=True,inferSchema=True)
df = df.drop("company_name","address","county","web")
rs = df.withColumn("phone1",regexp_replace(col("phone1"),'-',''))\
    .withColumn("phone2",regexp_replace(col('phone2'),'-',''))\
    .withColumn("email",regexp_replace(col('email'),'[_,@,.]',''))
rs.show(truncate=False)
