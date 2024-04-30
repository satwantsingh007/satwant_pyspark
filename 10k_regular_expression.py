from pyspark.sql import *
from pyspark.sql.functions import *
import re

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

data = r'C:\bigdata\drivers-20230725T062459Z-001\drivers\10000Records.csv'
df = spark.read.csv(data, header=True, inferSchema=True)
# df.show()
# to clean the data as like I had cleaned the column names we use regular expression
col = [re.sub("[^a-zA-Z0-9]", "", x.lower()) for x in df.columns]
df = df.toDF(*col)

df.createOrReplaceTempView('tab')
res = spark.sql("Select * from tab")
res.show(truncate=False)
