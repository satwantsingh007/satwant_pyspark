from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

host = "jdbc:mysql://satwantdb.cxq2iwgsy5fo.ap-south-1.rds.amazonaws.com:3306/satwant_db?useSSL=false&user=myuser&password=mypassword"
df = spark.read.format('jdbc').option("url",host).option("dbtable","emp").option("driver","com.mysql.cj.jdbc.Driver").load()
# df.show()

res = df.na.fill(0).withColumn('full_sal',col('sal')+col('comm'))\
    .withColumn('today',current_date())\
    .withColumn('experience',datediff(col('today'),col('hiredate')))\
    .withColumn('total_experience',col('today')-col('hiredate'))\
    .withColumn('total_experience',col('total_experience').cast(StringType()))
# res.show(truncate=False)
# res.printSchema()

# op = r'C:\bigdata\drivers-20230725T062459Z-001\drivers\Output\testing.csv'
# res.write.mode('overwrite').option('header','true').format('csv').save(op)
# res.write.mode('append').format('jdbc').option("url",host).option("dbtable","emp_testing").option("driver","com.mysql.cj.jdbc.Driver").save()

sfOptions = {
  "sfURL" : "wszhsva-qo35058.snowflakecomputing.com",
  "sfUser" : "SATWANTSINGH9451",
  "sfPassword" : "Waheguru@321",
  "sfDatabase" : "satwantdb",
  "sfSchema" : "public",
  "sfWarehouse" : "COMPUTE_WH"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

res.write.mode('append').format(SNOWFLAKE_SOURCE_NAME) \
  .options(**sfOptions) \
  .option("dbtable",  "emp_testing") \
  .save()

