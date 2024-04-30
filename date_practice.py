from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

spark.conf.set("spark.sql.session.timeZone", "CST")
possible_formats = ['dd-MM-yyyy', "dd-MMM-yyyy", "dd/MMM/yyyy", "d-MMM-yyyy", "dd/mm/yyyy", "yyyy-MMM-d", "yyyy-MM-dd"]
data = r'C:\bigdata\drivers-20230725T062459Z-001\drivers\datemultiformat.csv'
df = spark.read.csv(path=data, header=True, inferSchema=True)
def dateto(col, format=possible_formats):
    return coalesce(*[to_date(col, f) for f in format])
df1 = df.withColumn("hiredate", dateto(col("hiredate")))\
    .withColumn("prohibition_period",date_add(col("hiredate"),100))\
    .withColumn("100daysbefore",date_sub(col("hiredate"),100))
df2 = df.drop("empno","job","mgr","deptno").withColumn("hiredate",dateto(col("hiredate"))) \
    .withColumn("dtformat2", date_format(col("hiredate"), "MMM/dd/yyy")) \
    .withColumn("dtformat3", date_format(col("hiredate"), "EEE")) \
    .withColumn("dtformat4", date_format(col("hiredate"), "EEE/dd/MMMM/yyyy")) \
    .withColumn("dtformat5", date_format(col("hiredate"),"yyyy-MMMM-dd-EEEE"))\
    .withColumn("dtformat1", date_format(col("hiredate"), "dd/MMM/yyyy"))
df3 = df.withColumn("hiredate", dateto(col("hiredate")))\
    .withColumn("current_date",current_date())\
    .withColumn("ts",current_timestamp())\
    .withColumn("difference_date",datediff(col("current_date"),col("hiredate")))

df3.show(truncate=False)
