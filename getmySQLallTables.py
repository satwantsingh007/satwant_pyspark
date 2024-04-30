from pyspark.sql import *
from pyspark.sql.functions import *
from pandas import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

# importing the data from given list
# tabs = ["employees","customers","offices","orders"]
# for x in tabs:
#     print('importing table - ',x)
#     host = "jdbc:mysql://satwant-db.cxq2iwgsy5fo.ap-south-1.rds.amazonaws.com:3306/satwant_db?useSSL=false&user=myuser&password=mypassword"
#     df = spark.read.format("jdbc").option("url",host)\
#         .option("dbtable",x)\
#         .option("driver","com.mysql.cj.jdbc.Driver").load()
#     df.show()

# data = r'C:\bigdata\drivers-20230725T062459Z-001\drivers\bank-full.csv'
# df = spark.read.csv(path=data, header=True, inferSchema=True, sep=';')

# df.createOrReplaceTempView('bank')
# res = spark.sql("select * from bank where marital = 'single' and balance > 20000")
# res = df.where((col("marital") == "married") & (col("balance") > 25000))
# host = "jdbc:mysql://satwant-db.cxq2iwgsy5fo.ap-south-1.rds.amazonaws.com:3306/satwant_db?useSSL=false&user=myuser&password=mypassword"
# res.write.format("jdbc").option("url", host) \
#     .option("dbtable", 'satwant_bank_1') \
#     .option("driver", "com.mysql.cj.jdbc.Driver").save()


# Define JDBC connection properties
# jdbc_url = "jdbc:mysql://localhost:3306/satwant_venu?useSSL=false"
# connection_properties = {
#     "user": "root",
#     "password": "Mypassword@321",
#     "driver": "com.mysql.cj.jdbc.Driver"
# }
# data = r'C:\bigdata\drivers-20230725T062459Z-001\drivers\bank-full.csv'
# df = spark.read.csv(path=data, header=True, inferSchema=True, sep=';')
# df.write.jdbc(url=jdbc_url, table="satwant_bank", mode="overwrite", properties=connection_properties)

data = r'C:\bigdata\drivers-20230725T062459Z-001\drivers\empmysql.csv'
df = spark.read.csv(path=data,header=True,inferSchema=True)
# df.show()
# df.createOrReplaceTempView('emp')
# res = spark.sql("select *, row_number() over (order by sal desc) row_num, rank() over (order by sal desc) rnk, dense_rank() over (order by sal desc) drank from emp ")
# res.show()

win = Window.orderBy(col("sal").desc())
rs = df.withColumn('drank',dense_rank().over(win)).withColumn('rnk',rank().over(win))\
    .withColumn('row_num',row_number().over(win)).withColumn('percent',percent_rank().over(win))\
    .withColumn('ntile',ntile(3).over(win)).withColumn('lead',lead(col("sal")).over(win))\
    .withColumn('lag',lag(col("sal")).over(win))
rs.show()
