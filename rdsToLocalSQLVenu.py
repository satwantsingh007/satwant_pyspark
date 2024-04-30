from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

qry = "(select table_name from information_schema.TABLES where table_schema='satwant_db') t"
host = "jdbc:mysql://satwant-db.cxq2iwgsy5fo.ap-south-1.rds.amazonaws.com:3306/satwant_db?useSSL=false&user=myuser&password=mypassword"
df = spark.read.format("jdbc").option("url",host)\
        .option("dbtable",qry)\
        .option("driver","com.mysql.cj.jdbc.Driver").load()
df.show()
df.printSchema()
tabs = [x[0] for x in df.rdd.collect()]
for y in tabs:
    print('importing table - ', y)
    jdbc_url = "jdbc:mysql://localhost:3306/satwant_venu?useSSL=false"
    connection_properties = {
        "user": "root",
        "password": "Mypassword@321",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    df.write.jdbc(url=jdbc_url, table=y, mode="append", properties=connection_properties)
