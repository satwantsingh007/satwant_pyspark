from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.streaming import *

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
ssc = StreamingContext(spark.sparkContext, 10)

lines = ssc.socketTextStream("ec2-35-154-90-103.ap-south-1.compute.amazonaws.com", 1234)
#get Live data from ***localhost*** server ..**9999 port number*** get socket/ terminal/command prompt
#get data from terminal

def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

local_host = "jdbc:mysql://localhost:3306/satwant_venu?useSSL=false&user=root&password=Mypassword@321"

def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        df = rdd.map(lambda x: x.split(",")).toDF(["name", "age", "city"])
        df.show()
        hydd = df.where(col("city") == "hyd")
        dellf = df.where(col("city") == "del")
        hydd.write.mode("append").format("jdbc").option("url", local_host).option("dbtable", "hyd_stream").option("driver", "com.mysql.cj.jdbc.Driver").save()
        dellf.write.mode("append").format("jdbc").option("url", local_host).option("dbtable", "del_stream").option("driver", "com.mysql.cj.jdbc.Driver").save()
    except:
        pass


lines.foreachRDD(process)

# lines.pprint()
ssc.start()
ssc.awaitTermination()
