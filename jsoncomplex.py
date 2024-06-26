from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

data = r'C:\bigdata\drivers-20230725T062459Z-001\drivers\world_bank.json'
df = spark.read.json(data,mode='DROPMALFORMED')
# df.show(truncate=False)

def read_nested_json(df):
    column_list = []
    for column_name in df.schema.names:
        if isinstance(df.schema[column_name].dataType, ArrayType):
            df = df.withColumn(column_name, explode(column_name))
            column_list.append(column_name)
        elif isinstance(df.schema[column_name].dataType, StructType):
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)
    df = df.select(column_list)
    cols = [re.sub('[^a-zA-Z0-9]', "", c.lower()) for c in df.columns]
    df = df.toDF(*cols)
    return df

def flatten(df):
    read_nested_json_flag = True
    while read_nested_json_flag:
        df = read_nested_json(df)
        read_nested_json_flag = False
        for column_name in df.schema.names:
            if isinstance(df.schema[column_name].dataType, ArrayType):
                read_nested_json_flag = True
            elif isinstance(df.schema[column_name].dataType, StructType):
                read_nested_json_flag = True
    return df;

ndf=flatten(df)
ndf.printSchema()
