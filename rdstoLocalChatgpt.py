from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").master("local[*]").getOrCreate()

# Define JDBC URL and connection properties for RDS
rds_host = "jdbc:mysql://satwant-db.cxq2iwgsy5fo.ap-south-1.rds.amazonaws.com:3306/satwant_db?useSSL=false"
rds_properties = {
    "user": "myuser",
    "password": "mypassword",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read table names from RDS
table_names_df = spark.read.format("jdbc")\
    .options(url=rds_host, dbtable="(SELECT table_name FROM information_schema.tables WHERE table_schema='satwant_db') as t",
                                                   **rds_properties).load()

# Fetch table names
table_names = [row[0] for row in table_names_df.collect()]

# Write data from each table to local MySQL
for table_name in table_names:
    print(f"Importing data from table: {table_name}")
    # Read data from RDS table
    table_df = spark.read.format("jdbc").options(url=rds_host, dbtable=table_name, **rds_properties).load()

    # Define JDBC URL and connection properties for local MySQL
    local_host = "jdbc:mysql://localhost:3306/satwant_venu?useSSL=false"
    local_properties = {
        "user": "root",
        "password": "Mypassword@321",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    # Write data to local MySQL table
    table_df.write.jdbc(url=local_host, table=table_name, mode="append", properties=local_properties)
