from pyspark.sql.functions import col

from pyspark import Row

from pyspark.sql import DataFrameWriter, DataFrame, DataFrameReader,Row
from pyspark.sql.dataframe import DataFrameWriter,DataStreamWriter
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_TOPIC_NAME_CONS = "testtopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

mongodb_host_name = "localhost"
mongodb_port_no = "27017"
mongodb_user_name = "admin"
mongodb_password = "admin"
mongodb_database_name = "EmployeeData"
mongodb_collection_name = "EmployeeDetails"





# if __name__ == "__main__":
#     print("Real-Time Data Pipeline Started ...")

spark = SparkSession \
        .builder \
        .appName("Real-Time Data Pipeline Demo") \
        .master("local[*]") \
        .config("spark.jars", "file:///G://spark-sql-kafka-0-10_2.11-2.4.0.jar,file:///G://kafka-clients-1.1.0.jar") \
        .config("spark.executor.extraClassPath", "file:///G://spark-sql-kafka-0-10_2.11-2.4.0.jar:file:///G://kafka-clients-1.1.0.jar") \
        .config("spark.executor.extraLibrary", "file:///G://spark-sql-kafka-0-10_2.11-2.4.0.jar:file:///G://kafka-clients-1.1.0.jar") \
        .config("spark.driver.extraClassPath", "file:///G://spark-sql-kafka-0-10_2.11-2.4.0.jar:file:///G://kafka-clients-1.1.0.jar") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.0") \
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("out of library")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "testtopic") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


print("Printing Schema of transaction_detail_df: ")
df.printSchema()

schema = StructType(
    [
        StructField("employeeId", StringType()),
        StructField("name", StringType()),
        StructField("salary", StringType())

    ]
)


df1 = df.select(from_json(col("value").cast("string"), schema).alias("Employee Details"))
df2 = df1.select("Employee Details.*")

df2.printSchema()
spark_mongodb_output_uri = "mongodb://" + mongodb_user_name + ":" + mongodb_password + "@" + mongodb_host_name + ":" + mongodb_port_no + "/" + mongodb_database_name + "." + mongodb_collection_name
print(spark_mongodb_output_uri)


def foreach_batch_function(df2, epoch_id):
    print("inside function")
    df2.show()
    df2 \
        .write \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .mode("append") \
        .option("uri", spark_mongodb_output_uri).option("database", mongodb_database_name) \
        .option("collection", mongodb_collection_name) \
        .save()


df2.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()


