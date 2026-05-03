from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, current_timestamp
from pyspark.sql.types import *
import re

JDBC_URL = "jdbc:clickhouse://clickhouse:8123/default"
TABLE_NAME = "raw_trips"
DATA_DIR = "/opt/spark/citibike_2014"

spark = (
    SparkSession.builder.appName("citibike-ingestion")
    .config("spark.jars", "/opt/spark/jars/clickhouse-jdbc-0.9.4-all.jar")
    .getOrCreate()
)

schema = StructType(
    [
        StructField("tripduration", IntegerType()),
        StructField("starttime", TimestampType()),
        StructField("stoptime", TimestampType()),
        StructField("start_station_id", IntegerType()),
        StructField("end_station_id", IntegerType()),
        StructField("start_station_name", StringType()),
        StructField("end_station_name", StringType()),
        StructField("start_station_latitude", DoubleType()),
        StructField("start_station_longitude", DoubleType()),
        StructField("end_station_latitude", DoubleType()),
        StructField("end_station_longitude", DoubleType()),
        StructField("bikeid", IntegerType()),
        StructField("usertype", StringType()),
        StructField("birth_year", IntegerType()),
        StructField("gender", IntegerType()),
    ]
)

df = spark.read.options(header="True").schema(schema).csv(f"{DATA_DIR}/*/*.csv")


def clean(col_name: str) -> str:
    col_name = col_name.strip().lower()
    col_name = re.sub(r"[ -]+", "_", col_name)
    return col_name


df = df.toDF(*[clean(c) for c in df.columns])


# df = (
#     df.withColumn("tripduration", col("tripduration").cast("int"))
#     .withColumn("starttime", col("starttime").cast("timestamp"))
#     .withColumn("stoptime", col("stoptime").cast("timestamp"))
#     .withColumn("start_station_id", col("start_station_id").cast("int"))
#     .withColumn("end_station_id", col("end_station_id").cast("int"))
#     .withColumn("start_station_name", col("start_station_name").cast("string"))
#     .withColumn("end_station_name", col("end_station_name").cast("string"))
#     .withColumn("start_station_latitude", col("start_station_latitude").cast("double"))
#     .withColumn(
#         "start_station_longitude", col("start_station_longitude").cast("double")
#     )
#     .withColumn("end_station_latitude", col("end_station_latitude").cast("double"))
#     .withColumn("end_station_longitude", col("end_station_longitude").cast("double"))
#     .withColumn("bikeid", col("bikeid").cast("int"))
#     .withColumn("usertype", col("usertype").cast("string"))
#     .withColumn("birth_year", col("birth_year").cast("int"))
#     .withColumn("gender", col("gender").cast("int"))
# )

df = df.repartition(8)

df.write.format("jdbc").option("driver", "com.clickhouse.jdbc.ClickHouseDriver").option(
    "url", "jdbc:clickhouse://clickhouse:8123/default"
).option("user", "default").option("password", "default").option(
    "dbtable", "raw_trips"
).option("batchsize", "10000").option("numPartitions", "8").mode("append").save()

spark.stop()
