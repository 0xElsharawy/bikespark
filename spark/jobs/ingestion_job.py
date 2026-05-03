import re
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
    TimestampType,
)

JDBC_URL = "jdbc:clickhouse://clickhouse:8123/default"
JDBC_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"
TABLE_NAME = "raw_trips"
DATA_DIR = "/opt/spark/citibike_2014"
JAR_PATH = "/opt/spark/jars/clickhouse-jdbc-0.9.4-all.jar"

JDBC_PARTITIONS = 4
BATCH_SIZE = 50_000

spark = (
    SparkSession.builder.appName("citibike-ingestion-optimized")
    .config("spark.jars", JAR_PATH)
    .config("spark.cores.max", "4")
    .config("spark.executor.cores", "2")
    .config("spark.executor.memory", "2g")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.files.maxPartitionBytes", "134217728")
    .config("spark.sql.files.openCostInBytes", "134217728")
    .getOrCreate()
)

schema = StructType(
    [
        StructField("tripduration", IntegerType()),
        StructField("starttime", TimestampType()),
        StructField("stoptime", TimestampType()),
        StructField("start station id", IntegerType()),
        StructField("start station name", StringType()),
        StructField("start station latitude", DoubleType()),
        StructField("start station longitude", DoubleType()),
        StructField("end station id", IntegerType()),
        StructField("end station name", StringType()),
        StructField("end station latitude", DoubleType()),
        StructField("end station longitude", DoubleType()),
        StructField("bikeid", IntegerType()),
        StructField("usertype", StringType()),
        StructField("birth year", IntegerType()),
        StructField("gender", IntegerType()),
    ]
)


def clean(col_name: str) -> str:
    return re.sub(r"[ -]+", "_", col_name.strip().lower())


df = (
    spark.read.options(
        header="true",
        nullValue="\\N",
        timestampFormat="yyyy-MM-dd HH:mm:ss",
        mode="PERMISSIVE",
    )
    .schema(schema)
    .csv(f"{DATA_DIR}/*/*.csv")
)

df = df.toDF(*[clean(c) for c in df.columns])

df = df.repartition(JDBC_PARTITIONS)

(
    df.write.format("jdbc")
    .option("driver", JDBC_DRIVER)
    .option("url", JDBC_URL)
    .option("user", "default")
    .option("password", "default")
    .option("dbtable", TABLE_NAME)
    .option("numPartitions", str(JDBC_PARTITIONS))
    .option("batchsize", str(BATCH_SIZE))
    .option("socketTimeout", "300")
    .option("loginTimeout", "30")
    .option("async_insert", "1")
    .mode("append")
    .save()
)

spark.stop()
