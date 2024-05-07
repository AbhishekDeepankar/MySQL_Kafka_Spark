from pyspark.sql import SparkSession
from spark_consumer.helper_functions.get_spark import get_spark
from pyspark.sql.types import *
import pyspark.sql.functions as fx
import os
import kafka_mysql.Config.constants as c

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'


def spark_sub(
    spark: SparkSession,
    host: list[str],
    topic: list[str],
    offset: str
) -> str:

    hosts = ",".join(host)
    topics = ",".join(topic)

    my_schema = StructType([
        StructField("country_name", StringType()),
        StructField("date", StringType()),
        StructField("parameter", StringType()),
        StructField("product", StringType()),
        StructField("value", DoubleType()),
        StructField("unit", StringType()),
        StructField("created_on", TimestampType()),
        StructField("updated_on", TimestampType()),
    ])

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", hosts)
        .option("subscribe", topics)
        .option("startingOffsets", offset)
        .load()
    )

    value_df = (
        df.select(fx.from_json(fx.col("value").cast("string"), my_schema).alias("value"))
    )

    data_df = value_df.select(
        "value.*",
        fx.when(
            (fx.col("value.product").ilike('%Hydro%'))
            | (fx.col("value.product").ilike('%Wind%'))
            | (fx.col("value.product").ilike('%solar%'))
            | (fx.col("value.product").ilike('%Geothermal%'))
            | ((fx.col("value.product").ilike('%Renewable%'))
                & (~fx.col("value.product").ilike('%Non%'))),
            fx.lit("Renewable Energy")
        ).otherwise(fx.lit("Non-Renewable Energy")).alias("Classification")
    )

    query = (
        data_df.selectExpr("*")
        .writeStream.format("console")
        .option("truncate", False)
        .start()
    )

    query.awaitTermination()

    return '0'


if __name__ == "__main__":
    sparkSession = get_spark(
        app_name="kafkaConsumer",
        master_info="local[2]"

    )

    spark_sub(
        spark=sparkSession,
        host=c.pipeln_broker,
        topic=c.pipeln_topic,
        offset='earliest'
    )
