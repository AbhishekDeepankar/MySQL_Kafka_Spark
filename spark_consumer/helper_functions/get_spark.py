from pyspark.sql import SparkSession


def get_spark(
        app_name: str,
        master_info: str
) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).master(master_info).getOrCreate()

    return spark
