from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "Spark ETL Pipeline") -> SparkSession:
    """
    Create and return a local SparkSession.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
