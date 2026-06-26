from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType


MOVIES_SCHEMA = StructType([
    StructField("movieId", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True),
])

RATINGS_SCHEMA = StructType([
    StructField("userId", IntegerType(), False),
    StructField("movieId", IntegerType(), False),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", LongType(), True),
])

TAGS_SCHEMA = StructType([
    StructField("userId", IntegerType(), False),
    StructField("movieId", IntegerType(), False),
    StructField("tag", StringType(), True),
    StructField("timestamp", LongType(), True),
])

LINKS_SCHEMA = StructType([
    StructField("movieId", IntegerType(), False),
    StructField("imdbId", StringType(), True),
    StructField("tmdbId", IntegerType(), True),
])


def read_csv_with_schema(
    spark: SparkSession,
    path: str,
    schema: StructType
) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(path)
    )


def load_movies(spark: SparkSession, base_path: str) -> DataFrame:
    return read_csv_with_schema(spark, f"{base_path}/movies.csv", MOVIES_SCHEMA)


def load_ratings(spark: SparkSession, base_path: str) -> DataFrame:
    return read_csv_with_schema(spark, f"{base_path}/ratings.csv", RATINGS_SCHEMA)


def load_tags(spark: SparkSession, base_path: str) -> DataFrame:
    return read_csv_with_schema(spark, f"{base_path}/tags.csv", TAGS_SCHEMA)


def load_links(spark: SparkSession, base_path: str) -> DataFrame:
    return read_csv_with_schema(spark, f"{base_path}/links.csv", LINKS_SCHEMA)