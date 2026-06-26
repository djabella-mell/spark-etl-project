from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_movies(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["movieId"])
        .filter(F.col("movieId").isNotNull())
        .filter(F.col("title").isNotNull())
        .filter(F.col("genres").isNotNull())
    )


def clean_ratings(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates()
        .filter(F.col("userId").isNotNull())
        .filter(F.col("movieId").isNotNull())
        .filter(F.col("rating").isNotNull())
        .filter((F.col("rating") >= 0.5) & (F.col("rating") <= 5.0))
        .withColumn("rating_timestamp", F.to_timestamp(F.from_unixtime(F.col("timestamp"))))
        .withColumn("rating_year", F.year(F.col("rating_timestamp")))
    )


def clean_tags(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates()
        .filter(F.col("userId").isNotNull())
        .filter(F.col("movieId").isNotNull())
        .filter(F.col("tag").isNotNull())
        .filter(F.length(F.trim(F.col("tag"))) > 0)
        .withColumn("tag_timestamp", F.to_timestamp(F.from_unixtime(F.col("timestamp"))))
        .withColumn("tag_year", F.year(F.col("tag_timestamp")))
    )


def clean_links(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["movieId"])
        .filter(F.col("movieId").isNotNull())
        .filter(F.col("imdbId").isNotNull())
    )


def write_silver(df: DataFrame, path: str, partition_by: str | None = None) -> None:
    writer = df.write.mode("overwrite")

    if partition_by:
        writer = writer.partitionBy(partition_by)

    writer.parquet(path)