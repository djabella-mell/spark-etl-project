from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def top_rated_movies(ratings_df: DataFrame, min_votes: int = 50) -> DataFrame:
    """
    Return the top rated movies based on average rating,
    keeping only movies with at least min_votes ratings.
    """
    return (
        ratings_df
        .groupBy("movieId")
        .agg(
            F.count("*").alias("vote_count"),
            F.round(F.avg("rating"), 2).alias("avg_rating")
        )
        .filter(F.col("vote_count") >= min_votes)
        .orderBy(F.desc("avg_rating"), F.desc("vote_count"))
    )
def top_rated_movies_with_titles(
    ratings_df: DataFrame,
    movies_df: DataFrame,
    min_votes: int = 50
) -> DataFrame:
    """
    Return the top rated movies with their titles using a join
    between ratings and movies.
    """
    movie_stats = top_rated_movies(ratings_df, min_votes)

    return (
        movie_stats
        .join(movies_df, on="movieId", how="inner")
        .select("movieId", "title", "genres", "vote_count", "avg_rating")
        .orderBy(F.desc("avg_rating"), F.desc("vote_count"))
    )
def top_movies_by_genre(
    ratings_df: DataFrame,
    movies_df: DataFrame,
    min_votes: int = 20,
    top_n: int = 3
) -> DataFrame:
    """
    Return the top N rated movies for each genre using a window function.
    """
    movie_stats = top_rated_movies_with_titles(ratings_df, movies_df, min_votes)

    movies_by_genre = (
        movie_stats
        .withColumn("genre", F.explode(F.split(F.col("genres"), "\\|")))
        .filter(F.col("genre") != "(no genres listed)")
    )

    genre_window = Window.partitionBy("genre").orderBy(
        F.desc("avg_rating"),
        F.desc("vote_count")
    )

    return (
        movies_by_genre
        .withColumn("rank", F.row_number().over(genre_window))
        .filter(F.col("rank") <= top_n)
        .select("genre", "rank", "movieId", "title", "vote_count", "avg_rating")
        .orderBy("genre", "rank")
    )

def write_gold(df: DataFrame, path: str) -> None:
    """
    Write a Gold dataframe as CSV.
    """
    (
        df.write
        .mode("overwrite")
        .option("header", True)
        .csv(path)
    )