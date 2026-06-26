from src.utils import create_spark_session
from src.ingestion import load_movies, load_ratings, load_tags, load_links, validate_dataframe
from src.transform import clean_movies, clean_ratings, clean_tags, clean_links, write_silver
from src.analysis import top_rated_movies, write_gold


RAW_MOVIELENS_PATH = "data/raw/ml-latest-small"
SILVER_PATH = "data/silver"


def main():
    spark = create_spark_session()

    movies_df = load_movies(spark, RAW_MOVIELENS_PATH)
    ratings_df = load_ratings(spark, RAW_MOVIELENS_PATH)
    tags_df = load_tags(spark, RAW_MOVIELENS_PATH)
    links_df = load_links(spark, RAW_MOVIELENS_PATH)

    validate_dataframe(movies_df, "movies_raw")
    validate_dataframe(ratings_df, "ratings_raw")
    validate_dataframe(tags_df, "tags_raw")
    validate_dataframe(links_df, "links_raw")

    movies_clean = clean_movies(movies_df)
    ratings_clean = clean_ratings(ratings_df)
    tags_clean = clean_tags(tags_df)
    links_clean = clean_links(links_df)

    validate_dataframe(movies_clean, "movies_silver")
    validate_dataframe(ratings_clean, "ratings_silver")
    validate_dataframe(tags_clean, "tags_silver")
    validate_dataframe(links_clean, "links_silver")

    write_silver(movies_clean, f"{SILVER_PATH}/movies")
    write_silver(ratings_clean, f"{SILVER_PATH}/ratings", partition_by="rating_year")
    write_silver(tags_clean, f"{SILVER_PATH}/tags", partition_by="tag_year")
    write_silver(links_clean, f"{SILVER_PATH}/links")

    # Gold - Analysis 1
    top_movies = top_rated_movies(ratings_clean, min_votes=50)

    print("\n===== TOP RATED MOVIES =====")
    top_movies.show(10, truncate=False)

    write_gold(top_movies, "data/gold/top_rated_movies")

    spark.stop()


if __name__ == "__main__":
    main()