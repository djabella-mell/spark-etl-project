from src.utils import create_spark_session
from src.ingestion import load_movies, load_ratings, load_tags, load_links, validate_dataframe


RAW_MOVIELENS_PATH = "data/raw/ml-latest-small"


def main():
    spark = create_spark_session()

    movies_df = load_movies(spark, RAW_MOVIELENS_PATH)
    ratings_df = load_ratings(spark, RAW_MOVIELENS_PATH)
    tags_df = load_tags(spark, RAW_MOVIELENS_PATH)
    links_df = load_links(spark, RAW_MOVIELENS_PATH)

    validate_dataframe(movies_df, "movies")
    validate_dataframe(ratings_df, "ratings")
    validate_dataframe(tags_df, "tags")
    validate_dataframe(links_df, "links")

    spark.stop()


if __name__ == "__main__":
    main()