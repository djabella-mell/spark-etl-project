from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast


def broadcast_movies(movies_df: DataFrame) -> DataFrame:
    """
    Broadcast the movies DataFrame to optimize joins with ratings.
    """
    return broadcast(movies_df)