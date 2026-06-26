from src.utils import create_spark_session


def main():
    spark = create_spark_session()
    print("Spark session created successfully")
    print(f"Spark version: {spark.version}")
    spark.stop()


if __name__ == "__main__":
    main()