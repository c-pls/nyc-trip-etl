from pyspark.sql import SparkSession


def main():
    # Create a Spark session
    spark = SparkSession.builder.appName("SparkApp").getOrCreate()

    try:
        # Create a simple DataFrame with two columns: "ID" and "Value"
        data = [(1, "A"), (2, "B"), (3, "C")]
        columns = ["ID", "Value"]
        df = spark.createDataFrame(data, columns)

        # Show the contents of the DataFrame
        df.show()
    finally:
        # Stop the Spark session
        spark.stop()


if __name__ == "__main__":
    main()
