from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, to_date

import sys


def create_dimension_df(spark: SparkSession):
    def taxi_zone_df():
        return (
            spark.read.option("inferSchema", "true")
            .option("header", "true")
            .csv("s3://de-666ne92p/dim/taxi_zone_dim.csv")
        )

    def date_dim_df():
        return (
            spark.read.option("inferSchema", "true")
            .option("header", "true")
            .csv("s3://de-666ne92p/dim/date_dim.csv")
        )

    def vendor_df():
        schema = StructType(
            [
                StructField("VENDOR_KEY", IntegerType(), True),
                StructField("VENDOR_ID", IntegerType(), True),
                StructField("VENDOR_NAME", StringType(), True),
            ]
        )

        # Your CSV-like data
        data = [(1, 1, "Creative Mobile Technologies, LLC"), (2, 2, "VeriFone Inc.")]

        # Create a DataFrame
        return spark.createDataFrame(data, schema=schema)

    def trip_type_df():
        schema = StructType(
            [
                StructField("TRIP_TYPE_KEY", IntegerType(), True),
                StructField("TRIP_TYPE_ID", IntegerType(), True),
                StructField("TRIP_TYPE_NAME", StringType(), True),
            ]
        )

        data = [(1, 1, "Street-hail"), (2, 2, "Dispatch")]

        return spark.createDataFrame(data, schema=schema)

    def rate_code_df():
        schema = StructType(
            [
                StructField("RATE_CODE_KEY", IntegerType(), True),
                StructField("RATE_CODE_ID", IntegerType(), True),
                StructField("RATE_CODE_EFFECT", StringType(), True),
            ]
        )

        data = [
            (1, 1, "Standard rate"),
            (2, 2, "JFK"),
            (3, 3, "Newark"),
            (4, 4, "Nassau or Westchester"),
            (5, 5, "Negotiated fare"),
            (6, 6, "Group ride"),
        ]

        return spark.createDataFrame(data, schema=schema)

    def payment_type_df():
        schema = StructType(
            [
                StructField("PAYMENT_TYPE_KEY", IntegerType(), True),
                StructField("PAYMENT_TYPE_ID", IntegerType(), True),
                StructField("PAYMENT_TYPE_NAME", StringType(), True),
            ]
        )

        data = [
            (1, 1, "Credit card"),
            (2, 2, "Cash"),
            (3, 3, "No charge"),
            (4, 4, "Dispute"),
            (5, 5, "Unknown"),
            (6, 6, "Voided trip"),
        ]

        return spark.createDataFrame(data, schema=schema)

    return (
        taxi_zone_df(),
        date_dim_df(),
        vendor_df(),
        trip_type_df(),
        rate_code_df(),
        payment_type_df(),
    )


def main(raw_input_path: str, output_path: str):
    # Create a Spark session
    spark = SparkSession.builder.appName("SparkApp").getOrCreate()

    try:
        (
            taxi_zone_df,
            date_dim_df,
            vendor_df,
            trip_type_df,
            rate_code_df,
            payment_type_df,
        ) = create_dimension_df(spark)

        green_trip_raw_data_df = spark.read.format("parquet").load(raw_input_path)

        # Uppercase colume name in green trip data
        for col_name in green_trip_raw_data_df.columns:
            green_trip_raw_data_df = green_trip_raw_data_df.withColumnRenamed(
                col_name, col_name.upper()
            )

        fact_green_trip_data_df = (
            green_trip_raw_data_df.join(
                vendor_df,
                green_trip_raw_data_df["VENDORID"] == vendor_df["VENDOR_ID"],
                "inner",
            )
            .join(
                trip_type_df,
                green_trip_raw_data_df["TRIP_TYPE"] == trip_type_df["TRIP_TYPE_ID"],
                "inner",
            )
            .join(
                rate_code_df,
                green_trip_raw_data_df["RATECODEID"] == rate_code_df["RATE_CODE_ID"],
                "inner",
            )
            .join(
                payment_type_df,
                green_trip_raw_data_df["PAYMENT_TYPE"]
                == payment_type_df["PAYMENT_TYPE_ID"],
                "inner",
            )
            .join(
                taxi_zone_df.alias("taxi_zone_df_pu"),
                green_trip_raw_data_df["PULOCATIONID"]
                == col("taxi_zone_df_pu.LOCATIONID"),
                "inner",
            )
            .join(
                taxi_zone_df.alias("taxi_zone_df_do"),
                green_trip_raw_data_df["DOLOCATIONID"]
                == col("taxi_zone_df_do.LOCATIONID"),
                "inner",
            )
            .join(
                date_dim_df.alias("date_dim_df_pu"),
                to_date(green_trip_raw_data_df["LPEP_PICKUP_DATETIME"])
                == col("date_dim_df_pu.DATE"),
                "inner",
            )
            .join(
                date_dim_df.alias("date_dim_df_do"),
                to_date(green_trip_raw_data_df["LPEP_DROPOFF_DATETIME"])
                == col("date_dim_df_do.DATE"),
                "inner",
            )
            .select(
                col("VENDOR_KEY"),
                col("date_dim_df_pu.DATE_KEY").alias("LPEP_PICKUP_DATE_KEY"),
                col("date_dim_df_do.DATE_KEY").alias("LPEP_DROPOFF_DATE_KEY"),
                col("taxi_zone_df_pu.TAXI_ZONE_KEY").alias("PICK_UP_LOCATION_KEY"),
                col("taxi_zone_df_do.TAXI_ZONE_KEY").alias("DROP_OFF_LOCATION_KEY"),
                col("RATE_CODE_KEY"),
                col("TRIP_TYPE_KEY"),
                col("PAYMENT_TYPE_KEY"),
                col("LPEP_PICKUP_DATETIME"),
                col("LPEP_DROPOFF_DATETIME"),
                col("STORE_AND_FWD_FLAG"),
                col("PASSENGER_COUNT"),
                col("TRIP_DISTANCE"),
                col("FARE_AMOUNT"),
                col("EXTRA"),
                col("MTA_TAX"),
                col("EHAIL_FEE"),
                col("IMPROVEMENT_SURCHARGE"),
                col("TIP_AMOUNT"),
                col("TOLLS_AMOUNT"),
                col("TOTAL_AMOUNT"),
            )
        )

        fact_green_trip_data_df.write.mode("overwrite").parquet(output_path)

    finally:
        # Stop the Spark session
        spark.stop()


if __name__ == "__main__":
    raw_input_path = sys.argv[1]
    output_path = sys.argv[2]
    main(raw_input_path, output_path)
