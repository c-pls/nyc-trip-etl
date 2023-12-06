from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, to_date

import sys


def create_dimention_df(spark: SparkSession):
    def taxi_zone_df():
        return (
            spark.read.option("inferSchema", "true")
            .option("header", "true")
            .csv("s3://de-666ne92p/dim/taxi_zone_dim.csv")
            .cache()
        )

    def date_dim_df():
        return (
            spark.read.option("inferSchema", "true")
            .option("header", "true")
            .csv("s3://de-666ne92p/dim/date_dim.csv")
            .cache()
        )

    def hvfhs_license_dim_df():
        schema = StructType(
            [
                StructField("Hvfhs_license_key", IntegerType(), True),
                StructField("Hvfhs_license_id", StringType(), True),
                StructField("Hvfhs_license_name", StringType(), True),
            ]
        )

        data = [
            (1, "HV0002", "Juno"),
            (2, "HV0003", "Uber"),
            (3, "HV0004", "Via"),
            (4, "HV0005", "Lyft"),
        ]

        return spark.createDataFrame(data, schema=schema)

    return (taxi_zone_df(), date_dim_df(), hvfhs_license_dim_df())


def main(raw_input_path: str, output_path: str):
    # Create a Spark session
    spark = SparkSession.builder.appName("SparkApp").getOrCreate()

    try:
        (
            taxi_zone_df,
            date_dim_df,
            hvfhs_license_dim_df,
        ) = create_dimention_df(spark)

        fhvhv_trip_raw_data_df = spark.read.format("parquet").load(raw_input_path)

        # Uppercase colume name in fhvhv trip data
        for col_name in fhvhv_trip_raw_data_df.columns:
            fhvhv_trip_raw_data_df = fhvhv_trip_raw_data_df.withColumnRenamed(
                col_name, col_name.upper()
            )

        fact_fhvhv_trip_data_df = (
            fhvhv_trip_raw_data_df.join(
                hvfhs_license_dim_df,
                fhvhv_trip_raw_data_df["HVFHS_LICENSE_NUM"]
                == hvfhs_license_dim_df["HVFHS_LICENSE_ID"],
                "inner",
            )
            .join(
                taxi_zone_df.alias("taxi_zone_df_pu"),
                fhvhv_trip_raw_data_df["PULOCATIONID"]
                == col("taxi_zone_df_pu.LOCATIONID"),
                "inner",
            )
            .join(
                taxi_zone_df.alias("taxi_zone_df_do"),
                fhvhv_trip_raw_data_df["DOLOCATIONID"]
                == col("taxi_zone_df_do.LOCATIONID"),
                "inner",
            )
            .join(
                date_dim_df.alias("date_dim_df_rd"),
                to_date(fhvhv_trip_raw_data_df["REQUEST_DATETIME"])
                == col("date_dim_df_rd.DATE"),
                "left",
            )
            .join(
                date_dim_df.alias("date_dim_df_os"),
                to_date(fhvhv_trip_raw_data_df["ON_SCENE_DATETIME"])
                == col("date_dim_df_os.DATE"),
                "left",
            )
            .join(
                date_dim_df.alias("date_dim_df_pu"),
                to_date(fhvhv_trip_raw_data_df["PICKUP_DATETIME"])
                == col("date_dim_df_pu.DATE"),
                "left",
            )
            .join(
                date_dim_df.alias("date_dim_df_do"),
                to_date(fhvhv_trip_raw_data_df["DROPOFF_DATETIME"])
                == col("date_dim_df_do.DATE"),
                "left",
            )
            .select(
                col("HVFHS_LICENSE_KEY"),
                col("taxi_zone_df_pu.TAXI_ZONE_KEY").alias("PICK_UP_LOCATION_KEY"),
                col("taxi_zone_df_do.TAXI_ZONE_KEY").alias("DROP_OFF_LOCATION_KEY"),
                col("date_dim_df_rd.DATE_KEY").alias("REQUEST_DATE_KEY"),
                col("date_dim_df_os.DATE_KEY").alias("ON_SCENE_DATE_KEY"),
                col("date_dim_df_pu.DATE_KEY").alias("PICKUP_DATE_KEY"),
                col("date_dim_df_do.DATE_KEY").alias("DROPOFF_DATE_KEY"),
                col("PICKUP_DATETIME"),
                col("DROPOFF_DATETIME"),
                col("REQUEST_DATETIME"),
                col("ON_SCENE_DATETIME"),
                col("DISPATCHING_BASE_NUM"),
                col("ORIGINATING_BASE_NUM"),
                col("TRIP_MILES"),
                col("TRIP_TIME"),
                col("BASE_PASSENGER_FARE"),
                col("TOLLS"),
                col("BCF"),
                col("SALES_TAX"),
                col("CONGESTION_SURCHARGE"),
                col("AIRPORT_FEE"),
                col("TIPS"),
                col("DRIVER_PAY"),
                col("SHARED_REQUEST_FLAG"),
                col("SHARED_MATCH_FLAG"),
                col("ACCESS_A_RIDE_FLAG"),
                col("WAV_REQUEST_FLAG"),
                col("WAV_MATCH_FLAG"),
            )
        )

        fact_fhvhv_trip_data_df = fact_fhvhv_trip_data_df.repartition(10)

        fact_fhvhv_trip_data_df.write.mode("overwrite").parquet(output_path)

    finally:
        spark.stop()


if __name__ == "__main__":
    raw_input_path = sys.argv[1]
    output_path = sys.argv[2]
    main(raw_input_path, output_path)
