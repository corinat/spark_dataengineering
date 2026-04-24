from sedona.spark import SedonaContext
from pyspark.sql import DataFrame, SparkSession

METERS_PER_MILE = 1609.344


def compute_distance(spark: SparkSession, dataframe: DataFrame) -> DataFrame:
    dataframe.createOrReplaceTempView("trips")

    return spark.sql(f"""
        SELECT
            *,
            ST_DistanceSphere(
                ST_Point(start_station_longitude, start_station_latitude),
                ST_Point(end_station_longitude,   end_station_latitude)
            ) / {METERS_PER_MILE} AS distance
        FROM trips
    """)


def run(spark: SparkSession, input_dataset_path: str, transformed_dataset_path: str) -> None:
    sedona = SedonaContext.create(spark)
    input_dataset = sedona.read.parquet(input_dataset_path)
    input_dataset.show()

    dataset_with_distances = compute_distance(sedona, input_dataset)
    dataset_with_distances.show()

    dataset_with_distances.write.parquet(transformed_dataset_path, mode='append')
