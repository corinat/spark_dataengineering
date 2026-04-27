import argparse
import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sedona.spark import SedonaContext

logging.basicConfig(level=logging.INFO)


def main(raster_parquet_path: str, output_tif: str) -> None:
    spark = SparkSession.builder.appName("visualize_raster_band").getOrCreate()
    sedona = SedonaContext.create(spark)

    logging.info("Reading raster Parquet from: %s", raster_parquet_path)
    df = sedona.read.parquet(raster_parquet_path)

    row = df.select(
        F.expr("RS_AsGeoTiff(raster)").alias("geotiff_bytes"),
    ).first()

    if row is None:
        logging.error("No rows found in %s", raster_parquet_path)
        sys.exit(1)

    Path(output_tif).parent.mkdir(parents=True, exist_ok=True)
    Path(output_tif).write_bytes(bytes(row["geotiff_bytes"]))
    logging.info("Saved GeoTIFF to: %s", output_tif)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Visualize a raster band from ingested Parquet")
    parser.add_argument("raster_parquet_path", help="Path to the raster Parquet directory")
    parser.add_argument("output_tif", help="Path to write the raster as GeoTIFF")
    args = parser.parse_args()

    main(args.raster_parquet_path, args.output_tif)

