import logging
import sys

from sedona.spark import SedonaContext

from data_transformations.raster import zonal_stats

LOG_FILENAME = 'project.log'
APP_NAME = "Raster Pipeline: Zonal Statistics"

if __name__ == '__main__':
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)

    if len(sys.argv) != 4:
        logging.warning("Raster parquet path, vector path, and output path are required")
        sys.exit(1)

    raster_parquet_path = sys.argv[1]
    vector_path = sys.argv[2]
    output_path = sys.argv[3]

    spark = SedonaContext.builder().appName(APP_NAME).master("local").getOrCreate()
    logging.info("Application Initialized: " + spark.sparkContext.appName)
    zonal_stats.run(spark, raster_parquet_path, vector_path, output_path)
    logging.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
