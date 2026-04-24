import logging
import sys

from sedona.spark import SedonaContext

from data_transformations.raster import ingest

LOG_FILENAME = 'project.log'
APP_NAME = "Raster Pipeline: Ingest"

if __name__ == '__main__':
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)

    if len(sys.argv) != 3:
        logging.warning("Raster file path and output path are required")
        sys.exit(1)

    raster_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SedonaContext.builder().appName(APP_NAME).master("local").getOrCreate()
    logging.info("Application Initialized: " + spark.sparkContext.appName)
    ingest.run(spark, raster_path, output_path)
    logging.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
