import logging
import sys

from sedona.spark import SedonaContext

from data_transformations.raster import batch_ingest_rasters

LOG_FILENAME = 'project.log'
APP_NAME = "Raster Pipeline: Batch Ingest"

if __name__ == '__main__':
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)

    if len(sys.argv) != 4:
        logging.warning("Usage: raster_batch_ingest.py <input_dir> <output_path> <checkpoint_path>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_path = sys.argv[2]
    checkpoint_path = sys.argv[3]

    spark = SedonaContext.builder().appName(APP_NAME).master("local").getOrCreate()
    logging.info("Application Initialized: " + spark.sparkContext.appName)
    batch_ingest_rasters.run(spark, input_dir, output_path, checkpoint_path)
    logging.info("Application Done: " + spark.sparkContext.appName)
    spark.stop()
