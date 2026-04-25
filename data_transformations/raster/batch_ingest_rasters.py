import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BinaryType, LongType, StringType, StructField,
                               StructType, TimestampType)
from sedona.spark import SedonaContext

BINARY_FILE_SCHEMA = StructType([
    StructField("path", StringType(), nullable=False),
    StructField("modificationTime", TimestampType(), nullable=False),
    StructField("length", LongType(), nullable=False),
    StructField("content", BinaryType(), nullable=True),
])


def load_raster_batch(spark: SparkSession, input_dir: str) -> DataFrame:
    """Create a streaming DataFrame from a directory of GeoTIFF files.

    Monitors the given directory for new raster files using Spark Structured
    Streaming with the binaryFile source. Each new file is read, parsed with
    Sedona's RS_FromGeoTiff, and enriched with metadata columns.

    Args:
        spark: Active SparkSession, will be extended with a SedonaContext.
        input_dir: Path to the directory to monitor for new GeoTIFF files.

    Returns:
        Streaming DataFrame with columns: path, raster, width, height,
        num_bands, metadata, envelope.
    """
    sedona = SedonaContext.create(spark)

    raw_stream = (
        sedona.readStream
        .format("binaryFile")
        .schema(BINARY_FILE_SCHEMA)
        .option("pathGlobFilter", "*.tif")
        .option("recursiveFileLookup", "true")
        .load(input_dir)
    )

    return raw_stream.select(
        F.col("path"),
        F.expr("RS_FromGeoTiff(content)").alias("raster"),
    ).select(
        F.col("path"),
        F.col("raster"),
        F.expr("RS_Width(raster)").alias("width"),
        F.expr("RS_Height(raster)").alias("height"),
        F.expr("RS_NumBands(raster)").alias("num_bands"),
        F.expr("RS_Metadata(raster)").alias("metadata"),
        F.expr("RS_Envelope(raster)").alias("envelope"),
    )


def run(
    spark: SparkSession,
    input_dir: str,
    output_path: str,
    checkpoint_path: str,
) -> None:
    """Ingest GeoTIFF rasters from a directory and write them as Parquet.

    Processes all unprocessed .tif files in input_dir using trigger(availableNow=True)
    and exits when done. Re-running will only pick up files added since the last run,
    tracked via checkpoint.

    Args:
        spark: Active SparkSession.
        input_dir: Directory containing GeoTIFF files to process.
        output_path: Destination path for the output Parquet files.
        checkpoint_path: Path for checkpoint metadata.
    """
    logging.info("Starting batch raster ingest from: %s", input_dir)

    stream_df = load_raster_batch(spark, input_dir)

    query = (
        stream_df.writeStream
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )

    logging.info("Streaming query started. Awaiting termination...")
    query.awaitTermination()

    last_progress = query.lastProgress
    if last_progress is None or last_progress.get("numInputRows", 0) == 0:
        logging.info("No new files to process — all files already ingested.")
    else:
        num_rows = last_progress.get("numInputRows", 0)
        logging.info("Batch raster ingest complete. Processed %d rows. Output: %s", num_rows, output_path)
