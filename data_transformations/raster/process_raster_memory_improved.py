import logging
from collections.abc import Iterator

import numpy as np
import pandas as pd
import rasterio
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BinaryType, DoubleType, IntegerType, LongType,
                               StringType, StructField, StructType,
                               TimestampType)
from sedona.spark import SedonaContext

BINARY_FILE_SCHEMA = StructType([
    StructField("path", StringType(), nullable=False),
    StructField("modificationTime", TimestampType(), nullable=False),
    StructField("length", LongType(), nullable=False),
    StructField("content", BinaryType(), nullable=True),
])

TILE_SCHEMA = StructType([
    StructField("path", StringType(), nullable=False),
    StructField("block_x", IntegerType(), nullable=False),
    StructField("block_y", IntegerType(), nullable=False),
    StructField("width", IntegerType(), nullable=False),
    StructField("height", IntegerType(), nullable=False),
    StructField("num_bands", IntegerType(), nullable=False),
    StructField("mean_value", DoubleType(), nullable=True),
])


def _extract_blocks(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Open each COG file and yield one row per tile block.

    Called via mapInPandas — GDAL reads only the requested blocks from disk,
    so the full raster is never loaded into memory.
    """
    for pdf in iterator:
        rows = []
        for path in pdf["path"]:
            with rasterio.open(path) as src:
                for (_, _), window in src.block_windows(1):
                    data = src.read(window=window)
                    rows.append({
                        "path": path,
                        "block_x": int(window.col_off),
                        "block_y": int(window.row_off),
                        "width": int(window.width),
                        "height": int(window.height),
                        "num_bands": src.count,
                        "mean_value": float(np.nanmean(data)),
                    })
        yield pd.DataFrame(rows, columns=[f.name for f in TILE_SCHEMA])


def load_raster_batch(spark: SparkSession, input_dir: str) -> DataFrame:
    """Create a streaming DataFrame of tile blocks from a directory of COG files.

    Monitors the given directory for new raster files using Spark Structured
    Streaming with the binaryFile source. File content is dropped immediately —
    each path is passed to a mapInPandas function that opens the COG with
    rasterio and yields one row per tile block without loading the full raster.

    Args:
        spark: Active SparkSession, will be extended with a SedonaContext.
        input_dir: Path to the directory to monitor for new GeoTIFF files.

    Returns:
        Streaming DataFrame with columns: path, block_x, block_y, width,
        height, num_bands, mean_value.
    """
    sedona = SedonaContext.create(spark)

    path_stream = (
        sedona.readStream
        .format("binaryFile")
        .schema(BINARY_FILE_SCHEMA)
        .option("pathGlobFilter", "*.tif")
        .option("recursiveFileLookup", "true")
        .option("maxFilesPerTrigger", 3)  # process one file at a time to keep memory usage low
        .load(input_dir)
        .select("path")  # drop content — rasterio reads from disk by path
    )

    return path_stream.mapInPandas(_extract_blocks, schema=TILE_SCHEMA)


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
        .trigger(processingTime="1 minute")  # check for new files every minute
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
