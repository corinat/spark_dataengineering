import logging

from sedona.spark import SedonaContext
from pyspark.sql import SparkSession, DataFrame


def load_raster(spark: SparkSession, raster_path: str) -> DataFrame:
    """Load a GeoTIFF raster file and extract its metadata.

    Reads the GeoTIFF using Sedona's RS_FromGeoTiff and returns a DataFrame
    containing the raster object alongside its width, height, number of bands,
    full metadata array, and envelope (bounding geometry).

    Args:
        spark: Active SparkSession, will be extended with a SedonaContext.
        raster_path: Path to the GeoTIFF file.

    Returns:
        DataFrame with columns: raster, width, height, num_bands, metadata, envelope.
    """
    sedona = SedonaContext.create(spark)
    sedona.sql(f"""
        CREATE OR REPLACE TEMP VIEW raw_raster AS
        SELECT RS_FromGeoTiff(content) AS raster
        FROM binaryFile.`{raster_path}`
    """)
    return sedona.sql("""
        SELECT
            raster,
            RS_Width(raster)    AS width,
            RS_Height(raster)   AS height,
            RS_NumBands(raster) AS num_bands,
            RS_Metadata(raster) AS metadata,
            RS_Envelope(raster) AS envelope
        FROM raw_raster
    """)


def run(spark: SparkSession, raster_path: str, output_path: str) -> None:
    """Ingest a GeoTIFF raster and write it as Parquet.

    Entry point for the raster ingest step. Loads the raster, prints its schema
    and a preview, then persists it to Parquet for downstream processing.

    Args:
        spark: Active SparkSession.
        raster_path: Path to the source GeoTIFF file.
        output_path: Destination path for the output Parquet files.
    """
    logging.info("Ingesting raster from: %s", raster_path)
    df = load_raster(spark, raster_path)
    df.printSchema()
    df.show(truncate=False)
    df.write.parquet(output_path)
    logging.info("Raster ingested to: %s", output_path)
