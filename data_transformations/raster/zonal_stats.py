import logging

import geopandas as gpd
from pyspark.sql import DataFrame, SparkSession
from sedona.spark import SedonaContext
from shapely import wkb


def load_zones(spark: SparkSession, vector_path: str, table_name: str = "clip") -> DataFrame:
    """Load a vector zone layer from a GeoPackage file.

    Args:
        spark: Active SparkSession with Sedona support.
        vector_path: Path to the GeoPackage (.gpkg) file.
        table_name: Name of the layer/table inside the GeoPackage. Defaults to 'clip'.

    Returns:
        DataFrame with the zone geometries and any associated attributes.
    """
    return spark.read.format("geopackage") \
        .option("tableName", table_name) \
        .load(vector_path)


def compute_zonal_stats(spark: SparkSession, raster_df: DataFrame, zones_df: DataFrame) -> DataFrame:
    """Compute per-zone raster statistics using RS_ZonalStats.

    For each zone geometry, computes count, sum, mean, stddev, min, and max
    of band 1 pixel values that fall within the zone, excluding nodata pixels.

    Args:
        spark: Active SparkSession with Sedona support.
        raster_df: DataFrame produced by the ingest step, containing a 'raster' column.
        zones_df: DataFrame containing a 'geometry' column with zone polygons.

    Returns:
        DataFrame with columns: zone_geometry, pixel_count, pixel_sum, pixel_mean,
        pixel_stddev, pixel_min, pixel_max.
    """
    raster_df.createOrReplaceTempView("raster_data")
    zones_df.createOrReplaceTempView("zones")

    return spark.sql("""
        SELECT
            ST_AsBinary(z.geometry)                                 AS zone_geometry,
            RS_ZonalStats(r.raster, z.geometry, 1, 'count', true)  AS pixel_count,
            RS_ZonalStats(r.raster, z.geometry, 1, 'sum',   true)  AS pixel_sum,
            RS_ZonalStats(r.raster, z.geometry, 1, 'mean',  true)  AS pixel_mean,
            RS_ZonalStats(r.raster, z.geometry, 1, 'stddev',true)  AS pixel_stddev,
            RS_ZonalStats(r.raster, z.geometry, 1, 'min',   true)  AS pixel_min,
            RS_ZonalStats(r.raster, z.geometry, 1, 'max',   true)  AS pixel_max
        FROM raster_data r
        CROSS JOIN zones z
    """)


def run(spark: SparkSession, raster_parquet_path: str, vector_path: str, output_path: str) -> None:
    """Run the zonal statistics pipeline and write results as Parquet.

    Reads the ingested raster Parquet and the clip vector, computes per-zone
    statistics, previews the result, and persists it to Parquet.

    Args:
        spark: Active SparkSession.
        raster_parquet_path: Path to the Parquet files produced by the ingest step.
        vector_path: Path to the GeoPackage file containing zone geometries.
        output_path: Destination path for the output Parquet files.
    """
    logging.info("Reading ingested raster from: %s", raster_parquet_path)
    sedona = SedonaContext.create(spark)

    raster_df = sedona.read.parquet(raster_parquet_path)
    zones_df = load_zones(sedona, vector_path)

    logging.info("Computing zonal statistics")
    result = compute_zonal_stats(sedona, raster_df, zones_df)
    result.show(truncate=False)

    from pathlib import Path
    pdf = result.toPandas()
    gdf = gpd.GeoDataFrame(
        pdf.drop(columns=["zone_geometry"]),
        geometry=pdf["zone_geometry"].apply(wkb.loads),
        crs="EPSG:4326",
    )
    output_file = Path(output_path) / "part-0.parquet"
    Path(output_path).mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(output_file)
    logging.info("Zonal statistics written to: %s", output_path)
