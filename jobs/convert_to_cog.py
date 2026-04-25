"""Convert a GeoTIFF to Cloud Optimized GeoTIFF (COG).

COG conversion cannot be done with Spark/Sedona — Spark can read COGs
efficiently via HTTP range requests, but writing the COG internal structure
(tiling, overviews, leader metadata) requires GDAL-based tooling.
This script uses rasterio (which wraps GDAL) to perform the conversion.

Usage:
    poetry run python jobs/convert_to_cog.py <INPUT_TIFF> <OUTPUT_COG>

Example:
    poetry run python jobs/convert_to_cog.py \\
        resources/clipped_raster.tif \\
        resources/clipped_raster_cog.tif
"""

import logging
import sys
from pathlib import Path

import rasterio
from rasterio.enums import Resampling
from rasterio.shutil import copy

LOG_FILENAME = "project.log"

OVERVIEW_LEVELS = [2, 4, 8, 16]
OVERVIEW_RESAMPLING = Resampling.average
COG_OPTIONS = {
    "driver": "COG",
    "compress": "deflate",
    "blocksize": 512,
    "overviews": "auto",
}


def convert_to_cog(input_path: str, output_path: str) -> None:
    """Convert a GeoTIFF to Cloud Optimized GeoTIFF format.

    Builds internal overviews on the source file then writes a COG using
    rasterio's copy driver. The output is tiled (512x512), DEFLATE-compressed,
    and includes embedded overviews — making it suitable for efficient reads
    from cloud object storage (S3, GCS, ADLS) via HTTP range requests.

    Args:
        input_path: Path to the source GeoTIFF file.
        output_path: Destination path for the output COG file.
    """
    logging.info("Opening source raster: %s", input_path)
    with rasterio.open(input_path, "r+") as src:
        logging.info(
            "Source CRS: %s | Size: %dx%d | Bands: %d",
            src.crs,
            src.width,
            src.height,
            src.count,
        )
        logging.info("Building overviews at levels: %s", OVERVIEW_LEVELS)
        src.build_overviews(OVERVIEW_LEVELS, OVERVIEW_RESAMPLING)
        src.update_tags(ns="rio_overview", resampling=OVERVIEW_RESAMPLING.name)

        logging.info("Writing COG to: %s", output_path)
        copy(src, output_path, **COG_OPTIONS)

    logging.info("COG conversion complete: %s", output_path)
    print(f"COG written to: {output_path}")


if __name__ == "__main__":
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)

    if len(sys.argv) != 3:
        print("Usage: python jobs/convert_to_cog.py <INPUT_TIFF> <OUTPUT_COG>")
        sys.exit(1)

    input_tiff = sys.argv[1]
    output_cog = sys.argv[2]

    Path(output_cog).parent.mkdir(parents=True, exist_ok=True)
    convert_to_cog(input_tiff, output_cog)
