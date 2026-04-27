import logging
import tempfile
from pathlib import Path

import rasterio
from rasterio.enums import Resampling
from rasterio.shutil import copy

OVERVIEW_LEVELS = [2, 4, 8, 16]
OVERVIEW_RESAMPLING = Resampling.average
COG_OPTIONS = {
    "driver": "COG",
    "compress": "deflate",
    "blocksize": 512,
    "overviews": "auto",
}


def convert_to_cog(input_path: str, output_path: str) -> None:
    """Convert a GeoTIFF to Cloud Optimized GeoTIFF (COG).

    COG conversion cannot be done with Spark/Sedona — Spark can read COGs efficiently
    via HTTP range requests, but writing the internal structure (tiling, overviews,
    leader metadata) requires GDAL-based tooling. This function uses rasterio (GDAL).

    Data is copied to a temporary on-disk dataset in block-sized windows to keep memory
    usage bounded — suitable for large rasters. The source file is never modified.
    Output is tiled (512x512), DEFLATE-compressed, and includes embedded overviews.

    Args:
        input_path: Path to the source GeoTIFF file.
        output_path: Destination path for the output COG file.
    """
    logging.info("Opening source raster: %s", input_path)
    with rasterio.open(input_path) as src:
        logging.info(
            "Source CRS: %s | Size: %dx%d | Bands: %d",
            src.crs,
            src.width,
            src.height,
            src.count,
        )
        profile = src.profile.copy()
        profile.update(nodata=0)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir) / "tmp_overviews.tif"

            with rasterio.open(tmp_path, "w", **profile) as tmp_ds:
                for _, window in tmp_ds.block_windows(1):
                    tmp_ds.write(src.read(window=window), window=window)

                logging.info("Building overviews at levels: %s", OVERVIEW_LEVELS)
                tmp_ds.build_overviews(OVERVIEW_LEVELS, OVERVIEW_RESAMPLING)
                tmp_ds.update_tags(ns="rio_overview", resampling=OVERVIEW_RESAMPLING.name)

            logging.info("Writing COG to: %s", output_path)
            with rasterio.open(tmp_path) as overview_src:
                copy(overview_src, output_path, **COG_OPTIONS)

    logging.info("COG conversion complete: %s", output_path)
