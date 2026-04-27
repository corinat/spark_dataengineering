import argparse
import logging
from pathlib import Path

import geopandas as gpd

logging.basicConfig(level=logging.INFO)


def main(zonal_stats_path: str, output_gpkg: str) -> None:
    logging.info("Reading zonal statistics from: %s", zonal_stats_path)
    gdf = gpd.read_parquet(zonal_stats_path)

    Path(output_gpkg).parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(output_gpkg, driver="GPKG", layer="zonal_stats")
    logging.info("Saved GeoPackage to: %s", output_gpkg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export zonal statistics as a GeoPackage")
    parser.add_argument("zonal_stats_path", help="Path to the zonal statistics Parquet directory")
    parser.add_argument("output_gpkg", help="Path to write the output GeoPackage")
    args = parser.parse_args()

    main(args.zonal_stats_path, args.output_gpkg)
